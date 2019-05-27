/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.DeleteDeltaLocations;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.orc.ACIDConstants.ACID_BUCKET_INDEX;
import static com.facebook.presto.orc.ACIDConstants.ACID_META_COLS_COUNT;
import static com.facebook.presto.orc.ACIDConstants.ACID_META_COLUMNS;
import static com.facebook.presto.orc.ACIDConstants.ACID_ORIGINAL_TXN_INDEX;
import static com.facebook.presto.orc.ACIDConstants.ACID_ROWID_INDEX;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

public class DeletedRowsRegistry
{
    private final List<DeletedRowsLoader> loaders;
    private final String queryId;

    // variables to track cache hit rate, Guava Cache.stats() is not useful because multiple threads loading same
    // value get synchronised and only one of them load value but all of them update cache-miss count.
    // We want to know how many times did we read from cache rather
    private static final AtomicLong totalRequests = new AtomicLong();
    private static final AtomicLong requestsNeedingLoad = new AtomicLong();

    // For a query, DELETE_DELTA rows for a partition location will not change, they are cached in this cache
    private static Cache<String, List<RowId>> cache; // cachekey = QueryId_PartitionLocation
    private static Boolean lock = new Boolean(true);

    private static Properties orcProperty = new Properties();
    static {
        orcProperty.setProperty(SERIALIZATION_LIB, OrcSerde.class.getName());
    }
    private static final List<HiveColumnHandle> ACID_ROW_ID_COLUMN_HANDLES = createAcidRowIdMetaColumns();
    private static final int ROW_ID_SIZE = Long.BYTES * 2 + Integer.BYTES;
    private static final List<DeletedRowsLoader> EMPTY_LOADERS = ImmutableList.of();
    private static final CacheStats EMPTY_STATS = new AbstractCache.SimpleStatsCounter().snapshot();

    public DeletedRowsRegistry(
            OrcPageSourceFactory pageSourceFactory,
            ConnectorSession session,
            Configuration configuration,
            DateTimeZone hiveStorageTimeZone,
            HdfsEnvironment hdfsEnvironment,
            DataSize cacheSize,
            Duration cacheTTL,
            Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        if (cache == null) {
            synchronized (lock) {
                if (cache == null) {
                    cache = CacheBuilder.newBuilder()
                            .expireAfterAccess(cacheTTL.toMillis(), TimeUnit.MILLISECONDS)
                            .maximumWeight((int) cacheSize.toBytes())
                            .weigher((Weigher<String, List<RowId>>) (key, value) -> {
                                return ROW_ID_SIZE * value.size();
                            })
                            .recordStats()
                            .build();
                }
            }
        }

        if (!deleteDeltaLocations.isPresent()) {
            loaders = EMPTY_LOADERS;
        }
        else {
            ImmutableList.Builder<DeletedRowsLoader> loaders = ImmutableList.builder();
            for (String partitionLocation : deleteDeltaLocations.get().getDeleteDeltas().keySet()) {
                loaders.add(new DeletedRowsLoader(partitionLocation,
                        deleteDeltaLocations.get().getDeleteDeltas().get(partitionLocation),
                        pageSourceFactory,
                        configuration,
                        session,
                        hdfsEnvironment,
                        hiveStorageTimeZone));
            }
            this.loaders = loaders.build();
        }

        this.queryId = session.getQueryId();
    }

    public Block createIsValidBlock(Block orignalTxn, Block bucket, Block rowId)
    {
        Page input = new Page(orignalTxn, bucket, rowId);
        BlockBuilder isValidBlock = BOOLEAN.createFixedSizeBlockBuilder(input.getPositionCount());
        if (loaders == EMPTY_LOADERS) {
            for (int pos = 0; pos < rowId.getPositionCount(); pos++) {
                BOOLEAN.writeBoolean(isValidBlock, true);
            }
        }
        else {
            Set<RowId> deletedRows = loadDeletedRows();
            for (int pos = 0; pos < rowId.getPositionCount(); pos++) {
                BOOLEAN.writeBoolean(isValidBlock, !deletedRows.contains(createRowId(input, pos)));
            }
        }
        return isValidBlock.build();
    }

    private String getCacheKey(String queryId, String partitionLocation)
    {
        return new StringBuilder().append(queryId).append("_").append(partitionLocation).toString();
    }

    private static RowId createRowId(Page page, int position)
    {
        return new RowId(
                page.getBlock(0).getLong(position, 0),
                page.getBlock(1).getInt(position, 0),
                page.getBlock(2).getLong(position, 0));
    }

    @VisibleForTesting
    public Set<RowId> loadDeletedRows()
    {
        ImmutableSet.Builder<RowId> allDeletedRows = ImmutableSet.builder();
        for (DeletedRowsLoader loader : loaders) {
            try {
                totalRequests.incrementAndGet();
                allDeletedRows.addAll(cache.get(getCacheKey(queryId, loader.getPartitionLocation()), loader));
            }
            catch (ExecutionException e) {
                throw new PrestoException(HIVE_UNKNOWN_ERROR, "Could not load deleted rows for location: " + loader.getPartitionLocation(), e);
            }
        }
        return allDeletedRows.build();
    }

    private static List<HiveColumnHandle> createAcidRowIdMetaColumns()
    {
        ImmutableList.Builder<HiveColumnHandle> physicalColumns = ImmutableList.builder();
        for (int i = 0; i < ACID_META_COLS_COUNT; i++) {
            HiveType hiveType = null;
            switch (i) {
                case ACID_ORIGINAL_TXN_INDEX:
                    hiveType = HiveType.HIVE_LONG;
                    break;
                case ACID_BUCKET_INDEX:
                    hiveType = HiveType.HIVE_INT;
                    break;
                case ACID_ROWID_INDEX:
                    hiveType = HiveType.HIVE_LONG;
                    break;
                default:
                    // do nothing for other meta cols
                    break;
            }
            if (hiveType != null) {
                physicalColumns.add(new HiveColumnHandle(
                        ACID_META_COLUMNS[i],
                        hiveType,
                        hiveType.getTypeSignature(),
                        i,
                        REGULAR,
                        Optional.empty()));
            }
        }

        return physicalColumns.build();
    }

    public static CacheStats getCacheStats()
    {
        if (cache == null) {
            return EMPTY_STATS;
        }

        long missCount = requestsNeedingLoad.get();
        CacheStats stats = cache.stats();
        return new CacheStats(totalRequests.get() - missCount,
                missCount,
                stats.loadSuccessCount(),
                stats.loadExceptionCount(),
                stats.totalLoadTime(),
                stats.evictionCount());
    }

    public static long getCacheSize()
    {
        return cache.asMap().values().stream().map(rowIds -> rowIds.size()).reduce(0, Integer::sum) * ROW_ID_SIZE;
    }

    private class DeletedRowsLoader
            implements Callable<List<RowId>>
    {
        private String partitionLocation;
        private List<DeleteDeltaLocations.DeleteDeltaInfo> deleteDeltaInfos;
        private Configuration configuration;
        private DateTimeZone hiveStorageTimeZone;
        private OrcPageSourceFactory pageSourceFactory;
        private ConnectorSession session;
        private HdfsEnvironment hdfsEnvironment;

        public DeletedRowsLoader(
                String partitionLocation,
                List<DeleteDeltaLocations.DeleteDeltaInfo> deleteDeltaInfos,
                OrcPageSourceFactory pageSourceFactory,
                Configuration configuration,
                ConnectorSession session,
                HdfsEnvironment hdfsEnvironment,
                DateTimeZone hiveStorageTimeZone)
        {
            this.partitionLocation = partitionLocation;
            this.deleteDeltaInfos = deleteDeltaInfos;
            this.pageSourceFactory = pageSourceFactory;
            this.configuration = configuration;
            this.session = session;
            this.hdfsEnvironment = hdfsEnvironment;
            this.hiveStorageTimeZone = hiveStorageTimeZone;
        }

        public String getPartitionLocation()
        {
            return partitionLocation;
        }

        @Override
        public List<RowId> call()
                throws IOException
        {
            ImmutableList.Builder<RowId> builder = ImmutableList.builder();
            requestsNeedingLoad.incrementAndGet();
            for (DeleteDeltaLocations.DeleteDeltaInfo deleteDeltaInfo : deleteDeltaInfos) {
                Path path = createPath(partitionLocation, deleteDeltaInfo);
                FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
                RemoteIterator<LocatedFileStatus> fileStatuses = fileSystem.listFiles(path, false);
                while (fileStatuses.hasNext()) {
                    LocatedFileStatus fileStatus = fileStatuses.next();
                    if (!AcidUtils.hiddenFileFilter.accept(fileStatus.getPath())) {
                        // Ignore hidden files
                        continue;
                    }
                    Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                            configuration,
                            session,
                            fileStatus.getPath(),
                            0,
                            fileStatus.getLen(),
                            fileStatus.getLen(),
                            orcProperty,
                            ACID_ROW_ID_COLUMN_HANDLES,
                            TupleDomain.all(),
                            hiveStorageTimeZone);
                    if (!pageSource.isPresent()) {
                        throw new PrestoException(
                                HIVE_BAD_DATA,
                                "Could not create page source for delete delta ORC file: " + path);
                    }
                    while (!pageSource.get().isFinished()) {
                        Page page = pageSource.get().getNextPage();
                        if (page != null) {
                            for (int i = 0; i < page.getPositionCount(); i++) {
                                builder.add(createRowId(page, i));
                            }
                        }
                    }
                }
            }
            return builder.build();
        }

        private Path createPath(String partitionLocation, DeleteDeltaLocations.DeleteDeltaInfo deleteDeltaInfo)
        {
            return new Path(partitionLocation,
                    AcidUtils.deleteDeltaSubdir(
                            deleteDeltaInfo.getMinWriteId(),
                            deleteDeltaInfo.getMaxWriteId(),
                            deleteDeltaInfo.getStatementId()));
        }
    }

    @VisibleForTesting
    public static class RowId
    {
        long originalTxn;
        int bucket;
        long rowId;

        public RowId(long originalTxn, int bucket, long rowId)
        {
            this.originalTxn = originalTxn;
            this.bucket = bucket;
            this.rowId = rowId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(originalTxn, bucket, rowId);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (!(o instanceof RowId)) {
                return false;
            }

            RowId other = (RowId) o;
            if (originalTxn == other.originalTxn && bucket == other.bucket && rowId == other.rowId) {
                return true;
            }

            return false;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this).addValue(originalTxn).addValue(bucket).addValue(rowId).toString();
        }
    }
}
