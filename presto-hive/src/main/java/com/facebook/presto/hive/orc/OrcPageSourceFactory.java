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

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_MISSING_COLUMN_NAMES;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static com.facebook.presto.hive.HiveUtil.isDeserializerClass;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcPageSourceFactory
        implements HivePageSourceFactory
{
    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");
    private final TypeManager typeManager;
    private final boolean useOrcColumnNames;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    @Inject
    public OrcPageSourceFactory(TypeManager typeManager, HiveClientConfig config, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats)
    {
        this(typeManager, requireNonNull(config, "hiveClientConfig is null").isUseOrcColumnNames(), hdfsEnvironment, stats);
    }

    public OrcPageSourceFactory(TypeManager typeManager, boolean useOrcColumnNames, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.useOrcColumnNames = useOrcColumnNames;
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone)
    {
        return createPageSource(
                configuration,
                session,
                path,
                start,
                length,
                fileSize,
                schema,
                columns,
                effectivePredicate,
                hiveStorageTimeZone,
                false);
    }

    @VisibleForTesting
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            boolean returnActualPageSource)
    {
        if (!isDeserializerClass(schema, OrcSerde.class)) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (fileSize == 0) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }

        boolean isFullAcid = AcidUtils.isFullAcidTable(((Map<String, String>) (((Map) schema))));

        return Optional.of(createOrcPageSource(
                ORC,
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                fileSize,
                columns,
                useOrcColumnNames,
                effectivePredicate,
                hiveStorageTimeZone,
                typeManager,
                getOrcMaxMergeDistance(session),
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session),
                getOrcTinyStripeThreshold(session),
                getOrcMaxReadBlockSize(session),
                getOrcLazyReadSmallRanges(session),
                isOrcBloomFiltersEnabled(session),
                stats,
                isFullAcid,
                returnActualPageSource));
    }

    public static OrcPageSource createOrcPageSource(
            OrcEncoding orcEncoding,
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            boolean useOrcColumnNames,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            DataSize tinyStripeThreshold,
            DataSize maxReadBlockSize,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats,
            boolean isFullAcid,
            boolean returnActualPageSource)
    {
        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    maxMergeDistance,
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
        try {
            OrcReader reader = new OrcReader(orcDataSource, orcEncoding, maxMergeDistance, maxBufferSize, tinyStripeThreshold, maxReadBlockSize);

            List<HiveColumnHandle> physicalColumns = isFullAcid
                    ? getPhysicalHiveColumnHandlesACID(columns, useOrcColumnNames, reader, path)
                    : getPhysicalHiveColumnHandles(columns, useOrcColumnNames, reader, path);
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
            for (HiveColumnHandle column : physicalColumns) {
                if (column.getColumnType() == REGULAR) {
                    Type type = typeManager.getType(column.getTypeSignature());
                    includedColumns.put(column.getHiveColumnIndex(), type);
                    columnReferences.add(new ColumnReference<>(column, column.getHiveColumnIndex(), type));
                }
            }

            // effective predicate should be updated to have new column index in the Domain because data columns are now shifted by 5 positions
            if (isFullAcid && effectivePredicate.getDomains().isPresent()) {
                Map<HiveColumnHandle, Domain> predicateDomain = effectivePredicate.getDomains().get();
                ImmutableMap.Builder<HiveColumnHandle, Domain> newPredicateDomain = ImmutableMap.builder();
                for (Map.Entry<HiveColumnHandle, Domain> entry : predicateDomain.entrySet()) {
                    HiveColumnHandle columnHandle = entry.getKey();
                    Domain domain = entry.getValue();
                    for (HiveColumnHandle physicalColumn : physicalColumns) {
                        if (physicalColumn.getName().equals(columnHandle.getName())) {
                            newPredicateDomain.put(physicalColumn, domain);
                        }
                    }
                }
                effectivePredicate = TupleDomain.withColumnDomains(newPredicateDomain.build());
            }

            OrcPredicate predicate = new TupleDomainOrcPredicate<>(effectivePredicate, columnReferences.build(), orcBloomFiltersEnabled);

            OrcRecordReader recordReader = reader.createRecordReader(
                    includedColumns.build(),
                    predicate,
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE,
                    isFullAcid);

            if (isFullAcid && !returnActualPageSource) {
                return new ACIDOrcPageSource(
                        recordReader,
                        orcDataSource,
                        physicalColumns,
                        typeManager,
                        systemMemoryUsage,
                        stats);
            }

            return new OrcPageSource(
                    recordReader,
                    orcDataSource,
                    physicalColumns,
                    typeManager,
                    systemMemoryUsage,
                    stats);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    private static List<HiveColumnHandle> getPhysicalHiveColumnHandles(List<HiveColumnHandle> columns, boolean useOrcColumnNames, OrcReader reader, Path path)
    {
        if (!useOrcColumnNames) {
            return columns;
        }

        verifyFileHasColumnNames(reader.getColumnNames(), path);

        Map<String, Integer> physicalNameOrdinalMap = buildPhysicalNameOrdinalMap(reader);
        int nextMissingColumnIndex = physicalNameOrdinalMap.size();

        ImmutableList.Builder<HiveColumnHandle> physicalColumns = ImmutableList.builder();
        for (HiveColumnHandle column : columns) {
            Integer physicalOrdinal = physicalNameOrdinalMap.get(column.getName());
            if (physicalOrdinal == null) {
                // if the column is missing from the file, assign it a column number larger
                // than the number of columns in the file so the reader will fill it with nulls
                physicalOrdinal = nextMissingColumnIndex;
                nextMissingColumnIndex++;
            }
            physicalColumns.add(new HiveColumnHandle(column.getName(), column.getHiveType(), column.getTypeSignature(), physicalOrdinal, column.getColumnType(), column.getComment()));
        }
        return physicalColumns.build();
    }

    private static List<HiveColumnHandle> getPhysicalHiveColumnHandlesACID(List<HiveColumnHandle> columns, boolean useOrcColumnNames, OrcReader reader, Path path)
    {
        // Always use column names from reader for ACID files
        if (!useOrcColumnNames) {
            // TODO stagra: Is it better to just not throw this? It is possible to read both ACID and non ACID tables in same installation and we might not need this flag for nonACID
            throw new UnsupportedOperationException("Reading Full ACID tables without enabling 'hive.orc.use-column-names' is not supported");
        }

        verifyFileHasColumnNames(reader.getColumnNames(), path);

        Map<String, Integer> physicalNameOrdinalMap = buildPhysicalNameOrdinalMapACID(reader);
        int nextMissingColumnIndex = physicalNameOrdinalMap.size();

        ImmutableList.Builder<HiveColumnHandle> physicalColumns = ImmutableList.builder();
        // Add all meta columns
        for (Map.Entry<String, Integer> entry : physicalNameOrdinalMap.entrySet()) {
            if (entry.getValue() > 4) {
                // Data columns, skip in this step
                continue;
            }

            HiveType hiveType = null;
            switch (entry.getKey()) {
                case "operation":
                    hiveType = HiveType.HIVE_INT;
                    break;
                case "originalTransaction":
                    hiveType = HiveType.HIVE_LONG;
                    break;
                case "bucket":
                    hiveType = HiveType.HIVE_INT;
                    break;
                case "rowId":
                    hiveType = HiveType.HIVE_LONG;
                    break;
                case "currentTransaction":
                    hiveType = HiveType.HIVE_LONG;
                    break;
                default:
                    // do nothing for other columns
                    break;
            }
            physicalColumns.add(new HiveColumnHandle(
                    entry.getKey(),
                    hiveType,
                    hiveType.getTypeSignature(),
                    entry.getValue(),
                    REGULAR,
                    Optional.empty()));
        }

        for (HiveColumnHandle column : columns) {
            Integer physicalOrdinal = physicalNameOrdinalMap.get(column.getName());
            if (physicalOrdinal == null) {
                // if the column is missing from the file, assign it a column number larger
                // than the number of columns in the file so the reader will fill it with nulls
                physicalOrdinal = nextMissingColumnIndex;
                nextMissingColumnIndex++;
            }
            physicalColumns.add(new HiveColumnHandle(column.getName(), column.getHiveType(), column.getTypeSignature(), physicalOrdinal, column.getColumnType(), column.getComment()));
        }
        return physicalColumns.build();
    }

    private static void verifyFileHasColumnNames(List<String> physicalColumnNames, Path path)
    {
        if (!physicalColumnNames.isEmpty() && physicalColumnNames.stream().allMatch(physicalColumnName -> DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnName).matches())) {
            throw new PrestoException(
                    HIVE_FILE_MISSING_COLUMN_NAMES,
                    "ORC file does not contain column names in the footer: " + path);
        }
    }

    private static Map<String, Integer> buildPhysicalNameOrdinalMap(OrcReader reader)
    {
        ImmutableMap.Builder<String, Integer> physicalNameOrdinalMap = ImmutableMap.builder();

        int ordinal = 0;
        for (String physicalColumnName : reader.getColumnNames()) {
            physicalNameOrdinalMap.put(physicalColumnName, ordinal);
            ordinal++;
        }

        return physicalNameOrdinalMap.build();
    }

    private static Map<String, Integer> buildPhysicalNameOrdinalMapACID(OrcReader reader)
    {
        ImmutableMap.Builder<String, Integer> physicalNameOrdinalMap = ImmutableMap.builder();

        List<OrcType> types = reader.getFooter().getTypes();
        // This is the structure of ACID file
        // struct<operation:int, originalTransaction:bigint, bucket:int, rowId:bigint, currentTransaction:bigint, row:struct<TABLE COLUMNS>>
        // RootStruct is type[0], originalTransaction is type[1], ..., RowStruct is type[6], table column1 is type[7] and so on
        if (types.size() < 7) {
            throw new PrestoException(
                    HIVE_BAD_DATA,
                    "ORC file does not contain adequate column types for ACID file: " + types);
        }

        List<String> tableColumnNames = types.get(6).getFieldNames();
        int ordinal = 0;
        // Add ACID meta columns
        for (int i = 0; i < types.get(0).getFieldCount() - 1; i++) { // -1 to skip the row STRUCT of data columns
            // Keeping ordinals starting from 0 as it will match with OrcRecordReader.getStatisticsByColumnOrdinal data column ordinals
            physicalNameOrdinalMap.put(types.get(0).getFieldName(i), ordinal);
            ordinal++;
        }

        // Add Data columns
        for (String physicalColumnName : tableColumnNames) {
            // Keeping ordinals starting from 0 as it will match with OrcRecordReader.getStatisticsByColumnOrdinal data column ordinals
            physicalNameOrdinalMap.put(physicalColumnName, ordinal);
            ordinal++;
        }

        return physicalNameOrdinalMap.build();
    }
}
