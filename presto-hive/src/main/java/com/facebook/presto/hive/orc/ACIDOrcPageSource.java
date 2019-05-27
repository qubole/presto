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
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.orc.ACIDConstants.ACID_BUCKET_INDEX;
import static com.facebook.presto.orc.ACIDConstants.ACID_META_COLS_COUNT;
import static com.facebook.presto.orc.ACIDConstants.ACID_ORIGINAL_TXN_INDEX;
import static com.facebook.presto.orc.ACIDConstants.ACID_ROWID_INDEX;

public class ACIDOrcPageSource
        extends OrcPageSource
{
    private DeletedRowsRegistry deletedRowsRegistry;

    public ACIDOrcPageSource(
            OrcPageSourceFactory pageSourceFactory,
            ConnectorSession session,
            Configuration configuration,
            DateTimeZone hiveStorageTimeZone,
            HdfsEnvironment hdfsEnvironment,
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<HiveColumnHandle> columns,
            TypeManager typeManager,
            AggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats,
            DataSize deletedRowsCacheSize,
            Duration deletedRowsCacheTTL,
            Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        super(recordReader, orcDataSource, columns, typeManager, systemMemoryContext, stats);
        deletedRowsRegistry = new DeletedRowsRegistry(
                pageSourceFactory,
                session,
                configuration,
                hiveStorageTimeZone,
                hdfsEnvironment,
                deletedRowsCacheSize,
                deletedRowsCacheTTL,
                deleteDeltaLocations);
    }

    @Override
    public Page getNextPage()
    {
        Page dataPage = super.getNextPage();
        if (dataPage == null) {
            return null;
        }
        Block[] blocks = processAcidBlocks(dataPage);
        return new Page(dataPage.getPositionCount(), blocks);
    }

    private Block[] processAcidBlocks(Page dataPage)
    {
        if (dataPage.getChannelCount() < ACID_META_COLS_COUNT) {
            // There should atlest be 5 columns i.e ACID meta columns should be there at the least
            throw new PrestoException(HIVE_BAD_DATA, "Did not read enough blocks for ACID file: " + dataPage.getChannelCount());
        }

        Block[] dataBlocks = new Block[dataPage.getChannelCount() - ACID_META_COLS_COUNT + 1]; // We will return only data blocks and one isValid block
        // Copy data block references
        int colIdx = 0;
        for (int i = ACID_META_COLS_COUNT; i < dataPage.getChannelCount(); i++) {
            dataBlocks[colIdx++] = dataPage.getBlock(i);
        }

        dataBlocks[colIdx] = deletedRowsRegistry.createIsValidBlock(dataPage.getBlock(ACID_ORIGINAL_TXN_INDEX), dataPage.getBlock(ACID_BUCKET_INDEX), dataPage.getBlock(ACID_ROWID_INDEX));

        return dataBlocks;
    }
}
