package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.orc.ACIDConstants.ACID_BUCKET_INDEX;
import static com.facebook.presto.orc.ACIDConstants.ACID_META_COLS_COUNT;
import static com.facebook.presto.orc.ACIDConstants.ACID_ORIGINAL_TXN_INDEX;
import static com.facebook.presto.orc.ACIDConstants.ACID_ROWID_INDEX;
import static com.facebook.presto.orc.ACIDConstants.ACID_ROW_STRUCT_INDEX;

public class ACIDOrcPageSource
        extends OrcPageSource
{
    private DeletedRowsRegistry deletedRowsRegistry;

    public ACIDOrcPageSource(OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<HiveColumnHandle> columns,
            TypeManager typeManager,
            AggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats)
    {
        super(recordReader, orcDataSource, columns, typeManager, systemMemoryContext, stats);
        deletedRowsRegistry = new DeletedRowsRegistry();
    }

    @Override
    public Page getNextPage()
    {
        Page dataPage = super.getNextPage();
        if (dataPage == null) {
            return null;
        }
        Block[] blocks = processAcidBlocks(dataPage.getBlocks());
        return new Page(dataPage.getPositionCount(), blocks);
    }

    private Block[] processAcidBlocks(Block[] blocks)
    {
        if (blocks.length < ACID_ROW_STRUCT_INDEX) {
            // There should atlest be 6 columns, 5 meta columns and atleast 1 data
            throw new PrestoException(HIVE_BAD_DATA, "Did not read enough blocks for ACID file: " + blocks.length);
        }

        Block[] dataBlocks = new Block[blocks.length - ACID_META_COLS_COUNT + 1]; // We will return only data blocks and one isValid block
        // Copy data block references
        int colIdx = 0;
        for (int i = ACID_META_COLS_COUNT; i < blocks.length; i++) {
            dataBlocks[colIdx++] = blocks[i];
        }

        dataBlocks[colIdx] = deletedRowsRegistry.createIsValidBlock(blocks[ACID_ORIGINAL_TXN_INDEX], blocks[ACID_BUCKET_INDEX], blocks[ACID_ROWID_INDEX]);

        return dataBlocks;
    }
}
