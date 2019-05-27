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
package com.facebook.presto.hive.acid;

import com.facebook.presto.hive.DeleteDeltaLocations;
import com.facebook.presto.hive.orc.DeletedRowsRegistry;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.IntegerType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.acid.AcidPageProcessorProvider.createDeletedRowsRegistry;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeletedRowsRegistry
{
    @Test
    public void testReadingDeletedRows()
    {
        DeleteDeltaLocations deleteDeltaLocations = new DeleteDeltaLocations();
        addDeleteDelta(deleteDeltaLocations, 4L, 4L, 0);
        addDeleteDelta(deleteDeltaLocations, 7L, 7L, 0);
        DeletedRowsRegistry registry = createDeletedRowsRegistry(Optional.of(deleteDeltaLocations));
        Set<DeletedRowsRegistry.RowId> deletedRows = registry.loadDeletedRows();
        assertTrue(deletedRows.size() == 2, "Expected to read 2 deleted rows but read: " + deletedRows);
        assertTrue(deletedRows.contains(new DeletedRowsRegistry.RowId(2L, 536870912, 0L)), "RowId{2, 536870912, 0} not found in: " + deletedRows);
        assertTrue(deletedRows.contains(new DeletedRowsRegistry.RowId(6L, 536870912, 0L)), "RowId{6, 536870912, 0} not found in: " + deletedRows);
    }

    @Test
    public void testIsValidBlockWithDeletedRows()
    {
        DeleteDeltaLocations deleteDeltaLocations = new DeleteDeltaLocations();
        addDeleteDelta(deleteDeltaLocations, 4L, 4L, 0);
        addDeleteDelta(deleteDeltaLocations, 7L, 7L, 0);
        DeletedRowsRegistry registry = createDeletedRowsRegistry(Optional.of(deleteDeltaLocations));

        int size = 10;
        BlockBuilder originalTxn = BigintType.BIGINT.createFixedSizeBlockBuilder(size);
        for (long i = 0; i < size; i++) {
            originalTxn.writeLong(i);
        }

        BlockBuilder bucket = IntegerType.INTEGER.createFixedSizeBlockBuilder(size);
        for (int i = 0; i < size; i++) {
            bucket.writeInt(536870912);
        }

        BlockBuilder rowId = BigintType.BIGINT.createFixedSizeBlockBuilder(size);
        for (long i = 0; i < size; i++) {
            rowId.writeLong(0L);
        }

        // 2 rows should be deleted RowId{2, 536870912, 0} and RowId{6, 536870912, 0}
        Block isValid = registry.createIsValidBlock(originalTxn.build(), bucket.build(), rowId.build());
        for (int i = 0; i < size; i++) {
            if (i == 2 || i == 6) {
                assertFalse(BooleanType.BOOLEAN.getBoolean(isValid, i), "Row number " + i + " should not be marked valid");
            }
            else {
                assertTrue(BooleanType.BOOLEAN.getBoolean(isValid, i), "Row number " + i + " should have been marked valid");
            }
        }
    }

    @Test
    public void testIsValidBlockWithNoDeletedRows()
    {
        DeletedRowsRegistry registry = createDeletedRowsRegistry(Optional.empty());
        int size = 100;
        Block originalTxn = fillDummyValues(BigintType.BIGINT.createFixedSizeBlockBuilder(size), size);
        Block bucket = fillDummyValues(IntegerType.INTEGER.createFixedSizeBlockBuilder(size), size);
        Block rowID = fillDummyValues(BigintType.BIGINT.createFixedSizeBlockBuilder(size), size);
        Block isValid = registry.createIsValidBlock(originalTxn, bucket, rowID);
        assertTrue(isValid.getPositionCount() == size, "Created isValid block of unexpected size " + isValid.getPositionCount());
        for (int i = 0; i < size; i++) {
            assertTrue(BooleanType.BOOLEAN.getBoolean(isValid, i), "Read 'False' at position " + i);
        }
    }

    private Block fillDummyValues(BlockBuilder blockBuilder, int size)
    {
        for (int i = 0; i < size; i++) {
            blockBuilder.appendNull();
        }
        return blockBuilder.build();
    }

    private void addDeleteDelta(DeleteDeltaLocations deleteDeltaLocations, long minWriteId, long maxWriteId, int statementId)
    {
        // ClassLoader finds top level resources, find that and build delta locations from it
        File partitionLocation = new File((Thread.currentThread().getContextClassLoader().getResource("fullacid_delete_delta_test").getPath()));
        Path deleteDeltaPath = new Path(new Path(partitionLocation.toString()), AcidUtils.deleteDeltaSubdir(minWriteId, maxWriteId, statementId));
        deleteDeltaLocations.addDeleteDelta(deleteDeltaPath, minWriteId, maxWriteId, statementId);
    }
}
