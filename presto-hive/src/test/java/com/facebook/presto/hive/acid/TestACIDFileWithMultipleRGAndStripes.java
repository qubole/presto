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

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.orc.OrcPageSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/*
 * Tests reading file with multiple row groups and multiple stripes
 * Sample file used nationFile25kRowsSortedOnNationKey.orc
 * It has replicated version of standard nation table (from file nation.tbl), with original 25 rows replicated into 25000 rows
 * It has row group size = 1000 and has content in sorted order of nationkey
 * Hence first row group has all nationkey=0 rows, next has nationkey=1 and so on
 *
 * Table Schema: n_nationkey:int,n_name:string,n_regionkey:int,n_comment:string
 *
 * Actual File Schema:
 * struct<operation:int,originalTransaction:bigint,bucket:int,rowId:bigint,currentTransaction:bigint,row:struct<n_nationkey:int,n_name:string,n_regionkey:int,n_comment:string>>
 *
 * This file has 5 stripes and 25 rowgroups
 */
public class TestACIDFileWithMultipleRGAndStripes
{
    private String filename = "nationFile25kRowsSortedOnNationKey.orc";
    private List<String> columnNames = ImmutableList.of("n_nationkey", "n_name", "n_regionkey", "n_comment");
    private List<Type> columnTypes = ImmutableList.of(IntegerType.INTEGER, VarcharType.VARCHAR, IntegerType.INTEGER, VarcharType.VARCHAR);

    @Test
    public void testFullFileRead()
            throws IOException
    {
        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, columnNames, columnTypes);
        List<NationRow> rows = readFullFile(pageSource, true);

        List<NationRow> expected = getExpectedResult(Optional.empty(), Optional.empty());
        assertTrue(Objects.equals(expected, rows));
    }

    @Test
    public void testSingleColumnRead()
            throws IOException
    {
        int colToRead = 2;
        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, ImmutableList.of(columnNames.get(colToRead)), ImmutableList.of(columnTypes.get(colToRead)));
        List<NationRow> rows = readFileCols(pageSource, ImmutableList.of(columnNames.get(colToRead)), ImmutableList.of(columnTypes.get(colToRead)), true);

        List<NationRow> expected = getExpectedResult(Optional.empty(), Optional.of(colToRead));
        assertTrue(Objects.equals(expected, rows));
    }

    @Test
    /*
     * tests file stats based pruning works fine
     */
    public void testFullFileSkipped()
            throws IOException
    {
        Domain nonExistingDomain = Domain.create(ValueSet.of(IntegerType.INTEGER, 100L), false);
        HiveColumnHandle nationKeyColumnHandle = new HiveColumnHandle(
                columnNames.get(0),
                HiveType.toHiveType(new HiveTypeTranslator(), columnTypes.get(0)),
                columnTypes.get(0).getTypeSignature(),
                0,
                REGULAR,
                Optional.empty());
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(nationKeyColumnHandle, nonExistingDomain));

        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, columnNames, columnTypes, tupleDomain);
        List<NationRow> rows = readFullFile(pageSource, true);

        assertTrue(rows.size() == 0);
        assertTrue(((OrcPageSource) pageSource).getRecordReader().isFileSkipped());
    }

    @Test
    /*
     * Tests stripe stats and row groups stats based pruning works fine
     */
    public void testSomeStripesAndRowGroupRead()
            throws IOException
    {
        Domain nonExistingDomain = Domain.create(ValueSet.of(IntegerType.INTEGER, 0L), false);
        HiveColumnHandle nationKeyColumnHandle = new HiveColumnHandle(
                columnNames.get(0),
                HiveType.toHiveType(new HiveTypeTranslator(), columnTypes.get(0)),
                columnTypes.get(0).getTypeSignature(),
                0,
                REGULAR,
                Optional.empty());
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(nationKeyColumnHandle, nonExistingDomain));

        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, columnNames, columnTypes, tupleDomain);
        List<NationRow> rows = readFullFile(pageSource, true);

        List<NationRow> expected = getExpectedResult(Optional.of(0), Optional.empty());
        assertTrue(rows.size() != 0);
        assertFalse(((OrcPageSource) pageSource).getRecordReader().isFileSkipped());
        assertTrue(((OrcPageSource) pageSource).getRecordReader().stripesRead() == 1);  // 1 out of 5 stripes should be read
        assertTrue(((OrcPageSource) pageSource).getRecordReader().getRowGroupsRead() == 1); // 1 out of 25 rowgroups should be read
        assertTrue(Objects.equals(expected, rows));
    }

    private List<NationRow> readFullFile(ConnectorPageSource pageSource, boolean resultsNeeded)
    {
        List<NationRow> rows = new ArrayList(resultsNeeded ? 25000 : 0);
        ImmutableList.Builder<Type> expectedReadTypesBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> expectedReadNamesBuilder = ImmutableList.builder();
        expectedReadTypesBuilder.add(IntegerType.INTEGER); // operation
        expectedReadNamesBuilder.add("operation");
        expectedReadTypesBuilder.add(BigintType.BIGINT);   // originalTxn
        expectedReadNamesBuilder.add("originalTransaction");
        expectedReadTypesBuilder.add(IntegerType.INTEGER); // bucket
        expectedReadNamesBuilder.add("bucket");
        expectedReadTypesBuilder.add(BigintType.BIGINT);   // rowId
        expectedReadNamesBuilder.add("rowId");
        expectedReadTypesBuilder.add(BigintType.BIGINT);   // currentTxn
        expectedReadNamesBuilder.add("currentTransaction");
        expectedReadTypesBuilder.addAll(columnTypes);
        expectedReadNamesBuilder.addAll(columnNames);
        List<Type> expectedReadTypes = expectedReadTypesBuilder.build();
        List<String> expectedNames = expectedReadNamesBuilder.build();

        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page != null) {
                assertTrue(page.getChannelCount() == 9, "Did not read required number of blocks: " + page.getChannelCount());
                page = page.getLoadedPage();

                if (!resultsNeeded) {
                    continue;
                }

                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
                    for (int idx = 0; idx < expectedReadTypes.size(); idx++) {
                        values.put(expectedNames.get(idx), expectedReadTypes.get(idx).getObjectValue(AcidPageProcessorProvider.SESSION, page.getBlock(idx), pos));
                    }
                    rows.add(new NationRow(values.build()));
                }
            }
        }
        return rows;
    }

    private List<NationRow> readFileCols(ConnectorPageSource pageSource, List<String> columnNames, List<Type> columnTypes, boolean resultsNeeded)
    {
        List<NationRow> rows = new ArrayList(resultsNeeded ? 25000 : 0);
        ImmutableList.Builder<Type> expectedReadTypesBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> expectedReadNamesBuilder = ImmutableList.builder();
        expectedReadTypesBuilder.add(IntegerType.INTEGER); // operation
        expectedReadNamesBuilder.add("operation");
        expectedReadTypesBuilder.add(BigintType.BIGINT);   // originalTxn
        expectedReadNamesBuilder.add("originalTransaction");
        expectedReadTypesBuilder.add(IntegerType.INTEGER); // bucket
        expectedReadNamesBuilder.add("bucket");
        expectedReadTypesBuilder.add(BigintType.BIGINT);   // rowId
        expectedReadNamesBuilder.add("rowId");
        expectedReadTypesBuilder.add(BigintType.BIGINT);   // currentTxn
        expectedReadNamesBuilder.add("currentTransaction");
        expectedReadTypesBuilder.addAll(columnTypes);
        expectedReadNamesBuilder.addAll(columnNames);
        List<Type> expectedReadTypes = expectedReadTypesBuilder.build();
        List<String> expectedNames = expectedReadNamesBuilder.build();

        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page != null) {
                assertTrue(page.getChannelCount() == expectedNames.size(), "Did not read required number of blocks: " + page.getChannelCount());
                page = page.getLoadedPage();

                if (!resultsNeeded) {
                    continue;
                }

                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
                    for (int idx = 0; idx < expectedReadTypes.size(); idx++) {
                        values.put(expectedNames.get(idx), expectedReadTypes.get(idx).getObjectValue(AcidPageProcessorProvider.SESSION, page.getBlock(idx), pos));
                    }
                    rows.add(new NationRow(values.build()));
                }
            }
        }
        return rows;
    }

    /*
     * Returns rows for expected response, explodes each row from nation.tbl into 1000 rows
     *
     * If onlyForRowId is provided, then only that row from nation.tbls is read and exploded and others are ignored
     */
    private List<NationRow> getExpectedResult(Optional<Integer> onlyForRowId, Optional<Integer> onlyForColumnId)
            throws IOException
    {
        String nationFilePath = Thread.currentThread().getContextClassLoader().getResource("nation.tbl").getPath();
        final ImmutableList.Builder<NationRow> result = ImmutableList.builder();
        long rowId = 0;
        BufferedReader br = new BufferedReader(new FileReader(nationFilePath));
        try {
            String line;
            int lineNum = -1;
            while ((line = br.readLine()) != null) {
                lineNum++;
                if (onlyForRowId.isPresent() && onlyForRowId.get() != lineNum) {
                    continue;
                }
                rowId += replicateIntoResult(line, result, rowId, onlyForColumnId);
            }
        }
        finally {
            br.close();
        }
        return result.build();
    }

    private long replicateIntoResult(String line, ImmutableList.Builder<NationRow> resultBuilder, long startRowId, Optional<Integer> onlyForColumnId)
    {
        long replicationFactor = 1000; // same way the nationFile25kRowsSortedOnNationKey.orc is created
        for (int i = 0; i < replicationFactor; i++) {
            String[] cols = line.split("\\|");
            resultBuilder.add(new NationRow(
                    0,
                    2,
                    536870912,
                    startRowId++,
                    2,
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 0) ? Integer.parseInt(cols[0]) : -1,
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 1) ? cols[1] : "INVALID",
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 2) ? Integer.parseInt(cols[2]) : -1,
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 3) ? cols[3] : "INVALID"));
        }
        return replicationFactor;
    }

    private class NationRow
    {
        // prefilled values are the fixed ones in nationFile25kRowsSortedOnNationKey.orc
        int operation;
        long originalTransaction;
        int bucket;
        long rowId;
        long currentTransaction;
        int nationkey;
        String name;
        int regionkey;
        String comment;

        public NationRow(Map<String, Object> row)
        {
            this(
                    (Integer) row.getOrDefault("operation", -1),
                    (Long) row.getOrDefault("originalTransaction", -1),
                    (Integer) row.getOrDefault("bucket", -1),
                    (Long) row.getOrDefault("rowId", -1),
                    (Long) row.getOrDefault("currentTransaction", -1),
                    (Integer) row.getOrDefault("n_nationkey", -1),
                    (String) row.getOrDefault("n_name", "INVALID"),
                    (Integer) row.getOrDefault("n_regionkey", -1),
                    (String) row.getOrDefault("n_comment", "INVALID"));
        }

        public NationRow(int operation, long originalTransaction, int bucket, long rowId, long currentTransaction, int nationkey, String name, int regionkey, String comment)
        {
            this.operation = operation;
            this.originalTransaction = originalTransaction;
            this.bucket = bucket;
            this.rowId = rowId;
            this.currentTransaction = currentTransaction;
            this.nationkey = nationkey;
            this.name = name;
            this.regionkey = regionkey;
            this.comment = comment;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operation, originalTransaction, bucket, rowId, currentTransaction, name, nationkey, regionkey, comment);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof NationRow)) {
                return false;
            }

            NationRow other = (NationRow) obj;
            return (operation == other.operation
                    && originalTransaction == other.originalTransaction
                    && bucket == other.bucket
                    && rowId == other.rowId
                    && currentTransaction == other.currentTransaction
                    && nationkey == other.nationkey
                    && name.equals(other.name)
                    && regionkey == other.regionkey
                    && comment.equals(other.comment));
        }
    }
}
