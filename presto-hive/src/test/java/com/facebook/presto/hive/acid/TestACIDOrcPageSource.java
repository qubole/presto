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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.BooleanType;
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
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.acid.AcidPageProcessorProvider.addNationTableDeleteDeltas;
import static org.testng.Assert.assertTrue;

/*
 * This class tests reading of ACID ORC File as it would be in actual queries
 * It tests ACIDOrcPageSource which returns Page with data blocks and isValid block and hides all ACID related metadata cols from exposure to callers
 */
public class TestACIDOrcPageSource
{
    private String filename = "nationFile25kRowsSortedOnNationKey.orc";
    private List<String> columnNames = ImmutableList.of("n_nationkey", "n_name", "n_regionkey", "n_comment");
    private List<Type> columnTypes = ImmutableList.of(IntegerType.INTEGER, VarcharType.VARCHAR, IntegerType.INTEGER, VarcharType.VARCHAR);

    @Test
    public void testFullFileReadWithoutDeleteDeltas()
            throws IOException
    {
        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, columnNames, columnTypes);
        List<AcidNationRow> rows = readFileCols(pageSource, columnNames, columnTypes, true);

        List<AcidNationRow> expected = getExpectedResult(Optional.empty(), Optional.empty(), Optional.empty());
        assertTrue(Objects.equals(expected, rows));
    }

    @Test
    public void testFullFileReadWithDeleteDeltas()
            throws IOException
    {
        DeleteDeltaLocations deleteDeltaLocations = new DeleteDeltaLocations();
        addNationTableDeleteDeltas(deleteDeltaLocations, 3L, 3L, 0);
        addNationTableDeleteDeltas(deleteDeltaLocations, 4L, 4L, 0);
        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, columnNames, columnTypes, Optional.of(deleteDeltaLocations));
        List<AcidNationRow> rows = readFileCols(pageSource, columnNames, columnTypes, true);

        List<AcidNationRow> expected = getExpectedResult(Optional.empty(), Optional.empty(), Optional.of(ImmutableList.of(5, 19)));
        assertTrue(Objects.equals(expected, rows));
    }

    @Test
    public void testSingleColumnReadWithoutDeleteDeltas()
            throws IOException
    {
        int colToRead = 2;
        ConnectorPageSource pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, ImmutableList.of(columnNames.get(colToRead)), ImmutableList.of(columnTypes.get(colToRead)));
        List<AcidNationRow> rows = readFileCols(pageSource, ImmutableList.of(columnNames.get(colToRead)), ImmutableList.of(columnTypes.get(colToRead)), true);

        List<AcidNationRow> expected = getExpectedResult(Optional.empty(), Optional.of(colToRead), Optional.empty());
        assertTrue(Objects.equals(expected, rows));
    }

    public static List<AcidNationRow> readFileCols(ConnectorPageSource pageSource, List<String> columnNames, List<Type> columnTypes, boolean resultsNeeded)
    {
        List<AcidNationRow> rows = new ArrayList(resultsNeeded ? 25000 : 0);
        ImmutableList.Builder<Type> expectedReadTypesBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> expectedReadNamesBuilder = ImmutableList.builder();
        expectedReadTypesBuilder.addAll(columnTypes);
        expectedReadNamesBuilder.addAll(columnNames);
        expectedReadTypesBuilder.add(BooleanType.BOOLEAN); // operation
        expectedReadNamesBuilder.add("isValid");

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
                    rows.add(new AcidNationRow(values.build()));
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
    public static List<AcidNationRow> getExpectedResult(Optional<Integer> onlyForRowId, Optional<Integer> onlyForColumnId, Optional<List<Integer>> invalidRows)
            throws IOException
    {
        String nationFilePath = Thread.currentThread().getContextClassLoader().getResource("nation.tbl").getPath();
        final ImmutableList.Builder<AcidNationRow> result = ImmutableList.builder();
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
                boolean isValid = true;
                if (invalidRows.isPresent() && invalidRows.get().contains(lineNum)) {
                    isValid = false;
                }
                rowId += replicateIntoResult(line, result, rowId, onlyForColumnId, isValid);
            }
        }
        finally {
            br.close();
        }
        return result.build();
    }

    public static long replicateIntoResult(String line, ImmutableList.Builder<AcidNationRow> resultBuilder, long startRowId, Optional<Integer> onlyForColumnId, boolean isValid)
    {
        long replicationFactor = 1000; // same way the nationFile25kRowsSortedOnNationKey.orc is created
        for (int i = 0; i < replicationFactor; i++) {
            String[] cols = line.split("\\|");
            resultBuilder.add(new AcidNationRow(
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 0) ? Integer.parseInt(cols[0]) : -1,
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 1) ? cols[1] : "INVALID",
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 2) ? Integer.parseInt(cols[2]) : -1,
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 3) ? cols[3] : "INVALID",
                    isValid));
        }
        return replicationFactor;
    }
}
