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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertTrue;

/*
 * Reads a ACID file with single data column and having single row in dataset
 *
 * Table Schema: col:integer
 * Content of read file:
 *  {"operation":0,"originalTransaction":2,"bucket":536870912,"rowId":0,"currentTransaction":2,"row":{"col":2}}
 */
public class TestSimpleACIDFileRead
{
    private String filename = "fullacid_basefile_singleCol_singleRow.orc";
    private List<String> columnNames = ImmutableList.of("col");
    private List<Type> columnTypes = ImmutableList.of(IntegerType.INTEGER);

    private ConnectorPageSource pageSource;

    @BeforeTest
    public void setup()
    {
        pageSource = AcidPageProcessorProvider.getAcidPageSource(filename, columnNames, columnTypes);
    }

    @Test
    public void testRead()
    {
        List<Page> pages = new ArrayList();
        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page != null) {
                pages.add(page.getLoadedPage());
            }
        }
        assertTrue(pages.size() == 1, "Did not read required pages");
        Page page = pages.get(0);
        assertTrue(page.getChannelCount() == 6, "Did not read required number of blocks: " + page.getChannelCount());
        assertTrue(page.getPositionCount() == 1, "Did not read required number of rows: " + page.getPositionCount());

        // check operation is read correctly
        int operation = page.getBlock(0).getInt(0, 0);
        long originalTransaction = page.getBlock(1).getLong(0, 0);
        int bucket = page.getBlock(2).getInt(0, 0);
        long rowId = page.getBlock(3).getLong(0, 0);
        long currentTransaction = page.getBlock(4).getLong(0, 0);
        int col = page.getBlock(5).getInt(0, 0);

        assertTrue(operation == 0, "Unexpected value of operation: " + operation);
        assertTrue(originalTransaction == 2, "Unexpected value of originalTransaction: " + originalTransaction);
        assertTrue(bucket == 536870912, "Unexpected value of bucket: " + bucket);
        assertTrue(rowId == 0, "Unexpected value of rowId: " + rowId);
        assertTrue(currentTransaction == 2, "Unexpected value of currentTransaction: " + currentTransaction);
        assertTrue(col == 2, "Unexpected value of col: " + col);
    }
}
