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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.TableType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveUtil.hiveColumnHandles;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static org.testng.Assert.assertTrue;

// This ensures that isValid column is returned as last column in ACID tables
// We depend on this assumption to create FileterNode on last column of ACID table in RelationPlanner
public class TestColumnHandlesForACID
{
    @Test
    public void testLastColumnIsValid()
    {
        List<Column> columns = ImmutableList.of(new Column("dummy", HiveType.valueOf("smallint"), Optional.empty()));
        Table table = Table.builder()
                .setDatabaseName("default")
                .setTableName("myTable")
                .setOwner("presto")
                .setTableType(TableType.MANAGED_TABLE.name())
                .setParameters(ImmutableMap.of(
                        TABLE_IS_TRANSACTIONAL, "true"))
                .setDataColumns(columns)
                .withStorage(storage -> storage
                        .setLocation("file:///dummuy/location")
                        .setStorageFormat(fromHiveStorageFormat(ORC))
                        .setSerdeParameters(ImmutableMap.of()))
                .build();
        List<HiveColumnHandle> columnHandles = hiveColumnHandles(table);
        assertTrue(columnHandles.get(columnHandles.size() - 1).getName().equalsIgnoreCase("$isValid"));
    }
}
