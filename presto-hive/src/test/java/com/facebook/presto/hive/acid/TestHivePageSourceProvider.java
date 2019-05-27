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
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceProvider;
import com.facebook.presto.hive.HiveSplit;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.orc.ACIDOrcPageSourceFactory;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.TestingConnectorTransactionHandle;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.facebook.presto.hive.acid.AcidPageProcessorProvider.CONFIG;
import static com.facebook.presto.hive.acid.AcidPageProcessorProvider.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.acid.AcidPageProcessorProvider.SESSION;
import static com.facebook.presto.hive.acid.AcidPageProcessorProvider.addNationTableDeleteDeltas;
import static com.facebook.presto.hive.acid.TestACIDOrcPageSource.getExpectedResult;
import static com.facebook.presto.hive.acid.TestACIDOrcPageSource.readFileCols;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertTrue;

public class TestHivePageSourceProvider
{
    private List<String> columnNames = ImmutableList.of("n_nationkey", "n_name", "n_regionkey", "n_comment", "isValid");
    private List<Type> columnTypes = ImmutableList.of(IntegerType.INTEGER, VarcharType.VARCHAR, IntegerType.INTEGER, VarcharType.VARCHAR, BooleanType.BOOLEAN);

    @Test
    public void testACIDTableWithDeletedRows()
            throws IOException
    {
        DeleteDeltaLocations deleteDeltaLocations = new DeleteDeltaLocations();
        addNationTableDeleteDeltas(deleteDeltaLocations, 3L, 3L, 0);
        addNationTableDeleteDeltas(deleteDeltaLocations, 4L, 4L, 0);

        OrcPageSourceFactory orcPageSourceFactory = new OrcPageSourceFactory(new TypeRegistry(), CONFIG, HDFS_ENVIRONMENT, new FileFormatDataSourceStats());
        ACIDOrcPageSourceFactory acidOrcPageSourceFactory = new ACIDOrcPageSourceFactory(new TypeRegistry(), CONFIG, HDFS_ENVIRONMENT, new FileFormatDataSourceStats(), orcPageSourceFactory);
        HivePageSourceProvider pageSourceProvider = new HivePageSourceProvider(CONFIG, HDFS_ENVIRONMENT, ImmutableSet.of(), ImmutableSet.of(acidOrcPageSourceFactory), new TypeRegistry());

        HiveSplit split = createHiveSplit(deleteDeltaLocations);
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(TestingConnectorTransactionHandle.INSTANCE, SESSION, split, getColumnHandles());
        // readFileCols adds isValid, remove it from passed columns before calling
        List<AcidNationRow> rows = readFileCols(pageSource, columnNames.subList(0, columnNames.size() - 1), columnTypes.subList(0, columnTypes.size() - 1), true);

        assertTrue(rows.size() == 25000, "Unexpected number of rows read: " + rows.size());
        // Out of 25k rows, 2k rows are deleted as per the given delete deltas
        int validRows = 0;
        for (AcidNationRow row : rows) {
            if (row.isValid) {
                validRows++;
            }
        }
        assertTrue(validRows == 23000, "Unexpected number of valid rows read: " + validRows);

        List<AcidNationRow> expected = getExpectedResult(Optional.empty(), Optional.empty(), Optional.of(ImmutableList.of(5, 19)));
        assertTrue(Objects.equals(expected, rows));
    }

    private List<ColumnHandle> getColumnHandles()
    {
        ImmutableList.Builder<ColumnHandle> builder = ImmutableList.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            HiveColumnHandle.ColumnType columnType = ((i == 4) ? HiveColumnHandle.ColumnType.ACID_ROW_VALIDITY : HiveColumnHandle.ColumnType.REGULAR);
            builder.add(new HiveColumnHandle(
                    columnNames.get(i),
                    HiveType.toHiveType(new HiveTypeTranslator(), columnTypes.get(i)),
                    columnTypes.get(i).getTypeSignature(),
                    i,
                    columnType,
                    Optional.empty()));
        }
        return builder.build();
    }

    private HiveSplit createHiveSplit(DeleteDeltaLocations deleteDeltaLocations)
            throws IOException
    {
        Configuration config = new JobConf(new Configuration(false));
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        Path path = new Path(Thread.currentThread().getContextClassLoader().getResource("nationFile25kRowsSortedOnNationKey.orc").getPath().toString());
        FileSystem fs = path.getFileSystem(config);
        FileStatus fileStatus = fs.getFileStatus(path);
        return new HiveSplit("default",
                "nation_acid",
                "UNPARTITIONED",
                path.toString(),
                0,
                fileStatus.getLen(),
                fileStatus.getLen(),
                createSchema(),
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                false,
                TupleDomain.all(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.of(deleteDeltaLocations));
    }

    private Properties createSchema()
    {
        Properties schema = new Properties();
        schema.put(META_TABLE_COLUMNS, "n_nationkey,n_name,n_regionkey,n_comment");
        schema.put(META_TABLE_COLUMN_TYPES, "int:string:int:string");
        schema.put("transactional_properties", "default");
        schema.put(SERIALIZATION_DDL, "struct nation_acid { i32 n_nationkey, string n_name, i32 n_regionkey, string n_comment}");
        schema.put(SERIALIZATION_LIB, "org.apache.hadoop.hive.ql.io.orc.OrcSerde");
        schema.put("transactional", "true");
        return schema;
    }
}
