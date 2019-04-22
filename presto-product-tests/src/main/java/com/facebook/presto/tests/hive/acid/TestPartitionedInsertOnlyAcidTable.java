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
package com.facebook.presto.tests.hive.acid;

import com.facebook.presto.hive.authentication.NoHiveMetastoreAuthentication;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.Transport;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.context.ThreadLocalTestContextHolder;
import io.prestodb.tempto.fulfillment.table.MutableTablesState;
import io.prestodb.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestodb.tempto.query.QueryExecutor;
import io.prestodb.tempto.query.QueryResult;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.tests.TestGroups.HIVE_ACID;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.prestodb.tempto.Requirements.compose;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class TestPartitionedInsertOnlyAcidTable
        extends ProductTest
        implements RequirementsProvider
{
    private static String tableName = "ORC_acid_single_int_column_partitioned";

    private static final HiveTableDefinition SINGLE_INT_COLUMN_PARTITIONED_ORC = singleIntColumnPartitionedTableDefinition(Optional.empty());

    private static HiveTableDefinition singleIntColumnPartitionedTableDefinition(Optional<String> serde)
    {
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate(buildSingleIntColumnPartitionedTableDDL(serde))
                .setNoData()
                .build();
    }

    private static String buildSingleIntColumnPartitionedTableDDL(Optional<String> rowFormat)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE %EXTERNAL% TABLE IF NOT EXISTS %NAME%(");
        sb.append("   col INT");
        sb.append(") ");
        sb.append("PARTITIONED BY (part_col INT) ");
        if (rowFormat.isPresent()) {
            sb.append("ROW FORMAT ").append(rowFormat.get());
        }
        sb.append(" STORED AS ORC");
        sb.append(" TBLPROPERTIES ('transactional_properties'='insert_only', 'transactional'='true')");
        return sb.toString();
    }

    @Inject
    private MutableTablesState tablesState;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(mutableTable(SINGLE_INT_COLUMN_PARTITIONED_ORC));
    }

    @Test(groups = {HIVE_ACID, PROFILE_SPECIFIC_TESTS})
    public void testReadingPartitionedInsertOnlyACIDTable()
    {
        String tableNameInDatabase = tablesState.get(tableName).getNameInDatabase();

        QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");
        hiveQueryExecutor.executeQuery(
                "INSERT OVERWRITE TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 1");

        String selectFromOnePartitionsSql = "SELECT * FROM " + tableNameInDatabase + " WHERE part_col = 2";
        QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(1, 2));

        hiveQueryExecutor.executeQuery(
                "INSERT INTO TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 2");
        onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).hasRowsCount(2);

        hiveQueryExecutor.executeQuery(
                "INSERT OVERWRITE TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 3");
        onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(3, 2));
    }

    @Test(groups = {HIVE_ACID, PROFILE_SPECIFIC_TESTS})
    public void testSelectOnCompactedTable()
    {
        /*
         * This test is not implemented because hdp3 has https://issues.apache.org/jira/browse/HIVE-21280 bug because of which manual compaction
         * command (in step 3 below) fails. This will be added when we upgrade to hive version with this fix
         *
         * 1. Fire a bunch of Insert overwrite and Insert into queries on a partition
         * 2. Issue alter table compact 'major' and wait
         * 3. Once compaction finishes, verify that correct data is read
         */
    }

    @Test(groups = {HIVE_ACID, PROFILE_SPECIFIC_TESTS})
    public void testFullACIDTableShouldFail()
    {
        QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");
        String fullAcidTable = getNewTableName();
        hiveQueryExecutor.executeQuery(
                String.format("CREATE TABLE %s (col string) stored as ORC TBLPROPERTIES ('transactional'='true')",
                fullAcidTable));
        assertThat(() -> query("SELECT * FROM " + fullAcidTable))
                .failsWithMessage(format("Reading from Full ACID tables are not supported: default.%s", fullAcidTable));
    }

    @Test(groups = {HIVE_ACID, PROFILE_SPECIFIC_TESTS})
    public void testInsertShouldFail()
    {
        String tableNameInDatabase = tablesState.get(tableName).getNameInDatabase();

        assertThat(() -> query("INSERT INTO " + tableNameInDatabase + " SELECT 1, 2"))
                .failsWithMessage(format("Inserting into Hive Transcational tables is not supported. default: %s", tableNameInDatabase));
    }

    @Test(groups = {HIVE_ACID, PROFILE_SPECIFIC_TESTS})
    public void testFilesForAbortedTransactionsIsIgnored()
            throws TException
    {
        String tableNameInDatabase = tablesState.get(tableName).getNameInDatabase();
        String databaseName = "default";

        QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");
        hiveQueryExecutor.executeQuery(
                "INSERT INTO TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 0");

        hiveQueryExecutor.executeQuery(
                "INSERT OVERWRITE TABLE " + tableNameInDatabase + " PARTITION (part_col=2) select 1");

        String selectFromOnePartitionsSql = "SELECT * FROM " + tableNameInDatabase + " WHERE part_col = 2";
        QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(1, 2));

        // Simulate aborted transaction in Hive which has left behind a write directory and file
        simulateAbortedHiveTranscation(databaseName, tableNameInDatabase, "part_col=2");

        // Above simluation would have written to the part_col a new delta directory that corresponds to a aborted txn but it should not be read
        onePartitionQueryResult = query(selectFromOnePartitionsSql);
        assertThat(onePartitionQueryResult).containsOnly(row(1, 2));
    }

    /*
     * This simulates a aborted transaction which leaves behind a file in a partition of a table as follows:
     *  1. Open Txn
     *  2. Rollback Txn
     *  3. Create a new table with location as the given partition location + delta directory for the rolledback transaction
     *  4. Insert something to it which will create a file for this table
     *  5. This file is invalid for the original table because the transaction is aborted
     */
    private void simulateAbortedHiveTranscation(String database, String tableName, String partitionSpec)
            throws TException
    {
        ThriftHiveMetastoreClient client = createMetastoreClient();
        try {
            // 1.
            long transaction = client.openTxn("test");

            AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(database, tableName);
            rqst.setTxnIds(Collections.singletonList(transaction));
            long writeId = client.allocateTableWriteIdsBatchIntr(rqst).get(0).getWriteId();

            // 2.
            client.rollbackTxn(transaction);

            // 3.
            Table t = client.getTable(database, tableName);
            String tableLocation = t.getSd().getLocation();
            String partitionLocation = tableLocation.endsWith("/") ? tableLocation + partitionSpec : tableLocation + "/" + partitionSpec;
            String deltaLocation = String.format("%s/delta_%s_%s_0000/",
                    partitionLocation,
                    String.format("%07d", writeId),
                    String.format("%07d", writeId));
            QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");

            String tmpTableName = getNewTableName();
            hiveQueryExecutor.executeQuery(String.format("CREATE EXTERNAL TABLE %s (col string) stored as ORC location '%s'",
                    tmpTableName,
                    deltaLocation));

            // 4.
            hiveQueryExecutor.executeQuery(String.format("INSERT INTO TABLE %s SELECT 'a'", tmpTableName));
            hiveQueryExecutor.executeQuery(String.format("DROP TABLE %s", tmpTableName));
        }
        finally {
            client.close();
        }
    }

    private String getNewTableName()
    {
        return "table_" + UUID.randomUUID().toString().replace('-', '_');
    }
    private ThriftHiveMetastoreClient createMetastoreClient()
            throws TException
    {
        URI metastore = URI.create("thrift://hadoop-master:9083");
        return new ThriftHiveMetastoreClient(
                Transport.create(
                    HostAndPort.fromParts(metastore.getHost(), metastore.getPort()),
                    Optional.empty(),
                    Optional.empty(),
                    10000,
                    new NoHiveMetastoreAuthentication()));
    }
}
