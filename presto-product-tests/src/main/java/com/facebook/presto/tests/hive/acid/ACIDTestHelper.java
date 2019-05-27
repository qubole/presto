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
import io.prestodb.tempto.context.ThreadLocalTestContextHolder;
import io.prestodb.tempto.query.QueryExecutor;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

public class ACIDTestHelper
{
    private ACIDTestHelper()
    {
    }

    /*
     * This simulates a aborted transaction which leaves behind a file in a partition of a table as follows:
     *  1. Open Txn
     *  2. Rollback Txn
     *  3. Create a new table with location as the given partition location + delta directory for the rolledback transaction
     *  4. Insert something to it which will create a file for this table
     *  5. This file is invalid for the original table because the transaction is aborted
     */
    public static void simulateAbortedHiveTranscation(String database, String tableName, String partitionSpec)
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

    private static String getNewTableName()
    {
        return "table_" + UUID.randomUUID().toString().replace('-', '_');
    }

    private static ThriftHiveMetastoreClient createMetastoreClient()
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
