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
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.tests.TestGroups.HIVE_ACID;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.prestodb.tempto.Requirements.compose;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestUnpartitionedFullAcidTable
        extends ProductTest
        implements RequirementsProvider
{
    private static String tableName = "ORC_acid_single_int_column";

    private static final HiveTableDefinition SINGLE_INT_COLUMN_ORC = singleIntColumnTableDefinition(Optional.empty());

    private static HiveTableDefinition singleIntColumnTableDefinition(Optional<String> serde)
    {
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate(buildSingleIntColumnTableDDL(serde))
                .setNoData()
                .build();
    }

    private static String buildSingleIntColumnTableDDL(Optional<String> rowFormat)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE %EXTERNAL% TABLE IF NOT EXISTS %NAME%(");
        sb.append("   col INT,");
        sb.append("   fcol INT");
        sb.append(") ");
        if (rowFormat.isPresent()) {
            sb.append("ROW FORMAT ").append(rowFormat.get());
        }
        sb.append(" STORED AS ORC");
        sb.append(" TBLPROPERTIES ('transactional'='true')");
        return sb.toString();
    }

    @Inject
    private MutableTablesState tablesState;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(mutableTable(SINGLE_INT_COLUMN_ORC));
    }

    @Test(groups = {HIVE_ACID, PROFILE_SPECIFIC_TESTS})
    public void testReadingUnpartitionedInsertOnlyACIDTable()
    {
        String tableNameInDatabase = tablesState.get(tableName).getNameInDatabase();

        QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");
        hiveQueryExecutor.executeQuery(
                "INSERT OVERWRITE TABLE " + tableNameInDatabase + " select 1, 11");

        String selectAllFromTable = "SELECT * FROM " + tableNameInDatabase;
        QueryResult onePartitionQueryResult = query(selectAllFromTable);
        assertThat(onePartitionQueryResult).containsOnly(row(1, 11));

        hiveQueryExecutor.executeQuery(
                "INSERT INTO TABLE " + tableNameInDatabase + " select 2, 22");
        onePartitionQueryResult = query(selectAllFromTable);
        assertThat(onePartitionQueryResult).hasRowsCount(2);

        // delete a row
        hiveQueryExecutor.executeQuery(
                "DELETE FROM " + tableNameInDatabase + " WHERE fcol=22");
        onePartitionQueryResult = query(selectAllFromTable);
        assertThat(onePartitionQueryResult).containsOnly(row(1, 11));

        // update the existing row
        hiveQueryExecutor.executeQuery(
                "UPDATE " + tableNameInDatabase + " set col = 3 where fcol=11");
        onePartitionQueryResult = query(selectAllFromTable);
        assertThat(onePartitionQueryResult).containsOnly(row(3, 11));
    }
}
