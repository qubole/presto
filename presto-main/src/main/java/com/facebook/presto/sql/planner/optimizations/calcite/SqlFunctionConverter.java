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
package com.facebook.presto.sql.planner.optimizations.calcite;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Created by shubham on 03/04/17.
 */
public class SqlFunctionConverter
{
    private SqlFunctionConverter()
    {
    }

    public static SqlOperator getCalciteComparisonOperator(ComparisonExpression expression)
    {
        switch (expression.getType()) {
            case EQUAL:
                return SqlStdOperatorTable.EQUALS;
            case LESS_THAN:
                return SqlStdOperatorTable.LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return SqlStdOperatorTable.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            case NOT_EQUAL:
                return SqlStdOperatorTable.NOT_EQUALS;
            case IS_DISTINCT_FROM:
                return SqlStdOperatorTable.IS_DISTINCT_FROM;
            default:
                throw new UnsupportedOperationException("Comparision type node found " + expression.getType());
        }
    }

    public static SqlOperator getCalciteLogicalOperator(LogicalBinaryExpression expression)
    {
        switch (expression.getType()) {
            case AND:
                return SqlStdOperatorTable.AND;
            case OR:
                return SqlStdOperatorTable.OR;
            default:
                    throw new UnsupportedOperationException("Binary Expression not supported: " + expression.getType());
        }
    }
}
