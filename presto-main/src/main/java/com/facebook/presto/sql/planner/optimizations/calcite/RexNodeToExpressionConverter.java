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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.List;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by shubham on 13/04/17.
 *
 * This the reverse of ExpressionToRexNodeConverter class and any addition done there should have a reverse logic added here
 * This should be instantiated for each node to be converted as inputSymbols will vary
 * This SHOULD NOT BE USED ACROSS NODES
 */
public class RexNodeToExpressionConverter
{
    /*
     * inputSymbols: Symbol at index i in this list is the outputSymbol of the Source PlanNode (converted from RelNode) for
     * the corresponding Field in output of source RelNode
     * i.e.
     * For field at position 'i' in rowType in Source RelNode, the corresponding symbol in coverted PlanNode is kept at index 'i'
     * in inputSymbols
     */
    List<Symbol> inputSymbols;

    public RexNodeToExpressionConverter(List<Symbol> inputSymbols)
    {
        this.inputSymbols = inputSymbols;
    }

    /*
     * This is used when a node has multiple children
     * This should be called for each child in the same order in which InputContext added to ExpressionToRexNodeConverter
     * E.g. for join ExpressionToRexNodeConverter is called for leftChild and then addInputContext for right child
     * So when converting Calcite Join node to Presto Join node, symbols of left child should added first and then right child's
     */
    public RexNodeToExpressionConverter addSymbols(List<Symbol> inputSymbols)
    {
        this.inputSymbols = ImmutableList
                .<Symbol>builder()
                .addAll(this.inputSymbols)
                .addAll(inputSymbols)
                .build();
        return this;
    }

    public Expression convert(RexNode node)
    {
        if (node instanceof RexCall) {
            return getPrestoFunctionCall((RexCall) node);
        }

        if (node instanceof RexInputRef) {
            Symbol sourceSymbol = inputSymbols.get(((RexInputRef) node).getIndex());
            return sourceSymbol.toSymbolReference();
        }

        if (node instanceof RexLiteral) {
            return convert((RexLiteral) node);
        }

        throw new UnsupportedOperationException("Unknown RexNode type: " + node.getClass());
    }

    public Expression getPrestoFunctionCall(RexCall node)
    {
        ImmutableList.Builder builder = new ImmutableList.Builder();
        for (RexNode operand : node.getOperands()) {
            builder.add(convert(operand));
        }

        switch (node.getOperator().getKind()) {
            case EQUALS:
                return getPrestoComparisonExpression(ComparisonExpressionType.EQUAL, builder.build());
            case LESS_THAN:
                return getPrestoComparisonExpression(ComparisonExpressionType.LESS_THAN, builder.build());
            case LESS_THAN_OR_EQUAL:
                return getPrestoComparisonExpression(ComparisonExpressionType.LESS_THAN_OR_EQUAL, builder.build());
            case GREATER_THAN:
                return getPrestoComparisonExpression(ComparisonExpressionType.GREATER_THAN, builder.build());
            case GREATER_THAN_OR_EQUAL:
                return getPrestoComparisonExpression(ComparisonExpressionType.GREATER_THAN_OR_EQUAL, builder.build());
            case NOT_EQUALS:
                return getPrestoComparisonExpression(ComparisonExpressionType.NOT_EQUAL, builder.build());
            case IS_DISTINCT_FROM:
                return getPrestoComparisonExpression(ComparisonExpressionType.IS_DISTINCT_FROM, builder.build());
            case OTHER_FUNCTION:
                SqlFunction function = checkType(node.getOperator(), SqlFunction.class, "SqlFunction");
                checkState(function.getFunctionType().equals(SqlFunctionCategory.USER_DEFINED_FUNCTION), "Non USER_DEFINED_FUNCTION never created while creating calcite plan");
                // TODO support windowing, distinct etc
                return new FunctionCall(QualifiedName.of(function.getName()), builder.build());

            case CAST:
                checkState(node.getOperands().size() == 1, "More than one operand in cast: " + node.getOperands().size());
                return new Cast(
                        convert(node.getOperands().get(0)),
                        TypeConverter.convert(node.getType()).getDisplayName()
                );
            case AND:
                return getPrestoLogicalBinaryExpression(LogicalBinaryExpression.Type.AND, builder.build());
            case OR:
                return getPrestoLogicalBinaryExpression(LogicalBinaryExpression.Type.OR, builder.build());
            default:
                throw new UnsupportedOperationException("Unknown operation: " + node.getOperator());
        }
    }

    private ComparisonExpression getPrestoComparisonExpression(ComparisonExpressionType type, List<Expression> operands)
    {
        checkState(operands.size() == 2, String.format("More than two operands [%d] for comparison operator %s", operands.size(), type));
        return new ComparisonExpression(type, operands.get(0), operands.get(1));
    }

    private LogicalBinaryExpression getPrestoLogicalBinaryExpression(LogicalBinaryExpression.Type type, List<Expression> operands)
    {
        // Calcite support n-ary binary expression
        checkState(operands.size() >= 2, String.format("More than two operands [%d] for binary logical operator %s", operands.size(), type));
        LogicalBinaryExpression expression = new LogicalBinaryExpression(type, operands.get(0), operands.get(1));
        for (int i = 2; i < operands.size(); i++) {
            expression = new LogicalBinaryExpression(type, expression, operands.get(i));
        }
        return expression;
    }

    private Literal convert(RexLiteral literal)
    {
        SqlTypeName sqlType = literal.getType().getSqlTypeName();

        switch (sqlType) {
            case BINARY:
                ByteString byteString = (ByteString) literal.getValue();
                return new BinaryLiteral(byteString.toString());

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                BigDecimal longValue = (BigDecimal) literal.getValue3();
                return new LongLiteral(longValue.toString());

            case DOUBLE:
                BigDecimal doubleValue = (BigDecimal) literal.getValue3();
                return new DoubleLiteral(doubleValue.toString());

            case DECIMAL:
                BigDecimal decimalValue = (BigDecimal) literal.getValue3();
                return new DecimalLiteral(decimalValue.toString());

            case FLOAT:
            case REAL:
                BigDecimal realValue = (BigDecimal) literal.getValue();
                return new GenericLiteral("real", realValue.toString());

            case VARCHAR:
            case CHAR:
                // TODO: how to differentiate between CHAR, STRING, JSON Literal of Presto as both are converted to CHAR calcite type
                String stringValue = (String) literal.getValue3();
                return new CharLiteral(stringValue);

            case BOOLEAN:
                String booleanValue = ((Boolean) literal.getValue3()) ? "true" : "false";
                return new BooleanLiteral(booleanValue);

            case DATE:
                int epchDays = (Integer) literal.getValue3();
                return new GenericLiteral("date", LocalDate.ofEpochDay(epchDays).toString());

            case TIME:
                Calendar calendar = (Calendar) literal.getValue();
                String timeValue = String.format("%s %s", calendar.toInstant().atZone(calendar.getTimeZone().toZoneId()).toLocalTime(), calendar.getTimeZone().getID());
                return new TimeLiteral(timeValue);

            case TIMESTAMP:
                Calendar timeStampCalendar = (Calendar) literal.getValue();
                DateTime dt = new DateTime(timeStampCalendar.getTime());
                DateTimeFormatter dateFormatter = ISODateTimeFormat.date().withZone(DateTimeZone.forID(timeStampCalendar.getTimeZone().toZoneId().getId()));
                DateTimeFormatter timeFormatter = ISODateTimeFormat.time().withZone(DateTimeZone.forID(timeStampCalendar.getTimeZone().toZoneId().getId()));
                String timestampValue = String.format("%s %s", dateFormatter.print(dt), timeFormatter.print(dt));
                return new TimestampLiteral(timestampValue);

            case INTERVAL_YEAR_MONTH:
                int months = ((BigDecimal) literal.getValue()).intValue();
                return new IntervalLiteral(Integer.toString(months), IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.MONTH);

            case INTERVAL_DAY_TIME:
                int millis = ((BigDecimal) literal.getValue()).intValue();
                return new IntervalLiteral(Double.toString((double) millis / 1000), IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.SECOND);

            case ANY:
                return new NullLiteral();

            default:
                throw new UnsupportedOperationException("Unsupported Type: " + sqlType);
        }
    }
}
