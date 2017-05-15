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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Collections.emptyList;

/**
 * Created by shubham on 24/03/17.
 */
public class ExpressionToRexNodeConverter
{
    RelOptCluster cluster;
    List<InputContext> inputContexts = new ArrayList();
    Metadata metadata;
    Session session;
    SymbolAllocator symbolAllocator;

    /*
     * inpDataType is the RowType for the result of the source of the node for which ExpressionToRexNodeConverter is being instantiated
     * nameToPos map is the mapping of the symbol name to the position in inpDataType i.e which inpDataType entry is for a particular presto symbol
     */
    public ExpressionToRexNodeConverter(Metadata metadata, SymbolAllocator symbolAllocator, Session session, RelOptCluster cluster, RelDataType inpDataType,
                                        ImmutableMap<String, Integer> nameToPosMap, int offset)
    {
        this.metadata = metadata;
        this.symbolAllocator = symbolAllocator;
        this.session = session;
        this.cluster = cluster;
        this.inputContexts.add(new InputContext(inpDataType, nameToPosMap, offset));
    }

    public ExpressionToRexNodeConverter addInputContext(RelDataType inpDataType, ImmutableMap<String, Integer> nameToPosMap)
    {
        InputContext lastContext = Iterables.getLast(inputContexts);
        int offset = lastContext.offsetInCalciteSchema + lastContext.calciteInpDataType.getFieldCount();

        inputContexts.add(new InputContext(inpDataType, nameToPosMap, offset));
        return this;
    }

    public RexNode convert(Expression expr)
    {
        // TODO what apart from comparisonExression can come in Predicate
        if (expr instanceof ComparisonExpression) {
            RexNode left = convert(((ComparisonExpression) expr).getLeft());
            RexNode right = convert(((ComparisonExpression) expr).getRight());
            SqlOperator calciteOperator = SqlFunctionConverter.getCalciteComparisonOperator((ComparisonExpression) expr);
            return cluster.getRexBuilder().makeCall(calciteOperator, ImmutableList.of(left, right));
        }

        if (expr instanceof SymbolReference) {
            return convert((SymbolReference) expr);
        }

        if (expr instanceof Literal) {
            return convert((Literal) expr);
        }

        if (expr instanceof FunctionCall) {
            return convert((FunctionCall) expr);
        }

        if (expr instanceof Cast) {
            RexNode childExpr = convert(((Cast) expr).getExpression());
            Type type = metadata.getType(parseTypeSignature(((Cast) expr).getType()));
            // do not call makeCast instaed of makeAbstractCast as it can remove cast alltogether e.g. when casting integer to bigint
            return cluster.getRexBuilder().makeAbstractCast(TypeConverter.convert(type, cluster), childExpr);
        }

        if (expr instanceof LogicalBinaryExpression) {
            SqlOperator calciteOperator = SqlFunctionConverter.getCalciteLogicalOperator((LogicalBinaryExpression) expr);
            return cluster.getRexBuilder().makeCall(
                    calciteOperator,
                    ImmutableList.of(
                            convert(((LogicalBinaryExpression) expr).getLeft()),
                            convert(((LogicalBinaryExpression) expr).getRight())
                    )
            );
        }
        // arrayConstructor for array ['1', '2']

        // Can be CurrentTime for current_time

        // Can be Row type for row literal

        throw new UnsupportedOperationException("Unsupported Expression: " + expr);
    }

    private RexNode convert(FunctionCall functionCall)
    {
        // TODO support windowing, distinct

        Map<Expression, Type> types = ExpressionAnalyzer.getExpressionTypes(session, metadata, new SqlParser(), symbolAllocator.getTypes(), functionCall, emptyList());

        RelDataType returnType = typeSignatureToRelDataType(types.get(functionCall).getTypeSignature());
        List<RexNode> childNodes = Lists.transform(functionCall.getArguments(),
                arg -> convert(arg));
        List<RelDataType> args = childNodes.stream().map(node -> node.getType()).collect(Collectors.toList());

        SqlOperandTypeChecker operandTypeChecker = OperandTypes.family(Lists.transform(args,
                arg -> Util.first(arg.getSqlTypeName().getFamily(), SqlTypeFamily.ANY)));
        SqlOperandTypeInference operandTypeInference = InferTypes.explicit(args);
        SqlReturnTypeInference returnTypeInference = ReturnTypes.explicit(returnType);
        SqlFunction sqlFunction = new SqlFunction(
                functionCall.getName().toString(),
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);

        return cluster.getRexBuilder().makeCall(sqlFunction, childNodes);
    }

    private RelDataType typeSignatureToRelDataType(TypeSignature typeSignature)
    {
        return TypeConverter.convert(metadata.getType(typeSignature), cluster);
    }

    private RexNode convert(Literal literal)
    {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
        LiteralConverter literalConverter = new LiteralConverter(metadata, cluster);
        return literal.accept(literalConverter, session.toConnectorSession());
    }

    private RexNode convert(SymbolReference symbol)
    {
        InputContext inputContext = getInputContext(symbol);
        int pos = inputContext.prestoNameToPosMap.get(symbol.getName());
        return cluster.getRexBuilder().makeInputRef(
                inputContext.calciteInpDataType.getFieldList().get(pos).getType(),
                pos + inputContext.offsetInCalciteSchema);
    }

    private InputContext getInputContext(SymbolReference symbol)
    {
        if (inputContexts.size() == 1) {
            return inputContexts.get(0);
        }

        for (InputContext inputContext : inputContexts) {
            if (inputContext.prestoNameToPosMap.containsKey(symbol.getName())) {
                return inputContext;
            }
        }

        throw new UnsupportedOperationException("Symbol not found in source nodes: " + symbol.getName());
    }

    private static class InputContext
    {
        private final RelDataType calciteInpDataType;
        private final ImmutableMap<String, Integer> prestoNameToPosMap;
        private final int offsetInCalciteSchema;

        private InputContext(RelDataType calciteInpDataType, ImmutableMap<String, Integer> prestoNameToPosMap, int offsetInCalciteSchema)
        {
            this.calciteInpDataType = calciteInpDataType;
            this.prestoNameToPosMap = prestoNameToPosMap;
            this.offsetInCalciteSchema = offsetInCalciteSchema;
        }
    }
}
