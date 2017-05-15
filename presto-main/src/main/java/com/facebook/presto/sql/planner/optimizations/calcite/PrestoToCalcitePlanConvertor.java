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
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoFilter;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoJoinNode;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoProject;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoRelNode;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoTableScan;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.RelOptPrestoTable;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;

/**
 * Created by shubham on 23/03/17.
 */
public class PrestoToCalcitePlanConvertor
        extends PlanVisitor<PrestoToCalcitePlanConvertor.PrestoToCalcitePlanContext, RelNode>
{
    private RelOptCluster cluster;
    private Metadata metadata;
    private SymbolAllocator symbolAllocator;
    private Session session;
    private RelOptSchema relOptSchema;

    public PrestoToCalcitePlanConvertor(Metadata metadata, SymbolAllocator symbolAllocator, Session session, RelOptCluster cluster, RelOptSchema schema)
    {
        this.metadata = metadata;
        this.symbolAllocator = symbolAllocator;
        this.session = session;
        this.cluster = cluster;
        this.relOptSchema = schema;
    }

    @Override
    public RelNode visitPlan(PlanNode node, PrestoToCalcitePlanContext context)
    {
        throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
    }

    @Override
    public RelNode visitOutput(OutputNode node, PrestoToCalcitePlanContext context)
    {
        // OutputNode is recreated when calcite plan converted back to presto plan
        return context.rewrite(node.getSource(), this);
    }

    @Override
    public RelNode visitJoin(JoinNode node, PrestoToCalcitePlanContext context)
    {
        RelNode left = context.rewrite(node.getLeft(), this);
        ExpressionToRexNodeConverter converter = new ExpressionToRexNodeConverter(
                metadata,
                symbolAllocator,
                session,
                cluster,
                left.getRowType(),
                context.getColumnMap(left),
                0);

        RelNode right = context.rewrite(node.getRight(), this);
        converter.addInputContext(right.getRowType(), context.getColumnMap(right));

        RexNode filter = null;
        if (node.getFilter().isPresent()) {
            filter = converter.convert(node.getFilter().get());
        }

        ImmutableList.Builder andClausesBuilder = new ImmutableList.Builder();
        for (JoinNode.EquiJoinClause joinClause : node.getCriteria()) {
            // workaround to support EquiJoinClause which uses Symbols instead of SymbolReferences
            RexNode leftOperand = converter.convert(joinClause.getLeft().toSymbolReference());
            RexNode rightOperand = converter.convert(joinClause.getRight().toSymbolReference());
            andClausesBuilder.add(cluster.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, leftOperand, rightOperand));
        }

        if (filter != null) {
            andClausesBuilder.add(filter);
        }

        RexNode joinCondition = createAndCondition(andClausesBuilder.build());

        JoinRelType joinType;

        switch (node.getType()) {
            case FULL:
                joinType = JoinRelType.FULL;
                break;
            case LEFT:
                joinType = JoinRelType.LEFT;
                break;
            case INNER:
                joinType = JoinRelType.INNER;
                break;
            case RIGHT:
                joinType = JoinRelType.RIGHT;
                break;
            default:
                throw new UnsupportedOperationException("Unkown join type: " + node.getType());
        }

        PrestoJoinNode joinNode = new PrestoJoinNode(
                cluster,
                cluster.traitSetOf(PrestoRelNode.CONVENTION, RelCollations.EMPTY),
                left,
                right,
                joinCondition,
                joinType);

        ImmutableMap.Builder builder = new ImmutableMap.Builder();
        for (int pos = 0; pos < node.getOutputSymbols().size(); pos++) {
            builder.put(node.getOutputSymbols().get(pos).getName(), pos);
        }
        context.addRelNodeColumnMap(joinNode, builder.build());

        return joinNode;
    }

    @Override
    public RelNode visitProject(ProjectNode node, PrestoToCalcitePlanContext context)
    {
        RelNode source = context.rewrite(node.getSource(), this);

        ExpressionToRexNodeConverter converter = new ExpressionToRexNodeConverter(metadata, symbolAllocator, session, cluster, source.getRowType(), context.getColumnMap(source), 0);

        ImmutableList.Builder projections = ImmutableList.builder();
        ImmutableList.Builder colNames = ImmutableList.builder();
        ImmutableMap.Builder posMapBuilder = ImmutableMap.builder();

        int pos = 0;
        for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
            projections.add(converter.convert(entry.getValue()));
            colNames.add(entry.getKey().getName());
            posMapBuilder.put(entry.getKey().getName(), pos++);
        }

        List<RexNode> projects = projections.build();
        List<String> names = colNames.build();

        RelNode project = new PrestoProject(
                cluster,
                cluster.traitSetOf(PrestoRelNode.CONVENTION, RelCollations.EMPTY),
                source,
                projects,
                RexUtil.createStructType(cluster.getTypeFactory(), projects, names));

        context.addRelNodeColumnMap(project, posMapBuilder.build());
        return project;
    }

    @Override
    public RelNode visitFilter(FilterNode node, PrestoToCalcitePlanContext context)
    {
        RelNode source = context.rewrite(node.getSource(), this);

        Expression predicate = node.getPredicate();
        // predicate can be one of these:
        // ComparisonExpression
        // InPredicate for in list
        // InPredicate for in subquery

        RexNode convertedPredicate = new ExpressionToRexNodeConverter(metadata, symbolAllocator, session, cluster, source.getRowType(), context.getColumnMap(source), 0)
                .convert(predicate);

        // TODO: understand this part, doing it like Hive right now
        RexNode factoredFilterExpr = RexUtil.pullFactors(cluster.getRexBuilder(), convertedPredicate);

        RelNode filterNode = new PrestoFilter(cluster, cluster.traitSetOf(PrestoRelNode.CONVENTION),
                source, factoredFilterExpr);

        context.addRelNodeColumnMap(filterNode, context.getColumnMap(source));

        return filterNode;
    }

    @Override
    public RelNode visitTableScan(TableScanNode node, PrestoToCalcitePlanContext context)
    {
        TableMetadata tableMetadata = metadata.getTableMetadata(session, node.getTable());
        SchemaTableName schemaTableName = tableMetadata.getTable();
        List<ColumnMetadata> columns = new ArrayList(node.getOutputSymbols()); //tableMetadata.getColumns();

        // TableScan can output colA_0 for the corresponding column colA in table when another TableScan has used up colA symbol
        // We tell Calcite that the table has columns with the name that TableScan of Presto would have aliased it to
        // This is fine till we just reuse the original TableScan in CalciteToPrestoPlanConverter as this original TableScan
        // has the right column metadata.
        // If we change this logic in CalciteToPrestoPlanConverter then we need to make changes here to create Calcite TableScan
        // which will have right table column info and some assignment s.t. rowType has the new symbol name
        RelOptPrestoTable table = new RelOptPrestoTable(
                relOptSchema,
                schemaTableName.getTableName(),
                TypeConverter.getType(
                        cluster,
                        node.getOutputSymbols(),
                        node.getOutputSymbols().stream().map(symbol -> symbolAllocator.getTypes().get(symbol)).collect(Collectors.toList())
                )
        );
        PrestoTableScan tableScan = new PrestoTableScan(cluster, cluster.traitSetOf(PrestoRelNode.CONVENTION), table, node);

        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<String, Integer>();
        int idx = 0;
        for (Symbol column : node.getAssignments().keySet()) {
            builder.put(column.getName(), idx++);
        }
        context.addRelNodeColumnMap(tableScan, builder.build());
        return tableScan;
    }

    // Merges conditions into one RexNode with AND operator
    private RexNode createAndCondition(List<RexNode> conditions)
    {
        if (conditions.size() == 0) {
            // TODO Is this the right way to handle this?
            return cluster.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
                    ImmutableList.of(cluster.getRexBuilder().makeLiteral(true),
                            cluster.getRexBuilder().makeLiteral(true)));
        }

        if (conditions.size() == 1) {
            return conditions.get(0);
        }

        RexNode node = generateAndNode(cluster.getRexBuilder(), conditions.get(0), conditions.get(1));
        if (conditions.size() == 2) {
            return node;
        }

        for (RexNode condition : conditions.subList(2, conditions.size())) {
            node = generateAndNode(cluster.getRexBuilder(), node, condition);
        }

        return node;
    }

    private RexNode generateAndNode(RexBuilder builder, RexNode left, RexNode right)
    {
        return builder.makeCall(SqlStdOperatorTable.AND, ImmutableList.of(left, right));
    }

    static class PrestoToCalcitePlanContext
    {
        // For each node, The m
        LinkedHashMap<RelNode, ImmutableMap<String, Integer>> relToPrestoColNameCalcitePosMap = new LinkedHashMap<RelNode, ImmutableMap<String, Integer>>();

        public RelNode rewrite(PlanNode node, PrestoToCalcitePlanConvertor nodeRewriter)
        {
            RelNode result = node.accept(nodeRewriter, this);
            verify(result != null, "nodeRewriter returned null for %s", node.getClass().getName());

            return result;
        }

        public void addRelNodeColumnMap(RelNode node, ImmutableMap<String, Integer> columnMap)
        {
            relToPrestoColNameCalcitePosMap.put(node, columnMap);
        }

        public ImmutableMap<String, Integer> getColumnMap(RelNode node)
        {
            return relToPrestoColNameCalcitePosMap.get(node);
        }
    }
}
