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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import io.airlift.log.Logger;
import org.apache.calcite.tools.Frameworks;

import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isEnableCalcite;

/**
 * Created by shubham on 07/03/17.
 */
public class CalciteOptimizer
    implements PlanOptimizer
{
    private static final Logger log = Logger.get(CalciteOptimizer.class);
    Metadata metadata;

    public CalciteOptimizer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!isEnableCalcite(session)) {
            return plan;
        }

        CalciteValidatorContext calciteValidatorContext = new CalciteValidatorContext();
        SimplePlanRewriter.rewriteWith(new PlanValidator(), plan, calciteValidatorContext);
        if (!calciteValidatorContext.isPlanSupported()) {
            log.warn("Cannot apply Calcite optimizer, unsupported node type found: " + calciteValidatorContext.getFirstUnsupportedNodeType());
            return plan;
        }

        PlanNode optimizedPlan;
        try {
            optimizedPlan = Frameworks.withPlanner(new CalcitePlannerAction(plan, metadata, idAllocator, symbolAllocator, session), Frameworks
                    .newConfigBuilder().typeSystem(new PrestoTypeSystemImpl()).build());
        }
        catch (Exception e) {
            log.warn(e, "Could not optimize plan using Calcite ");
            return plan;
        }

        // TODO remove this warning
        log.warn("Successfully converted plan using Calcite");
        return optimizedPlan;
    }

    private static class PlanValidator
            extends SimplePlanRewriter<CalciteValidatorContext>
    {
        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<CalciteValidatorContext> context)
        {
            // reached here => unsupported node type
            context.get().setPlanNotSupported(node.toString());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<CalciteValidatorContext> context)
        {
            context.rewrite(node.getSource(), context.get());
            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<CalciteValidatorContext> context)
        {
            return node;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<CalciteValidatorContext> context)
        {
            context.rewrite(node.getSource(), context.get());
            return node;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<CalciteValidatorContext> context)
        {
            context.rewrite(node.getSource(), context.get());
            return node;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<CalciteValidatorContext> context)
        {
            context.rewrite(node.getLeft(), context.get());
            context.rewrite(node.getRight(), context.get());
            return node;
        }
    }

    class CalciteValidatorContext
    {
        boolean planSupported = true;
        String firstUnsupportedNodeType;

        public void setPlanNotSupported(String forNodeType)
        {
            planSupported = false;
            this.firstUnsupportedNodeType = forNodeType;
        }

        public boolean isPlanSupported()
        {
            return planSupported;
        }

        public String getFirstUnsupportedNodeType()
        {
            return firstUnsupportedNodeType;
        }
    }
}
