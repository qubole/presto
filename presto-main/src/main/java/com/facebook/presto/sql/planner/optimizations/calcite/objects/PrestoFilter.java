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
package com.facebook.presto.sql.planner.optimizations.calcite.objects;

import com.facebook.presto.sql.planner.optimizations.calcite.PrestoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;

/**
 * Created by shubham on 24/03/17.
 */
public class PrestoFilter extends Filter
        implements PrestoRelNode
{
    public static final RelFactories.FilterFactory DEFAULT_FILTER_FACTORY = new PrestoFilterFactoryImpl();

    public PrestoFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition)
    {
        super(cluster, traits, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet relTraitSet, RelNode source, RexNode condition)
    {
        return new PrestoFilter(source.getCluster(), relTraitSet, source, condition);
    }

    @Override
    public <C, R> R accept(PrestoRelVisitor<C, R> visitor, C context)
    {
        return visitor.visitFilter(this, context);
    }

    private static class PrestoFilterFactoryImpl implements RelFactories.FilterFactory
    {
        @Override
        public RelNode createFilter(RelNode child, RexNode condition)
        {
            RelOptCluster cluster = child.getCluster();
            PrestoFilter filter = new PrestoFilter(cluster, cluster.traitSetOf(PrestoRelNode.CONVENTION), child, condition);
            return filter;
        }
    }
}
