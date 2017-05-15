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
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

/**
 * Created by shubham on 25/04/17.
 */
public class PrestoJoinNode extends Join
        implements PrestoRelNode
{
    public static final RelFactories.JoinFactory HIVE_JOIN_FACTORY = new PrestoJoinFactoryImpl();

    public PrestoJoinNode(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, JoinRelType joinType)
    {
        super(cluster, traits, left, right, condition, joinType, ImmutableSet.of());
    }

    @Override
    public final PrestoJoinNode copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone)
    {
        return new PrestoJoinNode(left.getCluster(), traitSet, left, right, conditionExpr, joinType);
    }

    @Override
    public <C, R> R accept(PrestoRelVisitor<C, R> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    private static class PrestoJoinFactoryImpl implements RelFactories.JoinFactory
    {
        @Override
        public RelNode createJoin(RelNode left, RelNode right, RexNode condition, JoinRelType joinType,
                                  Set<String> variablesStopped, boolean semiJoinDone)
        {
            return new PrestoJoinNode(left.getCluster(), left.getCluster().traitSetOf(PrestoRelNode.CONVENTION), left, right, condition, joinType);
        }
    }
}
