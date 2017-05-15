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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;

/**
 * Created by shubham on 05/04/17.
 */
public class PrestoProject extends Project
    implements PrestoRelNode
{
    public static final RelFactories.ProjectFactory DEFAULT_PROJECT_FACTORY = new PrestoProjectFactoryImpl();

    public PrestoProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType)
    {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet relTraitSet, RelNode source, List<RexNode> projects, RelDataType rowType)
    {
        return new PrestoProject(source.getCluster(), relTraitSet, source, projects, rowType);
    }

    @Override
    public <C, R> R accept(PrestoRelVisitor<C, R> visitor, C context)
    {
        return visitor.visitProject(this, context);
    }

    private static class PrestoProjectFactoryImpl implements RelFactories.ProjectFactory
    {
        @Override
        public RelNode createProject(RelNode child,
                                     List<? extends RexNode> childExprs, List<String> fieldNames)
        {
            RelOptCluster cluster = child.getCluster();
            RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), childExprs, fieldNames);
            RelNode project = new PrestoProject(cluster, cluster.traitSetOf(PrestoRelNode.CONVENTION), child,
                    childExprs, rowType);

            return project;
        }
    }
}
