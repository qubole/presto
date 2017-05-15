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
import com.facebook.presto.sql.planner.plan.TableScanNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Created by shubham on 24/03/17.
 */
public class PrestoTableScan extends TableScan
    implements PrestoRelNode
{
    private final TableScanNode planNode;

    public PrestoTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, TableScanNode originalNode)
    {
        super(cluster, traitSet, table);
        this.planNode = originalNode;
    }

    public TableScanNode getPlanNode()
    {
        return planNode;
    }

    @Override
    public <C, R> R accept(PrestoRelVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs)
    {
        checkState(inputs.isEmpty(), "TableScan node should have children nodes");
        return this;
    }
}
