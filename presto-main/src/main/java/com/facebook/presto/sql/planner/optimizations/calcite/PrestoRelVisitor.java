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

import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoFilter;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoJoinNode;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoProject;
import com.facebook.presto.sql.planner.optimizations.calcite.objects.PrestoTableScan;
import org.apache.calcite.rel.RelNode;

/**
 * Created by shubham on 12/04/17.
 */
public class PrestoRelVisitor<C, R>
{
    public R visitPlan(RelNode node, C context)
    {
        return null;
    }

    public R visitJoin(PrestoJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableScan(PrestoTableScan node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitFilter(PrestoFilter node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitProject(PrestoProject node, C context)
    {
        return visitPlan(node, context);
    }
}
