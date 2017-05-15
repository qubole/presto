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
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Created by shubham on 24/03/17.
 */
public interface PrestoRelNode extends RelNode
{
    final Convention CONVENTION = new Convention.Impl("PRESTO", PrestoRelNode.class);

    public default <C, R> R accept(PrestoRelVisitor<C, R> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
