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
package com.facebook.presto.sql.planner.acid;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;

public class ACIDTpchMetadata

        extends TpchMetadata
{
    public ACIDTpchMetadata(String connectorId, ColumnNaming columnNaming, boolean predicatePushdownEnabled)
    {
        super(connectorId, columnNaming, predicatePushdownEnabled);
    }

    @Override
    public TpchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        TpchTableHandle originalTableHandle = super.getTableHandle(session, tableName);
        return new ACIDTpchTableHandle(
                originalTableHandle.getConnectorId(),
                originalTableHandle.getTableName(),
                originalTableHandle.getScaleFactor());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableMetadata actual = super.getTableMetadata(session, tableHandle);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.addAll(actual.getColumns());
        columns.add(new ColumnMetadata("$isValid", BooleanType.BOOLEAN));
        return new ConnectorTableMetadata(actual.getTable(), columns.build());
    }
}
