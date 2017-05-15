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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.IntervalDayTimeType;
import com.facebook.presto.type.IntervalYearMonthType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.CHAR;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static com.facebook.presto.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.P4_HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.StandardTypes.REAL;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.StandardTypes.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.TIME;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.StandardTypes.TINYINT;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;

/**
 * Created by shubham on 23/03/17.
 */
public class TypeConverter
{
    private TypeConverter()
    {
    }

    public static RelDataType getType(RelOptCluster cluster, List<Symbol> columns, List<Type> types)
    {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();

        ImmutableList.Builder colNames = ImmutableList.builder();
        ImmutableList.Builder colTypes = ImmutableList.builder();
        for (int pos = 0; pos < columns.size(); pos++) {
            colNames.add(columns.get(pos).getName());
            colTypes.add(convert(types.get(pos), dtFactory));
        }
        return dtFactory.createStructType(colTypes.build(), colNames.build());
    }

    public static RelDataType convert(Type type, RelOptCluster cluster)
    {
        return convert(type, cluster.getRexBuilder().getTypeFactory());
    }

    public static RelDataType convert(Type type, RelDataTypeFactory dtFactory)
    {
        RelDataType convertedType = null;

        // This is best suited to be part of each Type so that we could just do Type.getCalciteType() but that is too invasive
        switch (type.getTypeSignature().getBase()) {
            case BIGINT:
                convertedType = dtFactory.createSqlType(SqlTypeName.BIGINT);
                break;
            case INTEGER:
                convertedType = dtFactory.createSqlType(SqlTypeName.INTEGER);
                break;
            case SMALLINT:
                convertedType = dtFactory.createSqlType(SqlTypeName.SMALLINT);
                break;
            case TINYINT:
                convertedType = dtFactory.createSqlType(SqlTypeName.TINYINT);
                break;
            case BOOLEAN:
                convertedType = dtFactory.createSqlType(SqlTypeName.BOOLEAN);
                break;
            case DATE:
                convertedType = dtFactory.createSqlType(SqlTypeName.DATE);
                break;
            case DECIMAL:
                convertedType = dtFactory
                        .createSqlType(SqlTypeName.DECIMAL, ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
                break;
            case REAL:
                convertedType = dtFactory.createSqlType(SqlTypeName.REAL);
                break;
            case DOUBLE:
                convertedType = dtFactory.createSqlType(SqlTypeName.DOUBLE);
                break;
            case HYPER_LOG_LOG:
                convertedType = dtFactory.createSqlType(SqlTypeName.VARBINARY);
                break;
            case P4_HYPER_LOG_LOG:
                convertedType = dtFactory.createSqlType(SqlTypeName.VARBINARY);
                break;
            case INTERVAL_DAY_TO_SECOND:
                // TODO check if this breaks
                convertedType = dtFactory.createSqlType(SqlTypeName.INTERVAL_DAY_TIME);
                break;
            case INTERVAL_YEAR_TO_MONTH:
                convertedType = dtFactory.createSqlIntervalType(
                        new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1, 1)));
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
                convertedType = dtFactory.createSqlType(SqlTypeName.TIMESTAMP);
                break;
            case TIME:
            case TIME_WITH_TIME_ZONE:
                convertedType = dtFactory.createSqlType(SqlTypeName.TIME);
                break;
            case VARBINARY:
                convertedType = dtFactory.createSqlType(SqlTypeName.VARBINARY);
                break;
            case VARCHAR:
                convertedType = dtFactory.createSqlType(SqlTypeName.VARCHAR, ((VarcharType) type).getLength());
                break;
            case CHAR:
                convertedType = dtFactory.createSqlType(SqlTypeName.VARCHAR, ((CharType) type).getLength());
                break;
            case ROW:
                ImmutableList.Builder types = ImmutableList.builder();
                ImmutableList.Builder names = ImmutableList.builder();
                int unnamedFieldCount = 0;
                for (RowType.RowField rowField : ((RowType) type).getFields()) {
                    types.add(convert(rowField.getType(), dtFactory));
                    names.add(rowField.getName().orElse("unnamed_" + (unnamedFieldCount + 1)));
                }
                convertedType = dtFactory.createStructType(types.build(), names.build());
                break;
            case ARRAY:
                RelDataType elementType = convert(((ArrayType) type).getElementType(), dtFactory);
                convertedType = dtFactory.createArrayType(elementType, -1);
                break;
            case MAP:
                convertedType = dtFactory.createMapType(
                        convert(((MapType) type).getKeyType(), dtFactory),
                        convert(((MapType) type).getValueType(), dtFactory));
                break;
            case JSON:
                // TODO check if VARCHAR.length is enough for json
                convertedType = dtFactory.createSqlType(SqlTypeName.VARCHAR, ((VarcharType) type).getLength());
                break;
        }
        return convertedType;
    }

    // Calcite Type to Presto Type
    public static Type convert(RelDataType type)
    {
        if (type.isStruct()) {
            List types = type.getFieldList().stream().map(field -> convert(field.getType())).collect(Collectors.toList());
            List names = type.getFieldList().stream().map(field -> field.getName()).collect(Collectors.toList());
            return new RowType(types, Optional.of(names));
        }
        else if (type.getComponentType() != null) {
            return new ArrayType(convert(type.getComponentType()));
        }
        else if (type.getKeyType() != null) {
            return new MapType(
                    convert(type.getKeyType()),
                    convert(type.getValueType())
            );
        }

        // All Primitive types below
        switch (type.getSqlTypeName()) {
            case BIGINT:
                return BigintType.BIGINT;
            case INTEGER:
                return IntegerType.INTEGER;
            case SMALLINT:
                return SmallintType.SMALLINT;
            case TINYINT:
                return TinyintType.TINYINT;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case DATE:
                return DateType.DATE;
            case DECIMAL:
                return DecimalType.createDecimalType(type.getPrecision(), type.getScale());
            case REAL:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case INTERVAL_DAY_TIME:
                // TODO check if this breaks
                return IntervalDayTimeType.INTERVAL_DAY_TIME;
            case INTERVAL_YEAR_MONTH:
                return IntervalYearMonthType.INTERVAL_YEAR_MONTH;
            case TIMESTAMP:
                // TODO: verify this is fine. We can convert all TIMESTAMP to TIMESTAMP_WITH_TIME_ZONE internally
                return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
            case TIME:
                // TODO verify this like TIMESTAMP above
                return TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
            case VARBINARY:
                // TODO: cannot differentiate between VARBINARY, HYPERLOG, HYPERLOGLOG
                return VarbinaryType.VARBINARY;
            case VARCHAR:
                // TODO: how to differentiate JSON
                if (type.getPrecision() == Integer.MAX_VALUE) {
                    return VarcharType.createUnboundedVarcharType();
                }
                else {
                    return VarcharType.createVarcharType(type.getPrecision());
                }
            case CHAR:
                return CharType.createCharType(type.getPrecision());
            default:
                throw new UnsupportedOperationException("Unknown type: " + type.getSqlTypeName());
        }
    }
}
