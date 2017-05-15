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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.DecimalParseResult;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.GregorianCalendar;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.REAL;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.util.DateTimeUtils.TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER;
import static com.facebook.presto.util.DateTimeUtils.TIMESTAMP_WITH_TIME_ZONE_FORMATTER;
import static com.facebook.presto.util.DateTimeUtils.TIME_FORMATTER;
import static com.facebook.presto.util.DateTimeUtils.TIME_WITH_TIME_ZONE_FORMATTER;
import static com.facebook.presto.util.DateTimeUtils.parseDayTimeInterval;
import static com.facebook.presto.util.DateTimeUtils.parseYearMonthInterval;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Created by shubham on 27/03/17.
 */
// This needs to be in sync with LiteralInterpreter.LiteralVisitor
public class LiteralConverter
        extends AstVisitor<RexNode, ConnectorSession>
{
    private final Metadata metadata;
    private final RexBuilder rexBuilder;
    private final RelDataTypeFactory dtFactory;

    public LiteralConverter(Metadata metadata, RelOptCluster cluster)
    {
        this.metadata = metadata;
        this.rexBuilder = cluster.getRexBuilder();
        this.dtFactory = rexBuilder.getTypeFactory();
    }

    @Override
    protected RexNode visitLiteral(Literal node, ConnectorSession session)
    {
        throw new UnsupportedOperationException("Unhandled literal type: " + node);
    }

    @Override
    protected RexNode visitBooleanLiteral(BooleanLiteral node, ConnectorSession session)
    {
        return rexBuilder.makeLiteral(node.getValue());
    }

    @Override
    protected RexNode visitLongLiteral(LongLiteral node, ConnectorSession session)
    {
        return rexBuilder.makeBigintLiteral(new BigDecimal(node.getValue()));
    }

    @Override
    protected RexNode visitDoubleLiteral(DoubleLiteral node, ConnectorSession session)
    {
        return rexBuilder.makeApproxLiteral(new BigDecimal(node.getValue()));
    }

    @Override
    protected RexNode visitDecimalLiteral(DecimalLiteral node, ConnectorSession context)
    {
        BigDecimal value = new BigDecimal(node.getValue());
        DecimalParseResult parsed = Decimals.parse(node.getValue());
        return rexBuilder.makeExactLiteral(
                value,
                dtFactory.createSqlType(SqlTypeName.DECIMAL, parsed.getType().getPrecision(), parsed.getType().getScale()));
    }

    @Override
    protected RexNode visitStringLiteral(StringLiteral node, ConnectorSession session)
    {
        return rexBuilder.makeLiteral(node.getValue());
    }

    @Override
    protected RexNode visitCharLiteral(CharLiteral node, ConnectorSession context)
    {
        return rexBuilder.makeLiteral(node.getValue());
    }

    @Override
    protected RexNode visitBinaryLiteral(BinaryLiteral node, ConnectorSession session)
    {
        return rexBuilder.makeBinaryLiteral(new ByteString(node.getValue().getBytes()));
    }

    @Override
    protected RexNode visitGenericLiteral(GenericLiteral node, ConnectorSession session)
    {
        Type type = metadata.getType(parseTypeSignature(node.getType()));
        if (type == null) {
            throw new UnsupportedOperationException("Unknown type: " + node.getType());
        }

        if (DATE.equals(type)) {
            GregorianCalendar cal = new GregorianCalendar();
            DateTimeFormatter formatter = ISODateTimeFormat.dateElementParser()
                    .withChronology(ISOChronology.getInstance(UTC));
            DateTime dateTime = formatter.parseDateTime(node.getValue());
            cal.setTime(dateTime.toDate());
            return rexBuilder.makeDateLiteral(cal);
        }

        if (JSON.equals(type)) {
            // treating json as string
            // TODO: how to differentiate STRING JSON while converting back from calcite data type?
            return rexBuilder.makeLiteral(node.getValue());
        }

        if (REAL.equals(type)) {
            BigDecimal bd = new BigDecimal(node.getValue());
            return rexBuilder.makeApproxLiteral(bd, dtFactory.createSqlType(SqlTypeName.REAL));
        }

        // ones below are set by optimizers

        if (BIGINT.equals(type)) {
            return rexBuilder.makeBigintLiteral(new BigDecimal(Long.parseLong(node.getValue())));
        }

        if (DOUBLE.equals(type)) {
            return rexBuilder.makeApproxLiteral(new BigDecimal(Double.parseDouble(node.getValue())));
        }

        throw new UnsupportedOperationException("Unkown type: " + node.getType());
    }

    @Override
    protected RexNode visitTimeLiteral(TimeLiteral node, ConnectorSession session)
    {
        DateTime dateTime;
        try {
            dateTime = TIME_WITH_TIME_ZONE_FORMATTER.parseDateTime(node.getValue());
        }
        catch (Exception e) {
            dateTime = TIME_FORMATTER.withChronology(getChronology(session.getTimeZoneKey())).parseDateTime(node.getValue());
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(dateTime.getMillis());
        calendar.setTimeZone(dateTime.getZone().toTimeZone());

        return rexBuilder.makeTimeLiteral(calendar, 0);
    }

    @Override
    protected RexNode visitTimestampLiteral(TimestampLiteral node, ConnectorSession session)
    {
        DateTime dateTime;
        try {
            dateTime = TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(node.getValue());
        }
        catch (Exception e) {
            dateTime = TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER
                    .withChronology(getChronology(session.getTimeZoneKey()))
                    .parseDateTime(node.getValue());
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(dateTime.getMillis());
        calendar.setTimeZone(dateTime.getZone().toTimeZone());

        return rexBuilder.makeTimestampLiteral(calendar, 0);
    }

    @Override
    protected RexNode visitIntervalLiteral(IntervalLiteral node, ConnectorSession session)
    {
        if (node.isYearToMonth()) {
            long months = node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            return rexBuilder.makeIntervalLiteral(new BigDecimal(months),
                    new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1, 1)));
        }
        else {
            long millis = node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            return rexBuilder.makeIntervalLiteral(new BigDecimal(millis),
                    new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, new SqlParserPos(1, 1)));
        }
    }

    @Override
    protected RexNode visitNullLiteral(NullLiteral node, ConnectorSession session)
    {
        // TODO: ANY type is okay?
        return rexBuilder.makeNullLiteral(SqlTypeName.ANY);
    }
}
