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
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.util.DateTimeUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.joda.time.DateTime;
import org.reflections.Reflections;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.util.DateTimeUtils.TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER;
import static com.facebook.presto.util.DateTimeUtils.TIMESTAMP_WITH_TIME_ZONE_FORMATTER;
import static com.facebook.presto.util.DateTimeUtils.TIME_FORMATTER;
import static com.facebook.presto.util.DateTimeUtils.TIME_WITH_TIME_ZONE_FORMATTER;
import static com.facebook.presto.util.DateTimeUtils.parseDayTimeInterval;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static org.testng.Assert.assertTrue;

/**
 * Created by shubham on 27/04/17.
 */
public class TestLiteralConversion
{
    Metadata metadata;
    RelOptCluster cluster;
    Session session;

    public TestLiteralConversion()
    {
        session = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        metadata = new LocalQueryRunner(session).getMetadata();

        Frameworks.withPlanner(
                new Frameworks.PlannerAction<Void>()
                {
                    @Override
                    public Void apply(RelOptCluster relOptCluster, RelOptSchema relOptSchema, SchemaPlus schemaPlus)
                    {
                        cluster = relOptCluster;
                        return null;
                    }
                },
                Frameworks.newConfigBuilder().typeSystem(new PrestoTypeSystemImpl()).build()
        );
    }

    @Test
    public void testLiteralConversion()
    {
        List<Literal> literals = new ArrayList();
        literals.add(BooleanLiteral.TRUE_LITERAL);

        literals.add(BooleanLiteral.FALSE_LITERAL);

        literals.add(new BinaryLiteral("af"));

        literals.add(new CharLiteral("abc"));

        literals.add(new DecimalLiteral("10.123"));

        literals.add(new DoubleLiteral("10.123"));

        // INTERVAL -'100-5' YEAR TO MONTH
        literals.add(new IntervalLiteral("100-5", IntervalLiteral.Sign.NEGATIVE, IntervalLiteral.IntervalField.YEAR, Optional.of(IntervalLiteral.IntervalField.MONTH)));

        // INTERVAL '03:04:11.333' HOUR TO SECOND
        literals.add(new IntervalLiteral("03:04:11.333", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.HOUR, Optional.of(IntervalLiteral.IntervalField.SECOND)));

        literals.add(new LongLiteral("100"));

        literals.add(new NullLiteral());

        literals.add(new StringLiteral("stringLiteral"));

        literals.add(new TimestampLiteral("2001-08-22 03:04:05.321"));
        literals.add(new TimestampLiteral("2001-08-22 03:04:05.321 Asia/Calcutta"));

        literals.add(new TimeLiteral("01:02:03.456"));
        literals.add(new TimeLiteral("01:02:03.456 Asia/Calcutta"));

        literals.add(new GenericLiteral("date", "2001-08-22"));
        literals.add(new GenericLiteral("json", "{\"a\": 1}"));

        LiteralConverter toRexConverter = new LiteralConverter(metadata, cluster);
        RexNodeToExpressionConverter fromRexConverter = new RexNodeToExpressionConverter(ImmutableList.of());
        for (Literal literal : literals) {
            verifyConversion(literal, toRexConverter, fromRexConverter);
        }

        verifyAllLiteralsTested(literals);
    }

    private void verifyAllLiteralsTested(List<Literal> testedLiterals)
    {
        List<Class> testedClasses = new ArrayList();
        for (Literal literal : testedLiterals) {
            testedClasses.add(literal.getClass());
        }

        Reflections reflections = new Reflections("com.facebook.presto.sql.tree");
        List<Class> untestedLiterals = new ArrayList();
        for (Class clazz : reflections.getSubTypesOf(Literal.class)) {
            if (!testedClasses.contains(clazz)) {
                untestedLiterals.add(clazz);
            }
        }

        assertTrue(untestedLiterals.isEmpty(), "Untested Literals found " + untestedLiterals);
    }

    private void verifyConversion(Literal originalLiteral, LiteralConverter toRexConverter, RexNodeToExpressionConverter fromRexConverter)
    {
        RexNode rexNode = originalLiteral.accept(toRexConverter, session.toConnectorSession());
        Literal convertedLiteral = (Literal) fromRexConverter.convert(rexNode);

        // special conditions in if case where the value or type changes
        if (originalLiteral instanceof IntervalLiteral) {
            // Values are changed for interval types to normalize to BigDecimal in RexNode
            IntervalLiteral originalIntervalLiteral = (IntervalLiteral) originalLiteral;
            IntervalLiteral convertedIntervalLiteral = (IntervalLiteral) convertedLiteral;
            if (originalIntervalLiteral.isYearToMonth()) {
                long originalMonths = originalIntervalLiteral.getSign().multiplier() * DateTimeUtils.parseYearMonthInterval(
                        originalIntervalLiteral.getValue(),
                        originalIntervalLiteral.getStartField(),
                        originalIntervalLiteral.getEndField());
                long convertedMonths = convertedIntervalLiteral.getSign().multiplier() * DateTimeUtils.parseYearMonthInterval(
                        convertedIntervalLiteral.getValue(),
                        convertedIntervalLiteral.getStartField(),
                        convertedIntervalLiteral.getEndField());
                assertTrue(originalMonths == convertedMonths,
                        String.format("Mismatch in months in Year to Month Interval conversion: [%d, %d]", originalMonths, convertedMonths));
            }
            else {
                long originalMillis = originalIntervalLiteral.getSign().multiplier() * parseDayTimeInterval(
                        originalIntervalLiteral.getValue(),
                        originalIntervalLiteral.getStartField(),
                        originalIntervalLiteral.getEndField());

                long convertedMillis = convertedIntervalLiteral.getSign().multiplier() * parseDayTimeInterval(
                        convertedIntervalLiteral.getValue(),
                        convertedIntervalLiteral.getStartField(),
                        convertedIntervalLiteral.getEndField());
                assertTrue(originalMillis == convertedMillis, String.format("Mismatch in millis in Day to Second Interval: [%d, %d]", originalMillis, convertedMillis));
            }
        }
        else if (originalLiteral instanceof StringLiteral) {
            // Converted to char type
            assertTrue(convertedLiteral instanceof CharLiteral, "Expected CharLiteral, found " + convertedLiteral.getClass());
            assertTrue(((StringLiteral) originalLiteral).getValue().equals(((CharLiteral) convertedLiteral).getValue()),
                    String.format("Mismatch in value for StringLiteral [%s, %s]", ((StringLiteral) originalLiteral).getValue(), ((CharLiteral) convertedLiteral).getValue()));
        }
        else if (originalLiteral instanceof TimestampLiteral) {
            TimestampLiteral original = (TimestampLiteral) originalLiteral;
            TimestampLiteral converted = (TimestampLiteral) convertedLiteral;
            DateTime originalDateTime;
            DateTime convertedDateTime;

            try {
                originalDateTime = TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(original.getValue());
            }
            catch (Exception e) {
                originalDateTime = TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER
                        .withChronology(getChronology(session.getTimeZoneKey()))
                        .parseDateTime(original.getValue());
            }

            // converted value is always with time zone
            convertedDateTime = TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(converted.getValue());
            matchDateTime(originalDateTime, convertedDateTime);
        }
        else if (originalLiteral instanceof TimeLiteral) {
            TimeLiteral original = (TimeLiteral) originalLiteral;
            TimeLiteral converted = (TimeLiteral) convertedLiteral;
            DateTime originalDateTime;
            DateTime convertedDateTime;
            try {
                originalDateTime = TIME_WITH_TIME_ZONE_FORMATTER.parseDateTime(original.getValue());
            }
            catch (Exception e) {
                originalDateTime = TIME_FORMATTER
                        .withChronology(getChronology(session.getTimeZoneKey()))
                        .parseDateTime(original.getValue());
            }
            convertedDateTime = TIME_WITH_TIME_ZONE_FORMATTER.parseDateTime(converted.getValue());
            matchDateTime(originalDateTime, convertedDateTime);
        }
        else if (originalLiteral instanceof GenericLiteral) {
            if (((GenericLiteral) originalLiteral).getType().equals("date")) {
                assertTrue(originalLiteral.equals(convertedLiteral),
                        String.format("Mismatch in date values [%s, %s]",
                                ((GenericLiteral) originalLiteral).getValue(),
                                ((GenericLiteral) convertedLiteral).getValue()));
            }
            else if (((GenericLiteral) originalLiteral).getType().equals("json")) {
                // Converted to char type
                assertTrue(convertedLiteral instanceof CharLiteral, "Expected CharLiteral, found " + convertedLiteral.getClass());
                assertTrue(((GenericLiteral) originalLiteral).getValue().equals(((CharLiteral) convertedLiteral).getValue()),
                        String.format("Mismatch in value for StringLiteral [%s, %s]", ((GenericLiteral) originalLiteral).getValue(), ((CharLiteral) convertedLiteral).getValue()));
            }
        }
        else {
            assertTrue(originalLiteral.equals(convertedLiteral), String.format("Literals do not match [%s, %s]", originalLiteral, convertedLiteral));
        }
    }

    private void matchDateTime(DateTime first, DateTime second)
    {
        assertTrue(first.getMillis() == second.getMillis(),
                String.format("Mismatch is millis [%d, %d]", first.getMillis(), second.getMillis()));
        // match timezones
        assertTrue(first.getZone().toTimeZone().getRawOffset() == second.getZone().toTimeZone().getRawOffset(),
                String.format("Timezone did not match: [%d, %d]", first.getZone().toTimeZone().getRawOffset(), second.getZone().toTimeZone().getRawOffset()));
    }
}
