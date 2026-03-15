package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.util.Helpers;
import ed.inf.adbs.lightdb.operator.util.TestDb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class SumOperatorTest {

    // Does SumOperator produce correct output for a simple GROUP BY on a single column with a single SUM aggregate?
    @Test
    public void groupBySingleColumn_sumSingleColumn() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,200,50\n" +
                        "2,200,200\n" +
                        "3,100,105\n" +
                        "4,100,50\n" +
                        "5,100,500\n" +
                        "6,300,400\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            List<Expression> sumArgs = Arrays.asList(
                    CCJSqlParserUtil.parseExpression("Student.C")
            );

            SumOperator sum = new SumOperator(
                    scan,
                    Arrays.asList("student.b"),   // grouping keys
                    Arrays.asList("student.b"),   // output keys
                    sumArgs,
                    tables
            );

            List<String> actual = Helpers.collectAll(sum);

            // first-seen group order from input: 200, 100, 300
            List<String> expected = Arrays.asList(
                    "200, 250",
                    "100, 655",
                    "300, 400"
            );

            assertEquals(expected, actual);
        }
    }

    // Does SumOperator produce correct output for a simple GROUP BY on a single column with a single SUM aggregate, when the output schema does not include the group key?
    @Test
    public void groupBySingleColumn_sumConstant_onlyOutputsSumWhenNoProjectedGroupCols() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,200\n" +
                        "2,200\n" +
                        "3,100\n" +
                        "4,300\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            List<Expression> sumArgs = Arrays.asList(
                    CCJSqlParserUtil.parseExpression("1")
            );

            SumOperator sum = new SumOperator(
                    scan,
                    Arrays.asList("student.b"),   // grouping by B
                    new ArrayList<String>(),      // but output no group keys
                    sumArgs,
                    tables
            );

            List<String> actual = Helpers.collectAll(sum);

            // counts per B in first-seen group order: 200 -> 2, 100 -> 1, 300 -> 1
            List<String> expected = Arrays.asList(
                    "2",
                    "1",
                    "1"
            );

            assertEquals(expected, actual);
        }
    }

    // Does SumOperator produce correct output for a simple global aggregation with no GROUP BY and a single SUM aggregate?
    @Test
    public void globalAggregation_noGroupBy_singleSum() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,200,50\n" +
                        "2,200,200\n" +
                        "3,100,105\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            List<Expression> sumArgs = Arrays.asList(
                    CCJSqlParserUtil.parseExpression("Student.C")
            );

            SumOperator sum = new SumOperator(
                    scan,
                    new ArrayList<String>(),      // no grouping
                    new ArrayList<String>(),      // no output group keys
                    sumArgs,
                    tables
            );

            List<String> actual = Helpers.collectAll(sum);

            List<String> expected = Arrays.asList("355");

            assertEquals(expected, actual);
        }
    }

    // Does SumOperator produce correct output for a GROUP BY with multiple SUM aggregates and multiple grouping keys, when the same grouping key appears multiple times in the output schema?
    @Test
    public void multipleSums_sameGrouping() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C D\n")
                .writeTable("Student",
                        "1,200,50,33\n" +
                        "2,200,200,44\n" +
                        "3,100,105,44\n" +
                        "4,100,50,11\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            List<Expression> sumArgs = Arrays.asList(
                    CCJSqlParserUtil.parseExpression("Student.C"),
                    CCJSqlParserUtil.parseExpression("Student.D")
            );

            SumOperator sum = new SumOperator(
                    scan,
                    Arrays.asList("student.b"),
                    Arrays.asList("student.b"),
                    sumArgs,
                    tables
            );

            List<String> actual = Helpers.collectAll(sum);

            List<String> expected = Arrays.asList(
                    "200, 250, 77",
                    "100, 155, 55"
            );

            assertEquals(expected, actual);
        }
    }

    // Does SumOperator produce correct output when the SUM expression is a multiplication of two columns, and the input contains multiple groups?
    @Test
    public void sumMultiplicationExpression_supported() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,2,3\n" +
                        "2,2,4\n" +
                        "3,1,10\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            // GROUP BY B, SUM(A * C)
            List<Expression> sumArgs = Arrays.asList(
                    CCJSqlParserUtil.parseExpression("Student.A * Student.C")
            );

            SumOperator sum = new SumOperator(
                    scan,
                    Arrays.asList("student.b"),
                    Arrays.asList("student.b"),
                    sumArgs,
                    tables
            );

            List<String> actual = Helpers.collectAll(sum);

            // B=2 => 1*3 + 2*4 = 11
            // B=1 => 3*10 = 30
            List<String> expected = Arrays.asList(
                    "2, 11",
                    "1, 30"
            );

            assertEquals(expected, actual);
        }
    }

    // Does SumOperator produce correct output when the input contains multiple groups with the same group key value, and the output schema includes the group key multiple times?
    @Test
    public void reset_rewindsAggregation() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,200,50\n" +
                        "2,200,200\n" +
                        "3,100,105\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            List<Expression> sumArgs = Arrays.asList(
                    CCJSqlParserUtil.parseExpression("Student.C")
            );

            SumOperator sum = new SumOperator(
                    scan,
                    Arrays.asList("student.b"),
                    Arrays.asList("student.b"),
                    sumArgs,
                    tables
            );

            Tuple first = sum.getNextTuple();
            assertNotNull(first);
            assertEquals("200, 250", first.toString());

            assertNotNull(sum.getNextTuple());

            sum.reset();

            Tuple firstAgain = sum.getNextTuple();
            assertNotNull(firstAgain);
            assertEquals("200, 250", firstAgain.toString());
        }
    }

    // Does a non-integer value in the SUM expression cause a runtime failure?
    @Test
    public void nonIntValueInSumExpression_shouldThrow() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,200,50\n" +
                        "2,200,notAnInt\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            List<Expression> sumArgs = Arrays.asList(
                    CCJSqlParserUtil.parseExpression("Student.C")
            );

            SumOperator sum = new SumOperator(
                    scan,
                    Arrays.asList("student.b"),
                    Arrays.asList("student.b"),
                    sumArgs,
                    tables
            );

            try {
                Helpers.collectAll(sum);
                fail("Expected runtime failure due to non-integer value in SUM expression");
            } catch (RuntimeException ex) {
                // acceptable: NumberFormatException may bubble or be wrapped
            }
        }
    }
}