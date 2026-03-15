package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.util.Helpers;
import ed.inf.adbs.lightdb.operator.util.TestDb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class FilterOperatorTest {

    // Does FilterOperator produce correct output for a simple comparison expression?
    @Test
    public void simpleComparison_filtersCorrectRows() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,200,50\n" +
                        "2,200,200\n" +
                        "3,100,105\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            List<TableMeta> tables =
                    Arrays.asList(Catalog.getInstance().getTable("Student").get());

            Expression expr =
                    CCJSqlParserUtil.parseCondExpression("Student.C > 100");

            FilterOperator filter = new FilterOperator(scan, expr, tables);

            List<String> actual = Helpers.collectAll(filter);

            List<String> expected = Arrays.asList(
                    "2, 200, 200",
                    "3, 100, 105"
            );

            assertEquals(expected, actual);
        }
    }

    // Does FilterOperator produce correct output for a simple comparison expression with an unqualified column ref?
    @Test
    public void logicalAnd_supported() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,200,50\n" +
                        "2,200,200\n" +
                        "3,100,105\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            List<TableMeta> tables =
                    Arrays.asList(Catalog.getInstance().getTable("Student").get());

            Expression expr =
                    CCJSqlParserUtil.parseCondExpression("Student.B = 200 AND Student.C > 100");

            FilterOperator filter = new FilterOperator(scan, expr, tables);

            List<String> actual = Helpers.collectAll(filter);

            List<String> expected = Arrays.asList(
                    "2, 200, 200"
            );

            assertEquals(expected, actual);
        }
    }

    // Does FilterOperator produce correct output for a simple comparison expression with an unqualified column ref?
    @Test
    public void logicalOr_supported() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,200,50\n" +
                        "2,200,200\n" +
                        "3,100,105\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            List<TableMeta> tables =
                    Arrays.asList(Catalog.getInstance().getTable("Student").get());

            Expression expr =
                    CCJSqlParserUtil.parseCondExpression("Student.A = 1 OR Student.C > 100");

            FilterOperator filter = new FilterOperator(scan, expr, tables);

            List<String> actual = Helpers.collectAll(filter);

            List<String> expected = Arrays.asList(
                    "1, 200, 50",
                    "2, 200, 200",
                    "3, 100, 105"
            );

            assertEquals(expected, actual);
        }
    }

    // Does FilterOperator produce correct output for a simple comparison expression with parentheses?
    @Test
    public void parentheses_respected() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,200,50\n" +
                        "2,200,200\n" +
                        "3,100,105\n" +
                        "4,100,10\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            List<TableMeta> tables =
                    Arrays.asList(Catalog.getInstance().getTable("Student").get());

            Expression expr =
                    CCJSqlParserUtil.parseCondExpression("(Student.B = 200 AND Student.C > 100) OR Student.A = 4");

            FilterOperator filter = new FilterOperator(scan, expr, tables);

            List<String> actual = Helpers.collectAll(filter);

            List<String> expected = Arrays.asList(
                    "2, 200, 200",
                    "4, 100, 10"
            );

            assertEquals(expected, actual);
        }
    }

    // Does FilterOperator produce correct output when the expression is null (i.e. it should pass through all tuples)?
    @Test
    public void nullExpression_passthrough() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "2,20\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            List<TableMeta> tables =
                    Arrays.asList(Catalog.getInstance().getTable("Student").get());

            FilterOperator filter = new FilterOperator(scan, null, tables);

            List<String> actual = Helpers.collectAll(filter);

            List<String> expected = Arrays.asList(
                    "1, 10",
                    "2, 20"
            );

            assertEquals(expected, actual);
        }
    }

    // Does reset work correctly and allow the FilterOperator to be re-iterated from the start, producing the same output?
    @Test
    public void reset_rewindsStream() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "2,20\n" +
                        "3,30\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            List<TableMeta> tables =
                    Arrays.asList(Catalog.getInstance().getTable("Student").get());

            Expression expr =
                    CCJSqlParserUtil.parseCondExpression("Student.A >= 2");

            FilterOperator filter = new FilterOperator(scan, expr, tables);

            Tuple first = filter.getNextTuple();
            assertEquals("2, 20", first.toString());

            filter.getNextTuple();

            filter.reset();

            Tuple firstAgain = filter.getNextTuple();
            assertEquals("2, 20", firstAgain.toString());
        }
    }

    // Does a non-integer value in the expression cause a runtime failure?
    @Test
    public void nonIntValue_throwsDuringComparison() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "2,notAnInt\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            List<TableMeta> tables =
                    Arrays.asList(Catalog.getInstance().getTable("Student").get());

            Expression expr =
                    CCJSqlParserUtil.parseCondExpression("Student.B > 5");

            FilterOperator filter = new FilterOperator(scan, expr, tables);

            try {
                Helpers.collectAll(filter);
                fail("Expected runtime exception due to non-integer comparison");
            } catch (RuntimeException ex) {
                // acceptable
            }
        }
    }
}