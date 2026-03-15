package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.util.TestDb;
import ed.inf.adbs.lightdb.operator.util.Helpers;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.expression.Expression;

import org.junit.Test;
import java.util.List;

import static org.junit.Assert.*;

public class SelectOperatorTest {

    // Does equals filter work at all? (Also tests basic expression parsing and evaluation.)
    @Test
    public void equalsFilter_keepsMatchingRows() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C D\n")
                .writeTable("Student",
                        "1,200,50,33\n" +
                        "2,200,200,44\n" +
                        "3,100,105,44\n")) {

            db.initCatalog();

            TableMeta meta = Catalog.getInstance().getTable("Student").get();
            Operator scan = new ScanOperator("Student");

            Expression where = CCJSqlParserUtil.parseCondExpression("Student.B = 200");
            Operator sel = new SelectOperator(scan, where, meta);

            List<String> actual = Helpers.collectAll(sel);

            assertEquals(2, actual.size());
            assertEquals("1, 200, 50, 33", actual.get(0));
            assertEquals("2, 200, 200, 44", actual.get(1));
        }
    }

    // Do other comparison operators work? Are AND/OR/parentheses respected? Does reset work? Do non-integer values in numeric comparisons cause failure?
    @Test
    public void comparisonOperators_work() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C D\n")
                .writeTable("Student",
                        "1,200,50,33\n" +
                        "2,200,200,44\n" +
                        "3,100,105,44\n" +
                        "4,100,50,11\n")) {

            db.initCatalog();

            TableMeta meta = Catalog.getInstance().getTable("Student").get();
            Operator scan = new ScanOperator("Student");

            Expression where = CCJSqlParserUtil.parseCondExpression("Student.D >= 33 AND Student.A != 3");
            Operator sel = new SelectOperator(scan, where, meta);

            List<String> actual = Helpers.collectAll(sel);

            // rows with D >= 33: A=1 (33), A=2 (44), A=3 (44)
            // exclude A=3 => keep A=1 and A=2
            assertEquals(2, actual.size());
            assertEquals("1, 200, 50, 33", actual.get(0));
            assertEquals("2, 200, 200, 44", actual.get(1));
        }
    }

    // Do AND/OR/parentheses work together correctly?
    @Test
    public void andOrParenthesis_respected() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C D\n")
                .writeTable("Student",
                        "1,200,50,33\n" +
                        "2,200,200,44\n" +
                        "3,100,105,44\n" +
                        "4,100,50,11\n")) {

            db.initCatalog();

            TableMeta meta = Catalog.getInstance().getTable("Student").get();
            Operator scan = new ScanOperator("Student");

            // (B=200 AND D=33) OR (A=4)
            Expression where = CCJSqlParserUtil.parseCondExpression("(Student.B = 200 AND Student.D = 33) OR Student.A = 4");
            Operator sel = new SelectOperator(scan, where, meta);

            List<String> actual = Helpers.collectAll(sel);

            assertEquals(2, actual.size());
            assertEquals("1, 200, 50, 33", actual.get(0));
            assertEquals("4, 100, 50, 11", actual.get(1));
        }
    }

    //Does an unknown column in the WHERE cause a runtime failure? (Tests that we don't just treat it as false and keep going, which would be bad if there are many such rows.)
    @Test
    public void unknownColumn_inWhere_shouldThrow() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,2,3\n" +
                        "4,5,6\n")) {

            db.initCatalog();

            TableMeta meta = Catalog.getInstance().getTable("Student").get();
            Operator scan = new ScanOperator("Student");

            Expression where = CCJSqlParserUtil.parseCondExpression("Student.X = 10");
            Operator sel = new SelectOperator(scan, where, meta);

            try {
                Helpers.collectAll(sel);
                fail("Expected a runtime failure due to unknown column in WHERE");
            } catch (RuntimeException ex) {
            }
        }
    }

    //Does an unknown operator in the WHERE cause a runtime failure? 
    @Test
    public void unknownOperator_inWhere_shouldThrow() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,2,3\n" +
                        "4,5,6\n")) {

            db.initCatalog();

            TableMeta meta = Catalog.getInstance().getTable("Student").get();
            Operator scan = new ScanOperator("Student");

            Expression where = CCJSqlParserUtil.parseCondExpression("Student.A @ 10");
            Operator sel = new SelectOperator(scan, where, meta);

            try {
                Helpers.collectAll(sel);
                fail("Expected a runtime failure due to unknown operator in WHERE");
            } catch (RuntimeException ex) {
            }
        }
    }

    // Does reset work correctly to allow re-scanning the input?
    @Test
    public void reset_rewindsSelectionStream() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "2,10\n" +
                        "3,20\n")) {

            db.initCatalog();

            TableMeta meta = Catalog.getInstance().getTable("Student").get();
            Operator scan = new ScanOperator("Student");

            Expression where = CCJSqlParserUtil.parseCondExpression("Student.B = 10");
            SelectOperator sel = new SelectOperator(scan, where, meta);

            Tuple first = sel.getNextTuple();
            assertNotNull(first);
            assertEquals("1, 10", first.toString());

            Tuple second = sel.getNextTuple();
            assertNotNull(second);

            sel.reset();

            Tuple firstAgain = sel.getNextTuple();
            assertNotNull(firstAgain);
            assertEquals("1, 10", firstAgain.toString());
        }
    }

    // Does a non-integer value in a numeric comparison cause a runtime failure? 
    @Test
    public void nonIntValue_inNumericComparison_shouldThrow() throws Exception {
        // This tests "reject non-int" at the right place: numeric evaluation.
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "2,notAnInt\n" +
                        "3,20\n")) {

            db.initCatalog();

            TableMeta meta = Catalog.getInstance().getTable("Student").get();
            Operator scan = new ScanOperator("Student");

            Expression where = CCJSqlParserUtil.parseCondExpression("Student.B > 10");
            Operator sel = new SelectOperator(scan, where, meta);

            try {
                Helpers.collectAll(sel);
                fail("Expected a runtime failure due to non-integer value in numeric comparison");
            } catch (RuntimeException ex) {

            }
        }
    }
}