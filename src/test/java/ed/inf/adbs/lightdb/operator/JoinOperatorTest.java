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

public class JoinOperatorTest {


    // Does JoinOperator produce correct output for a simple equi-join? (Also tests basic expression parsing and evaluation.)
    @Test
    public void equiJoin_returnsExpectedRows() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema(
                        "Student A B\n" +
                        "Enrolled A H\n"
                )
                .writeTable("Student",
                        "1,10\n" +
                        "2,20\n" +
                        "1,30\n")
                .writeTable("Enrolled",
                        "1,100\n" +
                        "2,200\n" +
                        "3,300\n")) {

            db.initCatalog();

            Operator left = new ScanOperator("Student");
            Operator right = new ScanOperator("Enrolled");

            List<TableMeta> leftTables = Arrays.asList(Catalog.getInstance().getTable("Student").get());
            List<TableMeta> rightTables = Arrays.asList(Catalog.getInstance().getTable("Enrolled").get());

            Expression on = CCJSqlParserUtil.parseCondExpression("Student.A = Enrolled.A");

            JoinOperator join = new JoinOperator(left, right, on, leftTables, rightTables);

            List<String> actual = Helpers.collectAll(join);

            // Conventional NLJ with left outer, right inner:
            // Student(1,10) joins Enrolled(1,100)
            // Student(2,20) joins Enrolled(2,200)
            // Student(1,30) joins Enrolled(1,100)
            List<String> expected = Arrays.asList(
                    "1, 10, 1, 100",
                    "2, 20, 2, 200",
                    "1, 30, 1, 100"
            );

            assertEquals(expected, actual);
        }
    }

    // Does JoinOperator return empty output when there are no matches?
    @Test
    public void joinWithNoMatches_returnsEmpty() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema(
                        "Student A B\n" +
                        "Enrolled A H\n"
                )
                .writeTable("Student",
                        "1,10\n" +
                        "2,20\n")
                .writeTable("Enrolled",
                        "3,300\n" +
                        "4,400\n")) {

            db.initCatalog();

            Operator left = new ScanOperator("Student");
            Operator right = new ScanOperator("Enrolled");

            List<TableMeta> leftTables = Arrays.asList(Catalog.getInstance().getTable("Student").get());
            List<TableMeta> rightTables = Arrays.asList(Catalog.getInstance().getTable("Enrolled").get());

            Expression on = CCJSqlParserUtil.parseCondExpression("Student.A = Enrolled.A");

            JoinOperator join = new JoinOperator(left, right, on, leftTables, rightTables);

            List<String> actual = Helpers.collectAll(join);

            assertTrue(actual.isEmpty());
        }
    }


    // Do duplicate keys produce the correct number of matching pairs?
    @Test
    public void duplicateKeys_produceAllMatchingPairs() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema(
                        "LeftT A X\n" +
                        "RightT A Y\n"
                )
                .writeTable("LeftT",
                        "1,10\n" +
                        "1,11\n")
                .writeTable("RightT",
                        "1,100\n" +
                        "1,101\n")) {

            db.initCatalog();

            Operator left = new ScanOperator("LeftT");
            Operator right = new ScanOperator("RightT");

            List<TableMeta> leftTables = Arrays.asList(Catalog.getInstance().getTable("LeftT").get());
            List<TableMeta> rightTables = Arrays.asList(Catalog.getInstance().getTable("RightT").get());

            Expression on = CCJSqlParserUtil.parseCondExpression("LeftT.A = RightT.A");

            JoinOperator join = new JoinOperator(left, right, on, leftTables, rightTables);

            List<String> actual = Helpers.collectAll(join);

            // 2 x 2 = 4 matches
            List<String> expected = Arrays.asList(
                    "1, 10, 1, 100",
                    "1, 10, 1, 101",
                    "1, 11, 1, 100",
                    "1, 11, 1, 101"
            );

            assertEquals(expected, actual);
        }
    }

    // Does an equi-join with unqualified column refs work correctly when there is only one table with that column? (Tests that unqualified refs resolve correctly in the presence of multiple tables.)
    @Test
    public void outputTupleOrder_isLeftThenRight() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema(
                        "Student A B C\n" +
                        "Enrolled A H\n"
                )
                .writeTable("Student", "1,10,99\n")
                .writeTable("Enrolled", "1,100\n")) {

            db.initCatalog();

            Operator left = new ScanOperator("Student");
            Operator right = new ScanOperator("Enrolled");

            List<TableMeta> leftTables = Arrays.asList(Catalog.getInstance().getTable("Student").get());
            List<TableMeta> rightTables = Arrays.asList(Catalog.getInstance().getTable("Enrolled").get());

            Expression on = CCJSqlParserUtil.parseCondExpression("Student.A = Enrolled.A");

            JoinOperator join = new JoinOperator(left, right, on, leftTables, rightTables);

            Tuple t = join.getNextTuple();
            assertNotNull(t);

            // left tuple first: Student A,B,C then Enrolled A,H
            assertEquals("1, 10, 99, 1, 100", t.toString());
            assertNull(join.getNextTuple());
        }
    }

    // Does a reset on the JoinOperator correctly rewinds both inputs and allows re-scanning the join output?
    @Test
    public void reset_rewindsJoinStream() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema(
                        "Student A B\n" +
                        "Enrolled A H\n"
                )
                .writeTable("Student",
                        "1,10\n" +
                        "2,20\n")
                .writeTable("Enrolled",
                        "1,100\n" +
                        "2,200\n")) {

            db.initCatalog();

            Operator left = new ScanOperator("Student");
            Operator right = new ScanOperator("Enrolled");

            List<TableMeta> leftTables = Arrays.asList(Catalog.getInstance().getTable("Student").get());
            List<TableMeta> rightTables = Arrays.asList(Catalog.getInstance().getTable("Enrolled").get());

            Expression on = CCJSqlParserUtil.parseCondExpression("Student.A = Enrolled.A");

            JoinOperator join = new JoinOperator(left, right, on, leftTables, rightTables);

            Tuple first = join.getNextTuple();
            assertNotNull(first);
            assertEquals("1, 10, 1, 100", first.toString());

            assertNotNull(join.getNextTuple());

            join.reset();

            Tuple firstAgain = join.getNextTuple();
            assertNotNull(firstAgain);
            assertEquals(first.toString(), firstAgain.toString());
        }
    }

    // Does a null predicate correctly produce the cross product of the two inputs?
    @Test
    public void nullPredicate_behavesAsCrossProduct() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema(
                        "L A\n" +
                        "R B\n"
                )
                .writeTable("L",
                        "1\n" +
                        "2\n")
                .writeTable("R",
                        "10\n" +
                        "20\n")) {

            db.initCatalog();

            Operator left = new ScanOperator("L");
            Operator right = new ScanOperator("R");

            List<TableMeta> leftTables = Arrays.asList(Catalog.getInstance().getTable("L").get());
            List<TableMeta> rightTables = Arrays.asList(Catalog.getInstance().getTable("R").get());

            JoinOperator join = new JoinOperator(left, right, null, leftTables, rightTables);

            List<String> actual = Helpers.collectAll(join);

            List<String> expected = Arrays.asList(
                    "1, 10",
                    "1, 20",
                    "2, 10",
                    "2, 20"
            );

            assertEquals(expected, actual);
        }
    }
}