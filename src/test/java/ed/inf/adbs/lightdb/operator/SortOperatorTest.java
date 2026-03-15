package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.util.Helpers;
import ed.inf.adbs.lightdb.operator.util.TestDb;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class SortOperatorTest {

    // Baseline: Does SortOperator produce correct output for a simple ascending sort on a single column?
    @Test
    public void sortsAscending_singleColumn() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "3,100,9\n" +
                        "1,300,8\n" +
                        "2,200,7\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            SortOperator sort = new SortOperator(
                    scan,
                    Arrays.asList("Student.A"),
                    Arrays.asList(Boolean.TRUE),
                    Arrays.asList("student.a", "student.b", "student.c")
            );

            List<String> actual = Helpers.collectAll(sort);

            List<String> expected = Arrays.asList(
                    "1, 300, 8",
                    "2, 200, 7",
                    "3, 100, 9"
            );

            assertEquals(expected, actual);
        }
    }

    // Does SortOperator produce correct output for a simple descending sort on a single column?
    @Test
    public void sortsDescending_singleColumn() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "3,30\n" +
                        "2,20\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            SortOperator sort = new SortOperator(
                    scan,
                    Arrays.asList("Student.A"),
                    Arrays.asList(Boolean.FALSE),
                    Arrays.asList("student.a", "student.b")
            );

            List<String> actual = Helpers.collectAll(sort);

            List<String> expected = Arrays.asList(
                    "3, 30",
                    "2, 20",
                    "1, 10"
            );

            assertEquals(expected, actual);
        }
    }

    // Does SortOperator produce correct output when sorting by multiple columns with different sort directions?
    @Test
    public void sortsByMultipleColumns() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,200,9\n" +
                        "2,100,7\n" +
                        "3,200,5\n" +
                        "4,100,8\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            // ORDER BY B ASC, C DESC
            SortOperator sort = new SortOperator(
                    scan,
                    Arrays.asList("Student.B", "Student.C"),
                    Arrays.asList(Boolean.TRUE, Boolean.FALSE),
                    Arrays.asList("student.a", "student.b", "student.c")
            );

            List<String> actual = Helpers.collectAll(sort);

            List<String> expected = Arrays.asList(
                    "4, 100, 8",
                    "2, 100, 7",
                    "1, 200, 9",
                    "3, 200, 5"
            );

            assertEquals(expected, actual);
        }
    }
    

    // Does SortOperator support unqualified column refs in ORDER BY when they are unambiguous?
    @Test
    public void supportsUnqualifiedColumns_whenUnambiguous() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "2,20\n" +
                        "1,10\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            SortOperator sort = new SortOperator(
                    scan,
                    Arrays.asList("A"),
                    Arrays.asList(Boolean.TRUE),
                    Arrays.asList("student.a", "student.b")
            );

            List<String> actual = Helpers.collectAll(sort);

            List<String> expected = Arrays.asList(
                    "1, 10",
                    "2, 20"
            );

            assertEquals(expected, actual);
        }
    }
    
    // Does SortOperator throw an exception if the ORDER BY clause references an unknown column?
    @Test 
    public void unknownOrderByColumn_throws() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student", "1,10\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            try {
                new SortOperator(
                        scan,
                        Arrays.asList("Student.Z"),
                        Arrays.asList(Boolean.TRUE),
                        Arrays.asList("student.a", "student.b")
                );
                fail("Expected IllegalArgumentException for unknown ORDER BY column");
            } catch (IllegalArgumentException ex) {
                // good
            }
        }
    }

    // Does SortOperator throw an exception if the ORDER BY clause references a column that is not in the projected schema?
    @Test
    public void orderByColumnNotPresentAfterProjection_throws() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,20,100\n" +
                        "2,10,200\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            // Project only A and C
            ProjectOperator proj = new ProjectOperator(
                    scan,
                    Arrays.asList("Student.A", "Student.C"),
                    tables
            );

            try {
                new SortOperator(
                        proj,
                        Arrays.asList("Student.B"),
                        Arrays.asList(Boolean.TRUE),
                        proj.getOutputRefs()
                );
                fail("Expected IllegalArgumentException because ORDER BY column is not in projected schema");
            } catch (IllegalArgumentException ex) {
                // good
            }
        }
    }

    //Do reset on the SortOperator correctly rewinds the input and allows re-scanning the sorted output?
    @Test
    public void reset_rewindsSortedStream() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "3,30\n" +
                        "1,10\n" +
                        "2,20\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            SortOperator sort = new SortOperator(
                    scan,
                    Arrays.asList("Student.A"),
                    Arrays.asList(Boolean.TRUE),
                    Arrays.asList("student.a", "student.b")
            );

            Tuple first = sort.getNextTuple();
            assertNotNull(first);
            assertEquals("1, 10", first.toString());

            assertNotNull(sort.getNextTuple());

            sort.reset();

            Tuple firstAgain = sort.getNextTuple();
            assertNotNull(firstAgain);
            assertEquals("1, 10", firstAgain.toString());
        }
    }

    // Are ties handled deterministically? 
    @Test
    public void tiesAreHandledDeterministically() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "2,10\n" +
                        "3,10\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            SortOperator sort = new SortOperator(
                    scan,
                    Arrays.asList("Student.B"),
                    Arrays.asList(Boolean.TRUE),
                    Arrays.asList("student.a", "student.b")
            );

            List<String> actual = Helpers.collectAll(sort);

            // Your SortOperator currently uses a deterministic tie-breaker on full tuple values.
            List<String> expected = Arrays.asList(
                    "1, 10",
                    "2, 10",
                    "3, 10"
            );

            assertEquals(expected, actual);
        }
    }
}