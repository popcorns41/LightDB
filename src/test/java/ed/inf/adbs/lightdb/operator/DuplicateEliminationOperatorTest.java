package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.operator.util.Helpers;
import ed.inf.adbs.lightdb.operator.util.TestDb;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class DuplicateEliminationOperatorTest {

    // Does DuplicateEliminationOperator correctly remove duplicate tuples while preserving the order of first occurrences?
    @Test
    public void removesDuplicateTuples_preservesFirstOccurrenceOrder() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "1,10\n" +
                        "2,20\n" +
                        "1,10\n" +
                        "3,30\n" +
                        "2,20\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            DuplicateEliminationOperator distinct = new DuplicateEliminationOperator(scan);

            List<String> actual = Helpers.collectAll(distinct);

            // LinkedHashSet semantics: keep first occurrence order
            List<String> expected = Arrays.asList(
                    "1, 10",
                    "2, 20",
                    "3, 30"
            );

            assertEquals(expected, actual);
        }
    }

    // Does DuplicateEliminationOperator correctly handle the case where there are no duplicates and simply returns all tuples in order?
    @Test
    public void doesNotRemoveDistinctTuples() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "2,20\n" +
                        "3,30\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            DuplicateEliminationOperator distinct = new DuplicateEliminationOperator(scan);

            List<String> actual = Helpers.collectAll(distinct);

            List<String> expected = Arrays.asList(
                    "1, 10",
                    "2, 20",
                    "3, 30"
            );

            assertEquals(expected, actual);
        }
    }

    // Does DuplicateEliminationOperator correctly handle the case where all tuples are duplicates and returns a single tuple?
    @Test
    public void removesDuplicatesAfterProjection() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,10,100\n" +
                        "2,10,200\n" +
                        "3,20,300\n" +
                        "4,10,400\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");

            // Project only B so duplicates appear
            ProjectOperator proj = new ProjectOperator(
                    scan,
                    Arrays.asList("B"),
                    Arrays.asList(ed.inf.adbs.lightdb.catalog.Catalog.getInstance().getTable("Student").get())
            );

            DuplicateEliminationOperator distinct = new DuplicateEliminationOperator(proj);

            List<String> actual = Helpers.collectAll(distinct);

            List<String> expected = Arrays.asList(
                    "10",
                    "20"
            );

            assertEquals(expected, actual);
        }
    }

    // Does a reset on the DuplicateEliminationOperator correctly rewinds the input and allows re-scanning the distinct tuples?
    @Test
    public void reset_rewindsDistinctStream() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "1,10\n" +
                        "2,20\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            DuplicateEliminationOperator distinct = new DuplicateEliminationOperator(scan);

            Tuple first = distinct.getNextTuple();
            assertNotNull(first);
            assertEquals("1, 10", first.toString());

            Tuple second = distinct.getNextTuple();
            assertNotNull(second);
            assertEquals("2, 20", second.toString());

            assertNull(distinct.getNextTuple());

            distinct.reset();

            Tuple firstAgain = distinct.getNextTuple();
            assertNotNull(firstAgain);
            assertEquals("1, 10", firstAgain.toString());
        }
    }

    // Does a qualified column ref in the projection that references the wrong table cause failure?
    @Test
    public void emptyInput_returnsNoTuples() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student", "")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            DuplicateEliminationOperator distinct = new DuplicateEliminationOperator(scan);

            List<String> actual = Helpers.collectAll(distinct);

            assertTrue(actual.isEmpty());
        }
    }

    // Does an unknown operator in the WHERE cause a runtime failure?
    @Test   
    public void allDuplicates_returnsSingleTuple() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "1,10\n" +
                        "1,10\n" +
                        "1,10\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            DuplicateEliminationOperator distinct = new DuplicateEliminationOperator(scan);

            List<String> actual = Helpers.collectAll(distinct);

            List<String> expected = Arrays.asList("1, 10");

            assertEquals(expected, actual);
        }
    }
}