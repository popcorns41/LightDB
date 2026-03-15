package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.util.TestDb;
import ed.inf.adbs.lightdb.operator.util.Helpers;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ProjectOperatorTest {

    // Does LightDB project columns in the right order? Are qualified column refs respected?
    @Test
    public void projectsSingleTable_columnsInRequestedOrder() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C D\n")
                .writeTable("Student",
                        "1,200,50,33\n" +
                        "2,200,200,44\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            // Project D, B, A
            ProjectOperator proj = new ProjectOperator(
                    scan,
                    Arrays.asList("Student.D", "Student.B", "Student.A"),
                    tables
            );

            List<String> actual = Helpers.collectAll(proj);

            assertEquals(2, actual.size());
            assertEquals("33, 200, 1", actual.get(0));
            assertEquals("44, 200, 2", actual.get(1));
        }
    }

    // Do unqualified column refs resolve correctly in single-table context?
    @Test
    public void projectsUnqualifiedColumns_singleTableAllowed() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "10,20,30\n" +
                        "11,21,31\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            // Unqualified refs should resolve in single-table context
            ProjectOperator proj = new ProjectOperator(
                    scan,
                    Arrays.asList("B", "A"),
                    tables
            );

            List<String> actual = Helpers.collectAll(proj);

            assertEquals(Arrays.asList("20, 10", "21, 11"), actual);
        }
    }

    // Does a qualified column ref that doesn't match the table cause failure?
    @Test
    public void qualifiedColumnRef_wrongTable_throws() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student", "1,2\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            try {
                new ProjectOperator(scan, Arrays.asList("OtherTable.A"), tables);
                fail("Expected IllegalArgumentException for qualified column ref with wrong table");
            } catch (IllegalArgumentException ex) {
                // good
            }
        }
    }

    // Does an unknown column in the projection cause failure?
    @Test
    public void unknownColumn_throws() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student", "1,2\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            try {
                new ProjectOperator(scan, Arrays.asList("Student.Z"), tables);
                fail("Expected IllegalArgumentException for unknown column");
            } catch (IllegalArgumentException ex) {
                // good
            }
        }
    }

    // Does a reset on the projection operator cause it to re-emit the projected stream from the beginning?
    @Test
    public void reset_rewindsProjectionStream() throws Exception {
        try (TestDb db = TestDb.create()
                .writeSchema("Student A B\n")
                .writeTable("Student",
                        "1,10\n" +
                        "2,20\n")) {

            db.initCatalog();

            Operator scan = new ScanOperator("Student");
            List<TableMeta> tables = Arrays.asList(Catalog.getInstance().getTable("Student").get());

            ProjectOperator proj = new ProjectOperator(scan, Arrays.asList("A"), tables);

            Tuple first = proj.getNextTuple();
            assertNotNull(first);
            assertEquals("1", first.toString());

            assertNotNull(proj.getNextTuple());

            proj.reset();

            Tuple firstAgain = proj.getNextTuple();
            assertNotNull(firstAgain);
            assertEquals("1", firstAgain.toString());
        }
    }

    // Does an ambiguous unqualified column ref in multi-table context cause failure?
    @Test
    public void ambiguousUnqualifiedColumn_twoTablesShouldThrow() throws Exception {
        // Only include this test if your resolver enforces ambiguity errors.
        // If your resolver "picks first table" silently, delete this test (but that design is dodgy).
        try (TestDb db = TestDb.create()
                .writeSchema(
                        "T1 A X\n" +
                        "T2 A Y\n"
                )
                .writeTable("T1", "1,100\n")
                .writeTable("T2", "1,200\n")) {

            db.initCatalog();

            List<TableMeta> tables = Arrays.asList(
                    Catalog.getInstance().getTable("T1").get(),
                    Catalog.getInstance().getTable("T2").get()
            );

            // child doesn't matter much; just scan one table
            Operator scan = new ScanOperator("T1");

            try {
                new ProjectOperator(scan, Arrays.asList("A"), tables);
                fail("Expected IllegalArgumentException for ambiguous unqualified column ref");
            } catch (IllegalArgumentException ex) {
                // good
            }
        }
    }
}