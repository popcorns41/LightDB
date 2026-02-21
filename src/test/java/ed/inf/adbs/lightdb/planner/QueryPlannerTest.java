package ed.inf.adbs.lightdb.planner;

import ed.inf.adbs.lightdb.operator.Operator;
import ed.inf.adbs.lightdb.planner.util.CwDbFixture;
import ed.inf.adbs.lightdb.planner.util.TestUtils;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

public class QueryPlannerTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @BeforeClass
    public static void loadCatalog() throws Exception {
        CwDbFixture.ensureLoaded();
    }

    //Does the output of SELECT * FROM Student match that of our Sudent table?
    @Test
    public void validSelectFileBuildsAndRuns() throws Exception {
        File sqlDir = tmp.newFolder("sql");
        File sql = TestUtils.writeSqlTemp(sqlDir, "q.sql", "SELECT * FROM Student;");

        Operator root = QueryPlanner.buildPlan(sql.getPath());
        List<String> actual = TestUtils.collectAll(root);

        Path expectedCsv = Paths.get("samples", "db", "data", "Student.csv");
        List<String> expected = TestUtils.readAllLines(expectedCsv);

        assertEquals(expected, actual);
    }

    //Does the output of SELECT DISTINCT Student.A FROM Student ORDER BY Student.A match the distinct values of A in Student.csv, sorted?
    @Test
    public void nonSelectStatementThrows() throws Exception {
        File sqlDir = tmp.newFolder("sql2");
        File sql = TestUtils.writeSqlTemp(sqlDir, "q.sql", "UPDATE Student SET A=1;");

        try {
            QueryPlanner.buildPlan(sql.getPath());
            fail("Expected RuntimeException wrapping an IllegalArgumentException for non-SELECT.");
        } catch (RuntimeException ex) {
            assertNotNull(ex.getCause());
            assertTrue(ex.getCause() instanceof IllegalArgumentException);
        }
    }

    // Does an ORDER BY on a column not in the projection throw an exception?
    @Test
    public void orderByNotInProjectionThrowsWrapped() throws Exception {
        File sqlDir = tmp.newFolder("sql3");
        File sql = TestUtils.writeSqlTemp(sqlDir, "q.sql", "SELECT Student.A FROM Student ORDER BY Student.B;");

        try {
            QueryPlanner.buildPlan(sql.getPath());
            fail("Expected RuntimeException wrapping IllegalArgumentException due to ORDER BY not in projection.");
        } catch (RuntimeException ex) {
            assertNotNull(ex.getCause());
            assertTrue(ex.getCause() instanceof IllegalArgumentException);
            assertTrue(ex.getCause().getMessage().toLowerCase().contains("order by"));
        }
    }
}