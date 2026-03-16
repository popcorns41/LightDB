package ed.inf.adbs.lightdb.planner;

import ed.inf.adbs.lightdb.operator.Operator;
import ed.inf.adbs.lightdb.planner.util.TestUtils;
import ed.inf.adbs.lightdb.planner.util.CwDbFixture; // use your actual package if different

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class ProjectionPushdownRegressionTest {

    @BeforeClass
    public static void loadCatalog() throws Exception {
        CwDbFixture.ensureLoaded();
    }

    private PlainSelect parse(String sql) throws Exception {
        Select sel = (Select) CCJSqlParserUtil.parse(sql);
        return sel.getPlainSelect();
    }

    // Does a simple SELECT * still return all columns of Student after projection pushdown?
    @Test
    public void selectStar_filterDoesNotLoseColumns() throws Exception {
        PlainSelect ps = parse(
                "SELECT * FROM Student WHERE Student.B = 200;"
        );

        Operator root = PlanBuilder.build(ps);
        List<String> actual = TestUtils.collectAll(root);

        List<String> expected = Arrays.asList(
                "1, 200, 50, 33",
                "2, 200, 200, 44"
        );

        assertEquals(expected, actual);
    }

    // Does a SELECT of specific columns still return the correct columns after projection pushdown?
    @Test
    public void joinRetainsColumnsNeededForJoinPredicate() throws Exception {
        PlainSelect ps = parse(
                "SELECT Student.A FROM Student, Enrolled WHERE Student.A = Enrolled.A;"
        );

        Operator root = PlanBuilder.build(ps);
        List<String> actual = TestUtils.collectAll(root);

        // order-insensitive bag comparison, because no ORDER BY
        List<String> expected = Arrays.asList(
                "1",
                "1",
                "1",
                "2",
                "3",
                "4"
        );

        TestUtils.assertBagEquals(expected, actual);
    }

    // Does a SELECT with a filter on a non-projected column still return the correct results after projection pushdown?
    @Test
    public void joinAndFilterRetainOnlyNecessaryColumns() throws Exception {
        PlainSelect ps = parse(
                "SELECT Student.D " +
                "FROM Student, Enrolled " +
                "WHERE Student.A = Enrolled.A AND Enrolled.H > 50;"
        );

        Operator root = PlanBuilder.build(ps);
        List<String> actual = TestUtils.collectAll(root);

        // Matches:
        // (1,101,75), (1,102,82), (1,103,92), (3,102,52)
        // Student.D values: 33, 33, 33, 44
        List<String> expected = Arrays.asList(
                "33",
                "33",
                "33",
                "44"
        );

        TestUtils.assertBagEquals(expected, actual);
    }

    // Does a SELECT with a GROUP BY on a non-projected column still return the correct results after projection pushdown?
    @Test
    public void groupByAndSumStillWorkAfterProjectionPushdown() throws Exception {
        PlainSelect ps = parse(
                "SELECT Student.B, SUM(Student.C) " +
                "FROM Student " +
                "GROUP BY Student.B;"
        );

        Operator root = PlanBuilder.build(ps);
        List<String> actual = TestUtils.collectAll(root);

        // first-seen group order from Student.csv: 200, 100, 300
        List<String> expected = Arrays.asList(
                "200, 250",
                "100, 655",
                "300, 400"
        );

        assertEquals(expected, actual);
    }

    // Does a SELECT with an ORDER BY on a non-projected column still return the correct results after projection pushdown?
    @Test
    public void orderByProjectedColumnStillWorks() throws Exception {
        PlainSelect ps = parse(
                "SELECT Student.A FROM Student ORDER BY Student.A;"
        );

        Operator root = PlanBuilder.build(ps);
        List<String> actual = TestUtils.collectAll(root);

        List<String> expected = Arrays.asList(
                "1",
                "2",
                "3",
                "4",
                "5",
                "6"
        );

        assertEquals(expected, actual);
    }

    // Does a SELECT with an ORDER BY on a non-projected column still return the correct results after projection pushdown?
    @Test
    public void requiredColumnsAnalysis_marksJoinFilterAggregateAndOutputColumns() throws Exception {
        PlainSelect ps = parse(
                "SELECT Student.B, SUM(Student.C) " +
                "FROM Student, Enrolled " +
                "WHERE Student.A = Enrolled.A AND Enrolled.H > 50 " +
                "GROUP BY Student.B " +
                "ORDER BY Student.B;"
        );

        QueryAnalysis qa = QueryAnalyser.analyze(ps);
        Map<String, Set<String>> required = RequiredColumnsAnalyser.analyse(ps, qa);

        assertTrue(required.containsKey("student"));
        assertTrue(required.containsKey("enrolled"));

        // Student needs:
        // A for join
        // B for select/group/order
        // C for sum
        assertTrue(required.get("student").contains("a"));
        assertTrue(required.get("student").contains("b"));
        assertTrue(required.get("student").contains("c"));

        // Enrolled needs:
        // A for join
        // H for filter
        assertTrue(required.get("enrolled").contains("a"));
        assertTrue(required.get("enrolled").contains("h"));
    }
}