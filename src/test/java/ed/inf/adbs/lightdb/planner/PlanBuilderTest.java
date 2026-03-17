package ed.inf.adbs.lightdb.planner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue; // adjust package if needed
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;

import ed.inf.adbs.lightdb.operator.Operator;
import ed.inf.adbs.lightdb.planner.util.CwDbFixture;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class PlanBuilderTest {

    // Load the catalog before running any tests
    @BeforeClass
    public static void loadCatalog() throws Exception {
        CwDbFixture.ensureLoaded();
    }

    private Operator build(String sql) throws Exception {
        Select sel = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect ps = sel.getPlainSelect();
        return PlanBuilder.build(ps);
    }
    // Does planner accept SELECT * without GROUP BY?
    @Test
    public void selectStarIsAccepted() throws Exception {
        Operator root = build("SELECT * FROM Student;");
        assertNotNull(root);
    }

    // Does non aggregate select item have to be a column reference?
    @Test
    public void nonAggregateSelectItemMustBeColumn() throws Exception {
        try {
            build("SELECT 1 FROM Student;");
            fail("Expected IllegalArgumentException for non-column select item");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("non-aggregate"));
        }
    }

    // Does planner require aggregate functions to be at the end of the select list?
    @Test
    public void aggregatesMustAppearAtEnd() throws Exception {
        try {
            build("SELECT SUM(1), Student.A FROM Student GROUP BY Student.A;");
            fail("Expected IllegalArgumentException for aggregate not at end");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("end of select"));
        }
    }

    // Does planner require GROUP BY items to be column references?
    @Test
    public void groupByMustBeColumnsOnly() throws Exception {
        try {
            build("SELECT Student.A, SUM(1) FROM Student GROUP BY 1;");
            fail("Expected IllegalArgumentException for GROUP BY non-column");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("group by"));
        }
    }

    // Does planner require non-aggregate select items to appear in GROUP BY?
    @Test
    public void nonAggregateSelectColsMustAppearInGroupBy() throws Exception {
        try {
            build("SELECT Student.A, SUM(1) FROM Student GROUP BY Student.B;");
            fail("Expected IllegalArgumentException because Student.A not in GROUP BY");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("must appear in group by"));
        }
    }

    // Does planner require ORDER BY items to be in the output schema?
    @Test
    public void orderByNotInOutputSchemaIsRejected() throws Exception {
        try {
            build("SELECT Student.A FROM Student ORDER BY Student.B;");
            fail("Expected IllegalArgumentException for ORDER BY not available after projection");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("order by"));
        }
    }

    // Does planner reject SELECT * if GROUP BY is present?
    @Test
    public void selectStarWithGroupByRejected() throws Exception {
        try {
            build("SELECT * FROM Student GROUP BY Student.A;");
            fail("Expected IllegalArgumentException for SELECT * with GROUP BY");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("select *"));
        }
    }

    // smoke test: does a valid query build a plan without throwing exceptions?
    @Test
    public void smoke_validQueryBuildsPlan() throws Exception {
        Operator root = build("SELECT Student.A FROM Student;");
        assertNotNull(root);
    }
}