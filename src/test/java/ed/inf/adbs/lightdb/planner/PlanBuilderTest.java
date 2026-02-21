package ed.inf.adbs.lightdb.planner;

import ed.inf.adbs.lightdb.planner.util.CwDbFixture;

import ed.inf.adbs.lightdb.operator.Operator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class PlanBuilderTest {

    @BeforeClass
    public static void loadCatalog() throws Exception {
        CwDbFixture.ensureLoaded();
    }

    // Helper method to build a plan from a SQL string. This allows us to write more concise tests.
    private Operator build(String sql) throws Exception {
        Select sel = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect ps = sel.getPlainSelect();
        return PlanBuilder.build(ps);
    }

    //Smoking gun test: does a simple SELECT * FROM Student build a plan without throwing an exception?
    //Basically, does my PlanBuiler compile and not crash? :D
    @Test
    public void selectStarIsAccepted() throws Exception {
        Operator root = build("SELECT * FROM Student;");
        assertNotNull(root);
    }

    // Does a non-SELECT statement throw an exception?
    @Test
    public void nonAggregateSelectItemMustBeColumn() throws Exception {
        try {
            build("SELECT 1 FROM Student;");
            fail("Expected IllegalArgumentException for non-column select item");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("non-aggregate"));
        }
    }

    // Does an aggregate select item that is not at the end of the select list throw an exception?
    @Test
    public void aggregatesMustAppearAtEnd() throws Exception {
        try {
            build("SELECT SUM(1), Student.A FROM Student GROUP BY Student.A;");
            fail("Expected IllegalArgumentException for aggregate not at end");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("end of select"));
        }
    }

    // Does a GROUP BY item that is not a column throw an exception?
    @Test
    public void groupByMustBeColumnsOnly() throws Exception {
        try {
            build("SELECT Student.A, SUM(1) FROM Student GROUP BY 1;");
            fail("Expected IllegalArgumentException for GROUP BY non-column");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("group by"));
        }
    }

    // Does a non-aggregate select item that is not in the GROUP BY throw an exception?
    @Test
    public void nonAggregateSelectColsMustAppearInGroupBy() throws Exception {
        try {
            build("SELECT Student.A, SUM(1) FROM Student GROUP BY Student.B;");
            fail("Expected IllegalArgumentException because Student.A not in GROUP BY");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("must appear in group by"));
        }
    }

    // Does an ORDER BY item that is not in the output schema throw an exception?
    @Test
    public void orderByNotInOutputSchemaIsRejected() throws Exception {
        try {
            build("SELECT Student.A FROM Student ORDER BY Student.B;");
            fail("Expected IllegalArgumentException for ORDER BY not available after projection");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("order by"));
        }
    }

    // Does a SELECT * with GROUP BY throw an exception (as per cw requirements)?
    @Test
    public void selectStarWithGroupByRejected() throws Exception {
        // Your analyzer explicitly rejects SELECT * with GROUP BY
        try {
            build("SELECT * FROM Student GROUP BY Student.A;");
            fail("Expected IllegalArgumentException for SELECT * with GROUP BY");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("select *"));
        }
    }

    // Does a valid query build a plan without throwing an exception?
    @Test
    public void smoke_validQueryBuildsPlan() throws Exception {
        Operator root = build("SELECT Student.A FROM Student;");
        assertNotNull(root);
    }
}