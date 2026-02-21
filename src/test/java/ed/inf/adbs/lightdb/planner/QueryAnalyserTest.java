package ed.inf.adbs.lightdb.planner;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class QueryAnalyserTest {
    private QueryAnalysis analyse(String sql) throws Exception {
        Select sel = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect ps = sel.getPlainSelect();
        return QueryAnalyser.analyze(ps);
    }

    // Does a simple SELECT * FROM Student have the expected analysis results (isStar=true, no aggs, no group by, etc)?
    @Test
    public void selectStar_noAgg_noGroupBy() throws Exception {
        QueryAnalysis qa = analyse("SELECT * FROM Student;");

        assertTrue(qa.isStar);
        assertFalse(qa.hasAggregation());
        assertTrue(qa.projectedCols.isEmpty());
        assertTrue(qa.groupByCols.isEmpty());
        assertTrue(qa.aggregates.isEmpty());
    }

    // Does a SELECT with GROUP BY but no aggregates throw an exception (as per cw requirements)?
    @Test
    public void selectTableStar_noAgg_noGroupBy() throws Exception {
        QueryAnalysis qa = analyse("SELECT Student.* FROM Student;");

        assertTrue(qa.isStar);
        assertFalse(qa.hasAggregation());
        assertTrue(qa.projectedCols.isEmpty());
        assertTrue(qa.groupByCols.isEmpty());
        assertTrue(qa.aggregates.isEmpty());
    }

    // Does a SELECT with GROUP BY but no aggregates throw an exception (as per cw requirements)?
    @Test
    public void starWithGroupByRejected() throws Exception {
        try {
            analyse("SELECT * FROM Student GROUP BY Student.A;");
            fail("Expected IllegalArgumentException for SELECT * with GROUP BY");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("select *"));
        }
    }

    // Does a non-aggregate select item throw an exception?
    @Test
    public void nonAggregateSelectItemMustBeColumn() throws Exception {
        try {
            analyse("SELECT 1 FROM Student;");
            fail("Expected IllegalArgumentException for non-column select item");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("non-aggregate"));
        }
    }

    // Does an ORDER BY item that is not in the output schema throw an exception?
    @Test
    public void simpleProjectionColumnsNormalized() throws Exception {
        QueryAnalysis qa = analyse("SELECT Student.A, Student.B FROM Student;");

        assertFalse(qa.isStar);
        assertFalse(qa.hasAggregation());

        assertEquals(2, qa.projectedCols.size());
        assertEquals("student.a", qa.projectedCols.get(0));
        assertEquals("student.b", qa.projectedCols.get(1));
    }

    // Does a simple SELECT with GROUP BY and aggregation have the expected analysis results (isStar=false, hasAggregation=true, projected cols, group by cols, and aggregates all correct)?
    @Test
    public void sumAtEndAccepted() throws Exception {
        QueryAnalysis qa = analyse("SELECT Student.B, SUM(1) FROM Student GROUP BY Student.B;");

        assertFalse(qa.isStar);
        assertTrue(qa.hasAggregation());

        assertEquals(1, qa.projectedCols.size());
        assertEquals("student.b", qa.projectedCols.get(0));

        assertEquals(1, qa.groupByCols.size());
        assertEquals("student.b", qa.groupByCols.get(0));

        assertEquals(1, qa.aggregates.size());
        assertEquals("sum", qa.aggregates.get(0).funcName);
        assertNotNull(qa.aggregates.get(0).arg);

        // expected output schema in agg mode: projectedCols + agg labels
        List<String> outRefs = qa.expectedOutputRefsForAgg();
        assertEquals(2, outRefs.size());
        assertEquals("student.b", outRefs.get(0));
        assertTrue(outRefs.get(1).startsWith("sum("));
    }

    // Does an aggregate select item that is not at the end of the select list throw an exception?
    @Test
    public void aggregatesMustAppearAtEndRejected() throws Exception {
        try {
            analyse("SELECT SUM(1), Student.A FROM Student GROUP BY Student.A;");
            fail("Expected IllegalArgumentException because aggregate is not at end");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("end of select"));
        }
    }

    // Does a GROUP BY item that is not a column throw an exception?
    @Test
    public void groupByOnlySupportsColumnsRejected() throws Exception {
        try {
            analyse("SELECT Student.A, SUM(1) FROM Student GROUP BY 1;");
            fail("Expected IllegalArgumentException for GROUP BY non-column");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("group by"));
        }
    }

    // Does a non-aggregate select item that is not in the GROUP BY throw an exception?
    @Test
    public void nonAggregateSelectColsMustBeInGroupByRejected() throws Exception {
        try {
            analyse("SELECT Student.A, SUM(1) FROM Student GROUP BY Student.B;");
            fail("Expected IllegalArgumentException because Student.A not in GROUP BY");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("must appear in group by"));
        }
    }

    // Does an ORDER BY item that is not in the output schema throw an exception?
    @Test
    public void sumOnlyWithGroupBy_hasNoProjectedCols() throws Exception {
        QueryAnalysis qa = analyse("SELECT SUM(1) FROM Student GROUP BY Student.B;");

        assertFalse(qa.isStar);
        assertTrue(qa.hasAggregation());

        assertTrue("No non-agg projected cols expected", qa.projectedCols.isEmpty());
        assertEquals(1, qa.groupByCols.size());
        assertEquals("student.b", qa.groupByCols.get(0));

        assertEquals(1, qa.aggregates.size());
        assertEquals("sum", qa.aggregates.get(0).funcName);

        // Output schema for agg should be: [] + [sum(...)]
        List<String> outRefs = qa.expectedOutputRefsForAgg();
        assertEquals(1, outRefs.size());
        assertTrue(outRefs.get(0).startsWith("sum("));
    }

    // Does an unsupported aggregate throw an exception?
    @Test
    public void countStarParsedAsAgg() throws Exception {
        // You haven't implemented COUNT in execution yet, but the analyzer supports it.
        QueryAnalysis qa = analyse("SELECT COUNT(*) FROM Student;");

        assertFalse(qa.isStar);
        assertTrue(qa.hasAggregation());

        assertTrue(qa.projectedCols.isEmpty());
        assertTrue(qa.groupByCols.isEmpty());

        assertEquals(1, qa.aggregates.size());
        assertEquals("count", qa.aggregates.get(0).funcName);
        assertNull("COUNT(*) arg should be null in our model", qa.aggregates.get(0).arg);
    }

    // Does COUNT(column) (an unsupported aggregate) throw an exception?
    @Test
    public void countColumnParsedAsAgg() throws Exception {
        QueryAnalysis qa = analyse("SELECT COUNT(Student.A) FROM Student;");

        assertEquals(1, qa.aggregates.size());
        assertEquals("count", qa.aggregates.get(0).funcName);
        assertNotNull(qa.aggregates.get(0).arg);
    }

    // Does an unsupported aggregate throw an exception?
    @Test
    public void unsupportedAggregateRejected() throws Exception {
        // Analyzer currently only recognizes sum/count.
        try {
            analyse("SELECT MIN(Student.A) FROM Student;");
            fail("Expected IllegalArgumentException for unsupported aggregate");
        } catch (IllegalArgumentException ex) {
            // message comes from non-agg item check, OR unsupported aggregate depending on your analyzer config
            // We'll accept either as long as it fails.
            assertNotNull(ex.getMessage());
        }
    }
}