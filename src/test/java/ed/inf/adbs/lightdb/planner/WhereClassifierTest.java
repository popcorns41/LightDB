package ed.inf.adbs.lightdb.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import ed.inf.adbs.lightdb.util.ExpressionUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

public class WhereClassifierTest {
    // Does splitSingleTable extract predicates that only reference the given table, and leave the rest behind?
    @Test
    public void splitsSingleTableJoinAndLeftovers() throws Exception {
        Expression where = CCJSqlParserUtil.parseCondExpression(
                "Student.B = 200 AND Enrolled.H = 75 AND Student.A = Enrolled.A"
        );

        WhereClassifier wc = new WhereClassifier(where);

        List<Expression> s = wc.extractSingleTable("Student");
        assertEquals(1, s.size());

        List<Expression> e = wc.extractSingleTable("Enrolled");
        assertEquals(1, e.size());

        Set<String> left = new HashSet<String>();
        left.add("Student");

        List<Expression> join = wc.extractJoinPredicates(left, "Enrolled");
        assertEquals(1, join.size());
        assertTrue(ExpressionUtils.referencedTables(join.get(0)).contains("Student"));
        assertTrue(ExpressionUtils.referencedTables(join.get(0)).contains("Enrolled"));

        assertTrue(wc.getRemaining().isEmpty());
    }

    // Does extractSingleTable not extract predicates that reference other tables, even if they also reference the given table?
    @Test
    public void doesNotExtractPredicatesReferencingOtherTables() throws Exception {
        Expression where = CCJSqlParserUtil.parseCondExpression(
                "Student.B = 200 AND Enrolled.H = 75 AND Student.A = Enrolled.A"
        );

        WhereClassifier wc = new WhereClassifier(where);

        List<Expression> s = wc.extractSingleTable("Student");
        assertEquals(1, s.size());
        assertTrue(ExpressionUtils.referencedTables(s.get(0)).contains("Student"));
        assertFalse(ExpressionUtils.referencedTables(s.get(0)).contains("Enrolled"));

        List<Expression> e = wc.extractSingleTable("Enrolled");
        assertEquals(1, e.size());
        assertTrue(ExpressionUtils.referencedTables(e.get(0)).contains("Enrolled"));
        assertFalse(ExpressionUtils.referencedTables(e.get(0)).contains("Student"));

        Set<String> left = new HashSet<String>();
        left.add("Student");

        List<Expression> join = wc.extractJoinPredicates(left, "Enrolled");
        assertEquals(1, join.size());
        assertTrue(ExpressionUtils.referencedTables(join.get(0)).contains("Student"));
        assertTrue(ExpressionUtils.referencedTables(join.get(0)).contains("Enrolled"));

        assertTrue(wc.getRemaining().isEmpty());
    }
}
