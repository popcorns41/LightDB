package ed.inf.adbs.lightdb.planner;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import ed.inf.adbs.lightdb.util.ExpressionUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

public class WhereClassifierTest {

    // Does extractSingleTable extract predicates that only reference the given table,
    // and leave the rest behind?
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

        Set<String> left = new HashSet<>();
        left.add("Student");

        List<Expression> join = wc.extractJoinPredicates(left, "Enrolled");
        assertEquals(1, join.size());
        assertTrue(ExpressionUtils.referencedTables(join.get(0)).contains("student"));
        assertTrue(ExpressionUtils.referencedTables(join.get(0)).contains("enrolled"));

        assertTrue(wc.getRemaining().isEmpty());
    }

    // Does extractSingleTable avoid pulling predicates that reference other tables?
    @Test
    public void doesNotExtractPredicatesReferencingOtherTables() throws Exception {
        Expression where = CCJSqlParserUtil.parseCondExpression(
                "Student.B = 200 AND Enrolled.H = 75 AND Student.A = Enrolled.A"
        );

        WhereClassifier wc = new WhereClassifier(where);

        List<Expression> s = wc.extractSingleTable("Student");
        assertEquals(1, s.size());
        assertTrue(ExpressionUtils.referencedTables(s.get(0)).contains("student"));
        assertFalse(ExpressionUtils.referencedTables(s.get(0)).contains("enrolled"));

        List<Expression> e = wc.extractSingleTable("Enrolled");
        assertEquals(1, e.size());
        assertTrue(ExpressionUtils.referencedTables(e.get(0)).contains("enrolled"));
        assertFalse(ExpressionUtils.referencedTables(e.get(0)).contains("student"));

        Set<String> left = new HashSet<>();
        left.add("Student");

        List<Expression> join = wc.extractJoinPredicates(left, "Enrolled");
        assertEquals(1, join.size());
        assertTrue(ExpressionUtils.referencedTables(join.get(0)).contains("student"));
        assertTrue(ExpressionUtils.referencedTables(join.get(0)).contains("enrolled"));

        assertTrue(wc.getRemaining().isEmpty());
    }
}