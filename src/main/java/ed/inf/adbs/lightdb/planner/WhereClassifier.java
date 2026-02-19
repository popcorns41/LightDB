package ed.inf.adbs.lightdb.planner;

import ed.inf.adbs.lightdb.util.ExpressionUtils;
import net.sf.jsqlparser.expression.Expression;

import java.util.*;

public final class WhereClassifier {

    private final List<Expression> remaining;

    public WhereClassifier(Expression where) {
        this.remaining = new ArrayList<Expression>(ExpressionUtils.splitConjuncts(where));
    }

    /** Predicates that reference only this table (or none). */
    public List<Expression> extractSingleTable(String tableName) {
        List<Expression> out = new ArrayList<Expression>();
        Iterator<Expression> it = remaining.iterator();

        while (it.hasNext()) {
            Expression e = it.next();
            Set<String> refs = ExpressionUtils.referencedTables(e);

            if (refs.isEmpty() || (refs.size() == 1 && refs.contains(tableName))) {
                out.add(e);
                it.remove();
            }
        }
        return out;
    }

    /** Predicates that connect (any of) leftTables to rightTable. */
    public List<Expression> extractJoinPredicates(Set<String> leftTables, String rightTable) {
        List<Expression> out = new ArrayList<Expression>();
        Iterator<Expression> it = remaining.iterator();

        while (it.hasNext()) {
            Expression e = it.next();
            Set<String> refs = ExpressionUtils.referencedTables(e);

            boolean touchesRight = refs.contains(rightTable);
            boolean touchesLeft = false;
            for (String lt : leftTables) {
                if (refs.contains(lt)) { touchesLeft = true; break; }
            }

            if (touchesLeft && touchesRight) {
                out.add(e);
                it.remove();
            }
        }
        return out;
    }

    public List<Expression> getRemaining() {
        return new ArrayList<Expression>(remaining);
    }
}
