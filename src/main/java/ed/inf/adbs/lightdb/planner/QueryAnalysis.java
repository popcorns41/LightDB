package ed.inf.adbs.lightdb.planner;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * QueryAnalysis is a simple data class that captures the essential information about the SELECT clause of a SQL query after analysis.
 * It includes:
 * - isStar: whether the SELECT is a star select (SELECT * or SELECT T.*)
 * - projectedCols: the list of projected columns (non-aggregate) in the SELECT clause
 * - groupByCols: the list of columns in the GROUP BY clause
 * - aggregates: the list of aggregate function calls in the SELECT clause (e.g., SUM, COUNT)
 * 
 * This class is used by the QueryAnalyser to summarize the structure of the query, 
 * which in turn informs how we build the query plan (e.g., whether we need a GroupByOperator, what the output schema should be, etc.).
 * 
 * While aggegrates functions other than SUM are not supported in this coursework, we still capture them in the analysis for 
 * completeness and potential future extension.
 */

public final class QueryAnalysis {

    public static final class AggCall {
        public final String funcName;     // e.g. "sum", "count"
        public final Expression arg;     

        public AggCall(String funcName, Expression arg) {
            if (funcName == null) throw new NullPointerException("funcName");
            this.funcName = funcName.trim().toLowerCase(java.util.Locale.ROOT);
            this.arg = arg;
        }

        @Override
        public String toString() {
            if (arg == null) return funcName + "(*)";
            return funcName + "(" + arg.toString() + ")";
        }
    }

    public final boolean isStar;                 // SELECT * or T.*
    public final List<String> projectedCols;     // non-aggregate SELECT columns (normalized refs)
    public final List<String> groupByCols;       // GROUP BY columns (normalized refs)
    public final List<AggCall> aggregates;       // aggregate calls in SELECT

    public QueryAnalysis(boolean isStar,
                         List<String> projectedCols,
                         List<String> groupByCols,
                         List<AggCall> aggregates) {
        this.isStar = isStar;
        this.projectedCols = Collections.unmodifiableList(new ArrayList<String>(projectedCols));
        this.groupByCols = Collections.unmodifiableList(new ArrayList<String>(groupByCols));
        this.aggregates = Collections.unmodifiableList(new ArrayList<AggCall>(aggregates));
    }

    public boolean hasAggregation() {
        return !aggregates.isEmpty() || !groupByCols.isEmpty();
    }

    /**
     * Output schema *after* either projection or aggregation:
     * - non-agg: projected cols (or star schema list later)
     * - agg: projected group cols + aggregate labels
     */
    public List<String> expectedOutputRefsForAgg() {
        List<String> out = new ArrayList<String>(projectedCols.size() + aggregates.size());
        out.addAll(projectedCols);
        for (AggCall a : aggregates) {
            out.add(a.toString().toLowerCase(java.util.Locale.ROOT));
        }
        return out;
    }
}