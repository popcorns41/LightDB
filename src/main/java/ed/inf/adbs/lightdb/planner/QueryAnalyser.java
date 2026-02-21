package ed.inf.adbs.lightdb.planner;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

/**
 * QueryAnalyser is a utility class that analyzes a parsed SQL query (specifically a PlainSelect) 
 * and extracts key information about the SELECT clause, GROUP BY clause, and aggregate functions.
 * It produces a QueryAnalysis object that summarizes:
 * - Whether the SELECT is a star select (SELECT * or SELECT T.*)
 * - The list of projected columns (non-aggregate) in the SELECT clause
 * - The list of columns in the GROUP BY clause
 * - The list of aggregate function calls in the SELECT clause (e.g., SUM, COUNT)
 * 
 * The need for QueryAnalyser arises from the fact that the presence of aggregates and GROUP BY affects how we plan the query execution.
 */

public final class QueryAnalyser {
    private QueryAnalyser() {}

    public static QueryAnalysis analyze(PlainSelect ps) {
        if (ps == null) throw new NullPointerException("ps");

        List<SelectItem<?>> items = ps.getSelectItems();
        if (items == null || items.isEmpty()) {
            throw new IllegalArgumentException("SELECT list is empty");
        }

        // 1) Detect star
        boolean isStar = isStarSelect(items);

        // 2) Parse GROUP BY columns (these define grouping keys)
        List<String> groupByCols = parseGroupByCols(ps);

        // 3) If star, classify quickly
        if (isStar) {

            if (!groupByCols.isEmpty()) {
                throw new IllegalArgumentException("SELECT * with GROUP BY is not supported");
            }
            // Also reject SELECT * with aggregates
            // (if someone writes SELECT *, SUM(...) that's nonsense)
            if (containsAnyAggregate(items)) {
                throw new IllegalArgumentException("SELECT * with aggregates is not supported");
            }

            return new QueryAnalysis(true,
                    Collections.<String>emptyList(),
                    Collections.<String>emptyList(),
                    Collections.<QueryAnalysis.AggCall>emptyList());
        }

        // 4) Parse SELECT list into: projectedCols (non-agg) and aggregates
        List<String> projectedCols = new ArrayList<String>();
        List<QueryAnalysis.AggCall> aggs = new ArrayList<QueryAnalysis.AggCall>();

        boolean seenAgg = false;

        for (SelectItem<?> si : items) {
            Expression e = si.getExpression();

            if (isAggregateFunction(e)) {
                seenAgg = true;
                Function f = (Function) e;
                aggs.add(parseAggCall(f));
                continue;
            }

            if (seenAgg) {
                throw new IllegalArgumentException("Aggregate functions must appear at end of SELECT list");
            }

            if (!(e instanceof Column)) {
                throw new IllegalArgumentException("Non-aggregate SELECT items must be columns, got: " + e);
            }

            projectedCols.add(colRef((Column) e));
        }

        // 5) Validate GROUP BY rule (if GROUP BY exists, every non-agg projected col must be in GROUP BY)
        if (!groupByCols.isEmpty()) {
            Set<String> gb = new HashSet<String>(groupByCols);
            for (String ref : projectedCols) {
                if (!gb.contains(ref)) {
                    throw new IllegalArgumentException("Non-aggregate SELECT column must appear in GROUP BY: " + ref);
                }
            }
        }

        return new QueryAnalysis(false, projectedCols, groupByCols, aggs);
    }

    // ----------------- helpers -----------------

    private static boolean isStarSelect(List<SelectItem<?>> items) {
        if (items.size() != 1) return false;
        Expression e = items.get(0).getExpression();
        return (e instanceof AllColumns) || (e instanceof AllTableColumns);
    }

    private static boolean containsAnyAggregate(List<SelectItem<?>> items) {
        for (SelectItem<?> si : items) {
            if (isAggregateFunction(si.getExpression())) return true;
        }
        return false;
    }

    private static boolean isAggregateFunction(Expression e) {
        if (!(e instanceof Function)) return false;
        Function f = (Function) e;
        if (f.getName() == null) return false;
        String name = f.getName().trim().toLowerCase(Locale.ROOT);

        // Extendable: add "min", "max", "avg" later.
        return name.equals("sum") || name.equals("count");
    }

    private static QueryAnalysis.AggCall parseAggCall(Function f) {
        String name = f.getName().trim().toLowerCase(Locale.ROOT);

        if (name.equals("sum")) {
            Expression arg = extractSingleArg(f, "SUM");
            return new QueryAnalysis.AggCall("sum", arg);
        }

        if (name.equals("count")) {
            // COUNT(*) or COUNT(expr)
            if (f.getParameters() == null || f.getParameters().isEmpty()) {
                // Some parser forms represent COUNT(*) with no params - accept
                return new QueryAnalysis.AggCall("count", null);
            }
            if (f.getParameters().size() != 1) {
                throw new IllegalArgumentException("COUNT() must have exactly one argument");
            }
            Object only = f.getParameters().get(0);
            if (only instanceof AllColumns) {
                return new QueryAnalysis.AggCall("count", null);
            }
            if (!(only instanceof Expression)) {
                throw new IllegalArgumentException("COUNT() arg is not an Expression: " + only);
            }
            return new QueryAnalysis.AggCall("count", (Expression) only);
        }

        throw new IllegalArgumentException("Unsupported aggregate function: " + f.getName());
    }

    private static Expression extractSingleArg(Function f, String name) {
        if (f.getParameters() == null || f.getParameters().isEmpty()) {
            throw new IllegalArgumentException(name + "() must have one argument");
        }
        if (f.getParameters().size() != 1) {
            throw new IllegalArgumentException(name + "() must have exactly one argument");
        }
        Object only = f.getParameters().get(0);
        if (!(only instanceof Expression)) {
            throw new IllegalArgumentException(name + "() argument is not an Expression: " + only);
        }
        return (Expression) only;
    }

    private static List<String> parseGroupByCols(PlainSelect ps) {
        GroupByElement gbe = ps.getGroupBy();
        if (gbe == null || gbe.getGroupByExpressionList() == null || gbe.getGroupByExpressionList().isEmpty()) {
            return Collections.emptyList();
        }

        List<String> refs = new ArrayList<String>();
        for (Object o : gbe.getGroupByExpressionList()) {
            if (!(o instanceof Expression)) {
                throw new IllegalArgumentException("GROUP BY element is not an Expression: " + o);
            }
            Expression e = (Expression) o;
            if (!(e instanceof Column)) {
                throw new IllegalArgumentException("GROUP BY only supports columns, got: " + e);
            }
            refs.add(colRef((Column) e));
        }
        return refs;
    }

    private static String colRef(Column c) {
        String col = c.getColumnName();
        String table = (c.getTable() == null) ? null : c.getTable().getName();
        if (table != null && !table.trim().isEmpty()) {
            return norm(table + "." + col);
        }
        return norm(col);
    }

    private static String norm(String s) {
        return s.trim().toLowerCase(Locale.ROOT);
    }
}