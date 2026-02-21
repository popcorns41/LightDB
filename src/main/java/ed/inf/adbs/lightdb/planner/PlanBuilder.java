package ed.inf.adbs.lightdb.planner;

import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.*;
import ed.inf.adbs.lightdb.util.ExpressionUtils;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

/**
 * PlanBuilder is the main class responsible for constructing a query execution plan (a tree of Operator instances) from a parsed SQL query (represented as a PlainSelect).
 * It performs the following stages:
 * 1) Base plan construction: builds a plan with Scan, Select, Join, and Filter operators based on the FROM and WHERE clauses.
 * 2) Aggregation: if there are aggregate functions or GROUP BY, it adds a SumOperator to perform the aggregation.
 * 3) Projection: if there is a SELECT list that is not just *, it adds a ProjectOperator to produce the desired output columns.
 * 4) DISTINCT: if SELECT DISTINCT is specified, it adds a DuplicateEliminationOperator.
 * 5) ORDER BY: if there is an ORDER BY clause, it adds a SortOperator at the end of the plan.
 *
 * The PlanBuilder relies on QueryAnalyser to analyze the structure of the query and determine what features are present (e.g., aggregates, grouping keys, projected columns), which informs how the plan is constructed.
 */

public final class PlanBuilder {
    private PlanBuilder() {}

    public static Operator build(PlainSelect ps) {
        PlanContext ctx = buildBase(ps);                 // Scan/Select/Join/Filter
        QueryAnalysis qa = QueryAnalyser.analyze(ps);    // single analysis pass

        ctx = applyAggregationIfPresent(ctx, qa);
        ctx = applyProjectionIfNeeded(ctx, qa);
        ctx = applyDistinct(ps, ctx);
        ctx = applyOrderBy(ps, ctx);

        return ctx.root;
    }

    // ===================== Stage 1: base plan =====================

    private static PlanContext buildBase(PlainSelect ps) {
        List<Table> fromTables = extractFromTables(ps);
        if (fromTables.isEmpty()) throw new IllegalArgumentException("FROM clause is required.");

        WhereClassifier wc = new WhereClassifier(ps.getWhere());

        Map<String, Plan> base = new HashMap<String, Plan>();

        for (Table t : fromTables) {
            String name = t.getName();

            TableMeta meta = Catalog.getInstance()
                    .getTable(name)
                    .orElseThrow(() -> new IllegalArgumentException("Table not found in catalog: " + name));

            Operator op = new ScanOperator(name);

            List<Expression> singles = wc.extractSingleTable(name);
            Expression singleWhere = ExpressionUtils.andAll(singles);
            if (singleWhere != null) {
                op = new SelectOperator(op, singleWhere, meta);
            }

            base.put(name, new Plan(op, Collections.singletonList(meta)));
        }

        Plan acc = base.get(fromTables.get(0).getName());

        for (int i = 1; i < fromTables.size(); i++) {
            String rightName = fromTables.get(i).getName();
            Plan right = base.get(rightName);

            Set<String> leftNames = new HashSet<String>();
            for (TableMeta tm : acc.tables) leftNames.add(tm.getName());

            List<Expression> joinConds = wc.extractJoinPredicates(leftNames, rightName);
            Expression joinExpr = ExpressionUtils.andAll(joinConds);

            Operator joined = new JoinOperator(acc.op, right.op, joinExpr, acc.tables, right.tables);

            List<TableMeta> merged = new ArrayList<TableMeta>(acc.tables.size() + right.tables.size());
            merged.addAll(acc.tables);
            merged.addAll(right.tables);

            acc = new Plan(joined, merged);
        }

        Operator root = acc.op;

        List<Expression> leftover = wc.getRemaining();
        if (!leftover.isEmpty()) {
            root = new FilterOperator(root, ExpressionUtils.andAll(leftover), acc.tables);
        }

        return new PlanContext(root, acc.tables, null);
    }

    // ===================== Stage 2: Aggregation =====================

    private static PlanContext applyAggregationIfPresent(PlanContext ctx, QueryAnalysis qa) {
        if (!qa.hasAggregation()) return ctx;


        // We enforce that all aggregates are SUM until implemention of additional functions (COUNT/etc).
        for (QueryAnalysis.AggCall a : qa.aggregates) {
            if (!a.funcName.equals("sum")) {
                throw new IllegalArgumentException("Aggregate not supported yet: " + a.funcName);
            }
        }

        // Grouping keys are from GROUP BY clause; if none, derive from projected columns
        List<String> groupingRefs = qa.groupByCols.isEmpty() ? qa.projectedCols : qa.groupByCols;

        // Output group refs are non-agg projected cols (may be empty!)
        List<String> outputGroupRefs = qa.projectedCols;

        // Extract SUM args
        List<Expression> sumArgs = new ArrayList<Expression>(qa.aggregates.size());
        for (QueryAnalysis.AggCall a : qa.aggregates) sumArgs.add(a.arg);

        ctx.root = new SumOperator(ctx.root, groupingRefs, outputGroupRefs, sumArgs, ctx.baseTables);

        // Schema after aggregation: outputGroupRefs + sum(...)
        ctx.outputRefs = qa.expectedOutputRefsForAgg();

        return ctx;
    }

    // ===================== Stage 3: Projection =====================

    private static PlanContext applyProjectionIfNeeded(PlanContext ctx, QueryAnalysis qa) {
        // If aggregation happened, output is already defined.
        if (ctx.outputRefs != null) return ctx;

        if (qa.isStar) {
            ctx.outputRefs = starOutputRefs(ctx.baseTables);
            return ctx;
        }

        ProjectOperator proj = new ProjectOperator(ctx.root, qa.projectedCols, ctx.baseTables);
        ctx.root = proj;
        ctx.outputRefs = proj.getOutputRefs();
        return ctx;
    }

    // ===================== Stage 4: DISTINCT =====================

    private static PlanContext applyDistinct(PlainSelect ps, PlanContext ctx) {
        if (ps.getDistinct() != null) {
            ctx.root = new DuplicateEliminationOperator(ctx.root);
        }
        return ctx;
    }

    // ===================== Stage 5: ORDER BY (after projection/agg) =====================

    private static PlanContext applyOrderBy(PlainSelect ps, PlanContext ctx) {
        OrderSpec order = parseOrderBy(ps);
        if (order != null) {
            if (ctx.outputRefs == null) throw new IllegalStateException("Internal: outputRefs not set before ORDER BY");
            ctx.root = new SortOperator(ctx.root, order.cols, order.asc, ctx.outputRefs);
        }
        return ctx;
    }

    // ===================== helpers =====================

    private static List<Table> extractFromTables(PlainSelect ps) {
        List<Table> out = new ArrayList<Table>();

        FromItem fi = ps.getFromItem();
        if (fi == null) return out;
        if (!(fi instanceof Table)) throw new IllegalArgumentException("Only base tables supported in FROM.");
        out.add((Table) fi);

        if (ps.getJoins() != null) {
            for (Join j : ps.getJoins()) {
                if (!(j.getRightItem() instanceof Table)) {
                    throw new IllegalArgumentException("Only base tables supported in FROM.");
                }
                out.add((Table) j.getRightItem());
            }
        }
        return out;
    }

    private static List<String> starOutputRefs(List<TableMeta> tablesInOrder) {
        List<String> refs = new ArrayList<String>();
        for (TableMeta tm : tablesInOrder) {
            for (int i = 0; i < tm.getColumns().size(); i++) {
                refs.add(norm(tm.getName() + "." + tm.getColumns().get(i).getName()));
            }
        }
        return refs;
    }

    private static final class OrderSpec {
        final List<String> cols;
        final List<Boolean> asc;
        OrderSpec(List<String> cols, List<Boolean> asc) { this.cols = cols; this.asc = asc; }
    }

    private static OrderSpec parseOrderBy(PlainSelect ps) {
        List<OrderByElement> elems = ps.getOrderByElements();
        if (elems == null || elems.isEmpty()) return null;

        List<String> cols = new ArrayList<String>(elems.size());
        List<Boolean> asc = new ArrayList<Boolean>(elems.size());

        for (OrderByElement e : elems) {
            Expression ex = e.getExpression();

            if (ex instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) ex;
                String table = (c.getTable() == null) ? null : c.getTable().getName();
                String col = c.getColumnName();
                cols.add(table == null ? norm(col) : norm(table + "." + col));
            } else if (ex instanceof Function) {
                // ORDER BY SUM(...) etc: use normalised label matching QueryAnalysis
                Function f = (Function) ex;
                String name = (f.getName() == null) ? "" : f.getName().trim().toLowerCase(Locale.ROOT);
                if (!name.equals("sum")) {
                    throw new IllegalArgumentException("ORDER BY aggregate not supported: " + ex);
                }
                // label: "sum(<arg>)"
                Expression arg = (Expression) f.getParameters().get(0);
                cols.add(norm("sum(" + arg.toString() + ")"));
            } else {
                throw new IllegalArgumentException("ORDER BY only supports columns (and SUM(...)), got: " + ex);
            }

            asc.add(Boolean.valueOf(e.isAsc()));
        }

        return new OrderSpec(cols, asc);
    }

    private static String norm(String s) {
        return s.trim().toLowerCase(Locale.ROOT);
    }

    // ===================== tiny structure classes =====================

    private static final class PlanContext {
        Operator root;
        final List<TableMeta> baseTables;
        List<String> outputRefs;

        PlanContext(Operator root, List<TableMeta> baseTables, List<String> outputRefs) {
            this.root = root;
            this.baseTables = baseTables;
            this.outputRefs = outputRefs;
        }
    }

    private static final class Plan {
        final Operator op;
        final List<TableMeta> tables;
        Plan(Operator op, List<TableMeta> tables) {
            this.op = op;
            this.tables = tables;
        }
    }
}