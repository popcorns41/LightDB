package ed.inf.adbs.lightdb.planner;

import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.*;
import ed.inf.adbs.lightdb.util.ExpressionUtils;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

/**
 * Builds an operator tree for a single SELECT statement.
 *
 * Operator order (bottom -> top):
 *   Scan (always)
 *   Select (optional, pushed down per-table + leftover filter)
 *   Join  (optional, left-deep, FROM order)
 *   Project (optional; not created for SELECT *)
 *   Sort    (optional; ORDER BY)
 *
 * NOTE (coursework simplification):
 *   Because we sort after projection, ORDER BY columns must be present in the projected output.
 *   If an ORDER BY column is not available after projection, we throw an error.
 */
public final class PlanBuilder {
    private PlanBuilder() {}

    public static Operator build(PlainSelect ps) {
        // 1) Extract tables in FROM order
        List<Table> fromTables = extractFromTables(ps);
        if (fromTables.isEmpty()) throw new IllegalArgumentException("FROM clause is required.");

        // 2) Classify WHERE conjuncts
        WhereClassifier wc = new WhereClassifier(ps.getWhere());

        // 3) Build base plans: Scan with pushed-down single-table selection
        Map<String, Plan> base = new HashMap<String, Plan>();

        for (Table t : fromTables) {
            String name = t.getName();

            TableMeta meta = Catalog.getInstance()
                    .getTable(name)
                    .orElseThrow(() -> new IllegalArgumentException("Table not found in catalog: " + name));

            Operator op = new ScanOperator(name);

            // push down conjuncts that reference only this table (or none)
            List<Expression> singles = wc.extractSingleTable(name);
            Expression singleWhere = ExpressionUtils.andAll(singles);
            if (singleWhere != null) {
                op = new SelectOperator(op, singleWhere, meta);
            }

            base.put(name, new Plan(op, Collections.singletonList(meta)));
        }

        // 4) Build left-deep join tree in FROM order, attaching join predicates when they become applicable
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

        // 5) Any leftover predicates apply at the top (rare but keeps you correct)
        List<Expression> leftover = wc.getRemaining();
        if (!leftover.isEmpty()) {
            root = new FilterOperator(root, ExpressionUtils.andAll(leftover), acc.tables);
        }

        // 6) Projection
        List<SelectItem<?>> selectItems = ps.getSelectItems();
        boolean isStar =
                selectItems.size() == 1 &&
                (selectItems.get(0).getExpression() instanceof AllColumns
                 || selectItems.get(0).getExpression() instanceof AllTableColumns);

        List<String> outputRefsInOrder;

        if (isStar) {
            // No ProjectOperator for SELECT *
            // But we still need output schema for SortOperator if ORDER BY exists.
            outputRefsInOrder = starOutputRefs(acc.tables);
        } else {
            ProjectOperator proj = buildProjectOperator(selectItems, root, acc.tables);
            root = proj;
            outputRefsInOrder = proj.getOutputRefs();
        }

        // 7) ORDER BY AFTER projection
        OrderSpec order = parseOrderBy(ps);
        if (order != null) {
            root = new SortOperator(root, order.cols, order.asc, outputRefsInOrder);
        }

        return root;
    }

    // ---------------- helpers ----------------

    private static List<Table> extractFromTables(PlainSelect ps) {
        List<Table> out = new ArrayList<Table>();

        FromItem fi = ps.getFromItem();
        if (fi == null) return out;
        if (!(fi instanceof Table)) {
            throw new IllegalArgumentException("Only base tables supported in FROM.");
        }
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

    private static ProjectOperator buildProjectOperator(List<SelectItem<?>> selectItems,
                                                        Operator child,
                                                        List<TableMeta> tablesInOrder) {
        List<String> cols = new ArrayList<String>(selectItems.size());

        for (SelectItem<?> item : selectItems) {
            Expression e = item.getExpression();

            if (!(e instanceof net.sf.jsqlparser.schema.Column)) {
                throw new IllegalArgumentException("Only column projections supported, got: " + e);
            }

            net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) e;

            if (c.getTable() != null && c.getTable().getName() != null) {
                cols.add(c.getTable().getName() + "." + c.getColumnName());
            } else {
                cols.add(c.getColumnName());
            }
        }

        return new ProjectOperator(child, cols, tablesInOrder);
    }

    private static List<String> starOutputRefs(List<TableMeta> tablesInOrder) {
        List<String> refs = new ArrayList<String>();
        for (TableMeta tm : tablesInOrder) {
            for (int i = 0; i < tm.getColumns().size(); i++) {
                refs.add(tm.getName() + "." + tm.getColumns().get(i).getName());
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
            if (!(e.getExpression() instanceof net.sf.jsqlparser.schema.Column)) {
                throw new IllegalArgumentException("ORDER BY only supports columns, got: " + e.getExpression());
            }

            net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) e.getExpression();

            if (c.getTable() != null && c.getTable().getName() != null) {
                cols.add(c.getTable().getName() + "." + c.getColumnName());
            } else {
                cols.add(c.getColumnName());
            }

            asc.add(Boolean.valueOf(e.isAsc()));
        }

        return new OrderSpec(cols, asc);
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