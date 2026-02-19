package ed.inf.adbs.lightdb.planner;

import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.*;
import ed.inf.adbs.lightdb.util.ExpressionUtils;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

public final class PlanBuilder {
    private PlanBuilder() {}

    public static Operator build(PlainSelect ps) {
        // 1) Tables in FROM order
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

            List<Expression> singles = wc.extractSingleTable(name);
            Expression singleWhere = ExpressionUtils.andAll(singles);
            if (singleWhere != null) {
                op = new SelectOperator(op, singleWhere, meta);
            }

            base.put(name, new Plan(op, Collections.singletonList(meta)));
        }

        // 4) Left-deep join tree in FROM order
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

        // 5) Apply leftover predicates at top (safety net)
        List<Expression> leftover = wc.getRemaining();
        if (!leftover.isEmpty()) {
            root = new FilterOperator(root, ExpressionUtils.andAll(leftover), acc.tables);
        }

        // 6) Projection last (only if not SELECT *)
        root = maybeWrapProjection(ps.getSelectItems(), root, acc.tables);

        return root;
    }

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

    private static Operator maybeWrapProjection(List<SelectItem<?>> selectItems, Operator child, List<TableMeta> tablesInOrder) {
        boolean isStar =
                selectItems.size() == 1 &&
                (selectItems.get(0).getExpression() instanceof AllColumns
                 || selectItems.get(0).getExpression() instanceof AllTableColumns);

        if (isStar) return child;

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

    private static final class Plan {
        final Operator op;
        final List<TableMeta> tables;
        Plan(Operator op, List<TableMeta> tables) {
            this.op = op;
            this.tables = tables;
        }
    }
}
