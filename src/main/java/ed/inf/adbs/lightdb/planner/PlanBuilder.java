package ed.inf.adbs.lightdb.planner;

import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.*;
import ed.inf.adbs.lightdb.util.ExpressionUtils;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * Builds an operator tree for a single SELECT statement.
 *
 * Uses a PlanContext that tracks:
 *  - root operator
 *  - base table schema order (for resolving Column refs pre-projection)
 *  - current output refs (post-projection / post-aggregation) for ORDER BY after projection
 *
 * Operator order (bottom -> top):
 *   Scan/Select/Join/Filter
 *   (GroupBy+Sum)  optional
 *   (Project)      optional (not created for SELECT *)
 *   (Distinct)     optional
 *   (Sort)         optional (ORDER BY after projection per coursework note)
 */

public final class PlanBuilder {
    private PlanBuilder() {}

    public static Operator build(PlainSelect ps){
        PlanContext ctx = buildBase(ps);
        ctx = applyGroupBySumIfPresent(ps,ctx);
        ctx = applyProjectionIfNeeded(ps,ctx);
        ctx = applyDistinct(ps,ctx);
        ctx = applyOrderBy(ps,ctx);
        return ctx.root;
    }

    // ===================== Stage 1: base plan =====================

    private static PlanContext buildBase(PlainSelect ps){
        // 1) Extract tables in FROM order
        List<Table> fromTables = extractFromTables(ps);
        if (fromTables.isEmpty()) throw new IllegalArgumentException("FROM clause is required.");

        // 2) Classify WHERE conjuncts
        WhereClassifier wc = new WhereClassifier(ps.getWhere());

        // 3) Build base plans: Scan with pushed-down single-table selection
        Map<String, Plan> base = new HashMap<String, Plan>();

        for (Table t : fromTables){
            String name = t.getName();

            TableMeta meta = Catalog.getInstance().getTable(name).orElseThrow(
                () -> new IllegalArgumentException("Table not found in catalog: " + name));
            
            Operator op = new ScanOperator(name);
            
            //push down conjuncts that reference only this table (or none)
            List<Expression> singles = wc.extractSingleTable(name);
            Expression singleWhere = ExpressionUtils.andAll(singles);
            if (singleWhere != null){
                op = new SelectOperator(op, singleWhere, meta);
            }

            base.put(name, new Plan(op, Collections.singletonList(meta)));
        }

        // 4) Build left-deep join tree in FROM order
        Plan acc = base.get(fromTables.get(0).getName());

        for (int i = 1; i < fromTables.size(); i++){
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

        //5) leftover predicates at the top
        List<Expression> leftover = wc.getRemaining();
        if (!leftover.isEmpty()){
            root = new FilterOperator(root, ExpressionUtils.andAll(leftover), acc.tables);
        }

        //baseTables schema is always acc.tables at this point
        return new PlanContext(root, acc.tables, null);
    }

    // ===================== Stage 2: GROUP BY + SUM =====================

    private static PlanContext applyGroupBySumIfPresent(PlainSelect ps, PlanContext ctx){
        //Detect aggregation query: any SUM(...) in SELECT or GROUP BY clause present
        AggSpec agg = parseAggSpec(ps);

        if (!agg.isAggQuery){
            return ctx; //no aggregation
        }

        ctx.root = new SumOperator(ctx.root, agg.groupByRefs, agg.outputGroupRefs, agg.sumArgs, ctx.baseTables);
        
        //Output refs: group keys + "sum(<arg>)" labels (for ORDER BY resolution)
        List<String> outRefs = new ArrayList<String>(agg.outputGroupRefs.size() + agg.sumArgs.size());
        outRefs.addAll(agg.outputGroupRefs);
        for (Expression arg : agg.sumArgs) outRefs.add(norm("sum(" + arg.toString() + ")"));
        
        ctx.outputRefs = outRefs;

        return ctx;
    }

    private static final class AggSpec {
        final boolean isAggQuery;
        final List<String> groupByRefs;     // grouping keys (from GROUP BY or derived)
        final List<String> outputGroupRefs; // non-agg SELECT cols (printed)
        final List<Expression> sumArgs;     // SUM argument expressions
        AggSpec(boolean isAggQuery, List<String> groupByRefs,  List<String> outputGroupRefs, List<Expression> sumArgs) {
            this.isAggQuery = isAggQuery;
            this.groupByRefs = groupByRefs;
            this.outputGroupRefs = outputGroupRefs;
            this.sumArgs = sumArgs;
        }
    }

    private static AggSpec parseAggSpec(PlainSelect ps) {
        List<SelectItem<?>> items = ps.getSelectItems();
        GroupByElement gbe = ps.getGroupBy();

        // Non-agg SELECT cols (must appear before SUMs)
        List<String> leadingCols = new ArrayList<String>();
        List<Expression> sumArgs = new ArrayList<Expression>();

        boolean seenSum = false;

        for (SelectItem<?> si : items) {
            Expression e = si.getExpression();

            if (isSumFunction(e)) {
                seenSum = true;
                sumArgs.add(extractSingleSumArg((Function) e));
                continue;
            }

            if (seenSum) {
                throw new IllegalArgumentException("SUM items must appear at the end of the SELECT list (coursework assumption).");
            }

            if (!(e instanceof net.sf.jsqlparser.schema.Column)) {
                throw new IllegalArgumentException("Non-aggregate SELECT items must be columns, got: " + e);
            }

            net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) e;
            leadingCols.add(colRef(c));
        }

        // Group-by refs from GROUP BY clause (if present)
        List<String> groupByRefsClause = parseGroupByRefs(ps);

        boolean isAggQuery = !sumArgs.isEmpty() || (gbe != null);
        if (!isAggQuery) {
            return new AggSpec(false,
                    Collections.<String>emptyList(),
                    Collections.<String>emptyList(),
                    Collections.<Expression>emptyList());
        }

        // Grouping keys: GROUP BY clause wins; else derive from non-agg SELECT cols.
        List<String> groupingRefs = groupByRefsClause.isEmpty() ? leadingCols : groupByRefsClause;

        // Output group refs are ONLY the non-agg SELECT cols (may be empty!)
        List<String> outputGroupRefs = leadingCols;

        // Validity check (SQL rule): if GROUP BY clause exists, every non-agg select col must be in GROUP BY
        if (!groupByRefsClause.isEmpty()) {
            Set<String> gb = new HashSet<String>(groupByRefsClause);
            for (String ref : leadingCols) {
                if (!gb.contains(ref)) {
                    throw new IllegalArgumentException(
                            "Non-aggregate SELECT column must appear in GROUP BY: " + ref + ", GROUP BY=" + groupByRefsClause
                    );
                }
            }
        }

        return new AggSpec(true, groupingRefs, outputGroupRefs, sumArgs);
    }

     private static boolean isSumFunction(Expression e) {
        if (!(e instanceof Function)) return false;
        Function f = (Function) e;
        return f.getName() != null && f.getName().equalsIgnoreCase("SUM");
    }

    //extract and validate SUM() argument expression, which must be a single expression
    private static Expression extractSingleSumArg(Function f) {
        if (f.getParameters() == null || f.getParameters().isEmpty()) {
            throw new IllegalArgumentException("SUM() must have one argument");
        }
        if (f.getParameters().size() != 1) {
            throw new IllegalArgumentException("SUM() must have exactly one argument");
        }

        Object only = f.getParameters().get(0);
        if (!(only instanceof Expression)) {
            throw new IllegalArgumentException("SUM() argument is not an Expression: " + only);
        }
        return (Expression) only;
    }

    // parse GROUP BY refs, which must be columns, and normalise to "table.column" if table present, 
    // otherwise just "column", and trim+lowercase for case-insensitive matching
    private static List<String> parseGroupByRefs(PlainSelect ps) {
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

            if (!(e instanceof net.sf.jsqlparser.schema.Column)) {
                throw new IllegalArgumentException("GROUP BY only supports columns, got: " + e);
            }

            refs.add(colRef((net.sf.jsqlparser.schema.Column) e));
        }

        return refs;
    }

    // ===================== Stage 3: Projection =====================

    private static PlanContext applyProjectionIfNeeded(PlainSelect ps, PlanContext ctx){
        //If aggregation already set output refs, do NOT apply ProjectOperator.
        if (ctx.outputRefs != null){
            return ctx;
        }

        List<SelectItem<?>> selectItems = ps.getSelectItems();
        boolean isStar = 
                selectItems.size() == 1 &&
                (selectItems.get(0).getExpression() instanceof AllColumns
                || selectItems.get(0).getExpression() instanceof AllTableColumns);

        if (isStar){
            ctx.outputRefs = starOutputRefs(ctx.baseTables);
            return ctx; //SELECT * : no ProjectOperator
        }

        ProjectOperator proj = buildProjectOperator(selectItems, ctx.root, ctx.baseTables);
        ctx.root = proj;
        ctx.outputRefs = proj.getOutputRefs();
        return ctx;
    }

    // ===================== Stage 4: DISTINCT =====================
    
    private static PlanContext applyDistinct(PlainSelect ps, PlanContext ctx){
        if (ps.getDistinct() != null){
            ctx.root = new DuplicateEliminationOperator(ctx.root);
        }
        return ctx;
    }

    // ===================== Stage 5: ORDER BY (after projection) =====================

    private static PlanContext applyOrderBy(PlainSelect ps, PlanContext ctx){
        OrderSpec order = parseOrderBy(ps);

        if (order != null) {
            if (ctx.outputRefs == null){
                throw new IllegalStateException("Internal error: outputRefs not set before ORDER BY");
            }
            ctx.root = new SortOperator(ctx.root, order.cols, order.asc, ctx.outputRefs);
        }

        return ctx;
    }

    // ===================== helpers =====================

    private static List<Table> extractFromTables(PlainSelect ps){
        List<Table> out = new ArrayList<Table>();

        FromItem fi = ps.getFromItem();
        if (fi == null) return out;
        if (!(fi instanceof Table)){
            throw new IllegalArgumentException("Only base tables supported in FROM.");
        }
        out.add((Table) fi);

        if (ps.getJoins() != null){
            for (Join j: ps.getJoins()) {
                if (!(j.getRightItem() instanceof Table)) {
                    throw new IllegalArgumentException("Only base tables supported in FROM.");
                }
                out.add((Table) j.getRightItem());
            }
        }
        return out;
    } 

    private static ProjectOperator buildProjectOperator(List<SelectItem<?>> selectItems, Operator child, List<TableMeta> tablesInOrder){
        List<String> cols = new ArrayList<String>(selectItems.size());

        for (SelectItem<?> item : selectItems){
            Expression e = item.getExpression();

            if (!(e instanceof Column)){
                throw new IllegalArgumentException("Only column projections supported, got: " + e); 
            }

            Column c = (Column) e;
            cols.add(colRef(c));
        }

        return new ProjectOperator(child,cols,tablesInOrder);
    }
    // normalise column reference to "table.column" if table is present, otherwise just "column", and trim+lowercase for case-insensitive matching
    private static String colRef(Column c){
        if (c.getTable() != null && c.getTable().getName() != null){
            return norm(c.getTable().getName() + "." + c.getColumnName());
        }
        return norm(c.getColumnName());
    }

    private static List<String> starOutputRefs(List<TableMeta> tablesInOrder){
        List<String> refs = new ArrayList<String>();
        for (TableMeta tm : tablesInOrder){
            for (int i = 0; i < tm.getColumns().size(); i++){
                refs.add(norm(tm.getName() + "." + tm.getColumns().get(i).getName()));
            }
        }
        return refs;
    }

    private static final class OrderSpec {
        final List<String> cols;
        final List<Boolean> asc;
        OrderSpec(List<String> cols, List<Boolean> asc){
            this.cols = cols;
            this.asc = asc;
        }
    }

    private static OrderSpec parseOrderBy(PlainSelect ps){
        List<OrderByElement> elems = ps.getOrderByElements();
        if (elems == null || elems.isEmpty()) return null;

        List<String> cols = new ArrayList<String>(elems.size());
        List<Boolean> asc = new ArrayList<Boolean>(elems.size());

        for (OrderByElement e : elems){
            //support ordering by columns AND by SUM(...) label
            Expression ex = e.getExpression();

            if (ex instanceof Column){
                cols.add(colRef((Column) ex));
            }else if (ex instanceof Function && ((Function) ex).getName() != null 
            && ((Function) ex).getName().equalsIgnoreCase("SUM")){
                Expression arg = extractSingleSumArg((Function) ex);
                cols.add(norm("sum(" + arg.toString() + ")"));
            }else {
                throw new IllegalArgumentException("ORDER BY only supports columns (and SUM(...)), got: " + ex);
            }

            asc.add(Boolean.valueOf(e.isAsc()));
        }

        return new OrderSpec(cols,asc);
    }

    private static String norm(String s) {
        return s.trim().toLowerCase(Locale.ROOT);
    }

    // ===================== Structure classes =====================
    private static final class PlanContext {
        Operator root;
        final List<TableMeta> baseTables;
        List<String> outputRefs; // schema of current output tuple (for ORDER BY after projection)

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