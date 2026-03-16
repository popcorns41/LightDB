package ed.inf.adbs.lightdb.planner;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.*;

public class RequiredColumnsAnalyser {
    private RequiredColumnsAnalyser() {
        // Private constructor to prevent instantiation
    }

     /**
     * Returns, for each table name, the set of required column refs as unqualified column names.
     *
     * Table names are normalised to lowercase.
     * Column names are normalised to lowercase.
     *
     * Sources of required columns:
     *  - non-aggregate projected columns
     *  - GROUP BY columns
     *  - ORDER BY columns
     *  - WHERE predicates
     *  - aggregate arguments (e.g. SUM(Student.C), SUM(Student.A * Student.C))
     *
     * This analyser only records columns that are explicitly qualified with a table name.
     */

     public static Map<String, Set<String>> analyse(PlainSelect ps, QueryAnalysis qa){
        if (ps == null) throw new NullPointerException("ps");
        if (qa == null) throw new NullPointerException("qa");

        Map<String, Set<String>> requiredCols = new HashMap<>();

        // 1) projected (non-aggregate) columns
        for (String colRef : qa.projectedCols) {
            addRef(requiredCols, colRef);
        }

        // 2) GROUP BY columns
        for (String colRef : qa.groupByCols) {
            addRef(requiredCols, colRef);
        }

        //3) aggregate args
        for (QueryAnalysis.AggCall agg : qa.aggregates) {
            if (agg.arg != null) {
                addFromExpression(requiredCols, agg.arg);
            }
        }

        //4) WHERE clause
        if (ps.getWhere() != null) {
            addFromExpression(requiredCols, ps.getWhere());
        }

        //5) ORDER BY columns
        if (ps.getOrderByElements() != null) {
            for (OrderByElement obe : ps.getOrderByElements()) {
                if (obe.getExpression() != null) {
                    addFromExpression(requiredCols, obe.getExpression());
                }
            }
        }

        return requiredCols;
    }


    // Helper method to add a single column ref to requiredCols map. The input is expected to be a normalised ref (e.g. "Student.C" or "C"), 
    // and we only add it if it's qualified (i.e. contains a dot).
    private static void addRef(Map<String, Set<String>> requiredCols, String normalisedRef){
        if (normalisedRef == null) return;

        String ref = normalisedRef.trim().toLowerCase(java.util.Locale.ROOT);

        if (!ref.contains(".")) {
            // unqualified ref, ignore
            return;
        }

        String[] parts = ref.split("\\.",2);
        String tableName = parts[0];
        String columnName = parts[1];

        Set<String> cols = requiredCols.get(tableName);
        if (cols == null) {
            cols = new LinkedHashSet<>();
            requiredCols.put(tableName, cols);
        }
        cols.add(columnName);
    }

    // Helper method to recursively extract column refs from an expression and add them to requiredCols
    private static void addFromExpression(final Map<String, Set<String>> requiredCols, Expression expr){
        expr.accept(new ExpressionDeParser() {
            // We override visit(Column) to capture column refs in the expression.
            @Override
            public void visit(Column column){
                String table = (column.getTable() == null) ? null : column.getTable().getName();
                String colName = column.getColumnName();

                if (table == null || table.trim().isEmpty()) {
                    // unqualified ref, ignore
                    return;
                }

                table = table.trim().toLowerCase(java.util.Locale.ROOT);
                colName = colName.trim().toLowerCase(java.util.Locale.ROOT);

                Set<String> cols = requiredCols.get(table);
                if (cols == null) {
                    cols = new LinkedHashSet<>();
                    requiredCols.put(table, cols);
                }
                cols.add(colName);
            }

            // We need to override visit(Function) to descend into aggregate arguments and extract column refs from them.
            @Override
            public void visit(Function function) {
                //descend into aggregate args/arthmetic expressions inside aggregate args
                if (function.getParameters() != null){
                    for (Object o : function.getParameters()){
                        if (o instanceof Expression) {
                            ((Expression) o).accept(this);
                        }
                    }
                }
            }
        });
    }

}
