package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.util.MultiTableColumnIndexResolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * ProjectOperator is a unary operator that performs projection on the tuples produced by its child operator. 
 * It takes a list of selected column references and a list of table metadata to resolve those references to column indices.
 * When getNextTuple() is called, it retrieves the next tuple from the child operator, extracts the values corresponding
 * to the selected columns based on the resolved indices, and returns a new tuple containing only those projected values. 
 * The operator also supports resetting, which resets the child operator to allow for re-scanning.
 * 
 * @PARAM child The child operator from which to retrieve tuples for projection.
 * @PARAM selectedColumns A list of column references (e.g., "table.column" or just "column") that specify which columns to project.
 * @PARAM tablesInOrder A list of TableMeta objects representing the tables in the order they appear in the operator tree, used for resolving column references.
 */

public final class ProjectOperator extends Operator {

    private final Operator child;
    private final int[] projectionIndexes;

    // the projected schema in tuple order, e.g. ["Student.D","Student.B","Student.A"]
    private final List<String> outputRefs;

    public ProjectOperator(Operator child, List<String> selectedColumns, List<TableMeta> tablesInOrder) {
        if (child == null) throw new NullPointerException("child");
        if (selectedColumns == null) throw new NullPointerException("selectedColumns");
        if (tablesInOrder == null) throw new NullPointerException("tablesInOrder");

        this.child = child;

        MultiTableColumnIndexResolver resolver = new MultiTableColumnIndexResolver(tablesInOrder);

        this.projectionIndexes = new int[selectedColumns.size()];
        List<String> refs = new ArrayList<String>(selectedColumns.size());

        for (int i = 0; i < selectedColumns.size(); i++) {
            String ref = selectedColumns.get(i);

            String table = null;
            String col = ref;

            if (ref.contains(".")) {
                String[] parts = ref.split("\\.");
                table = parts[0];
                col = parts[1];
            }

            projectionIndexes[i] = resolver.indexOf(table, col);

            // store a normalized reference for later ORDER BY resolution
            if (table != null && !table.trim().isEmpty()) {
                refs.add(norm(table) + "." + norm(col));
            } else {
                refs.add(norm(col));
            }
        }

        this.outputRefs = Collections.unmodifiableList(refs);
    }

    @Override
    public Tuple getNextTuple() {
        Tuple t = child.getNextTuple();
        if (t == null) return null;

        List<String> out = new ArrayList<String>(projectionIndexes.length);
        for (int idx : projectionIndexes) out.add(t.get(idx));

        return new Tuple(out);
    }

    @Override
    public void reset() {
        child.reset();
    }

    /** Schema of this operator's output tuple, in order. */
    public List<String> getOutputRefs() {
        return outputRefs;
    }

    private static String norm(String s) {
        return s.trim().toLowerCase(java.util.Locale.ROOT);
    }
}