package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.util.MultiTableColumnIndexResolver;

import java.util.ArrayList;
import java.util.List;

public final class ProjectOperator extends Operator {

    private final Operator child;
    private final int[] projectionIndexes;

    public ProjectOperator(Operator child, List<String> selectedColumns, List<TableMeta> tablesInOrder) {
        if (child == null) throw new NullPointerException("child");
        if (selectedColumns == null) throw new NullPointerException("selectedColumns");
        if (tablesInOrder == null) throw new NullPointerException("tablesInOrder");

        this.child = child;

        MultiTableColumnIndexResolver resolver = new MultiTableColumnIndexResolver(tablesInOrder);

        this.projectionIndexes = new int[selectedColumns.size()];
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
        }
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
}
