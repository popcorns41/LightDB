package ed.inf.adbs.lightdb.operator;


import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.catalog.ColumnMeta;
import ed.inf.adbs.lightdb.util.ColumnIndexResolver;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator extends Operator {

    private final Operator child;
    private final int[] projectionIndexes;

    public ProjectOperator(Operator child, List<String> selectedColumns, TableMeta tableMeta){
        if (child == null) throw new NullPointerException("child");
        if (selectedColumns == null) throw new NullPointerException("selectedColumns");
        if (tableMeta == null) throw new NullPointerException("tableMeta");

        this.child = child;

        List<String> columnNames = new ArrayList<String>();
        for (ColumnMeta c : tableMeta.getColumns()) {
            columnNames.add(c.getName());
        }

        ColumnIndexResolver resolver =
                new ColumnIndexResolver(tableMeta.getName(), columnNames);

        this.projectionIndexes = new int[selectedColumns.size()];
        for (int i = 0; i < selectedColumns.size(); i++) {
String col = selectedColumns.get(i);
            // allow both "A" and "Student.A"
            String table = null;
            String column = col;

            if (col.contains(".")) {
                String[] parts = col.split("\\.");
                table = parts[0];
                column = parts[1];
            }

            projectionIndexes[i] = resolver.indexOf(table, column);
        }
    }

    @Override
    public Tuple getNextTuple(){
        Tuple t = child.getNextTuple();
        if (t == null) return null;

        List<String> projectedValues = new ArrayList<String>(projectionIndexes.length);
        for (int idx : projectionIndexes) {
            projectedValues.add(t.get(idx));
        }

        return new Tuple(projectedValues);
    }

    @Override
    public void reset() {
        child.reset();
    }
}
