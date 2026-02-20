package ed.inf.adbs.lightdb.util;

import ed.inf.adbs.lightdb.catalog.ColumnMeta;
import ed.inf.adbs.lightdb.catalog.TableMeta;

import java.util.*;

/**
 * Resolves column references to tuple indices for a single operator that produces tuples containing fields from multiple tables.
 * This is used for operators like Join that combine fields from multiple tables into a single tuple. 
 * It supports both qualified (e.g. "Course.cid") and unqualified (e.g. "cid") column references, and handles ambiguity 
 * in unqualified references by throwing an exception.
 * 
 * @PARAM tablesInTupleOrder the list of tables whose fields are combined in the output tuples, in the order they appear in the tuple (e.g. [Course, Student] for a join of Course and Student)
 */

public final class MultiTableColumnIndexResolver implements ColumnResolver {
    private final Map<String, Integer> indexByQualified = new HashMap<String, Integer>();
    private final Map<String, List<Integer>> indexesByUnqualified = new HashMap<String, List<Integer>>();

    public MultiTableColumnIndexResolver(List<TableMeta> tablesInTupleOrder) {
        int offset = 0;
        for (TableMeta t : tablesInTupleOrder) {
            String table = norm(t.getName());
            for (int i = 0; i < t.getColumns().size(); i++) {
                ColumnMeta c = t.getColumns().get(i);
                String col = norm(c.getName());
                int idx = offset + i;

                indexByQualified.put(table + "." + col, idx);

                List<Integer> bucket = indexesByUnqualified.get(col);
                if (bucket == null) {
                    bucket = new ArrayList<Integer>();
                    indexesByUnqualified.put(col, bucket);
                }
                bucket.add(idx);
            }
            offset += t.getColumns().size();
        }
    }

    // This method resolves a column reference to its index in the output tuple. 
    // It first checks for a qualified reference (table.column), and if not found, 
    // it checks for an unqualified reference (column). 
    // If the unqualified reference is ambiguous (i.e. appears in multiple tables), it throws an exception.
    @Override
    public int indexOf(String maybeTable, String column) {
        if (maybeTable != null && !maybeTable.trim().isEmpty()) {
            String key = norm(maybeTable) + "." + norm(column);
            Integer idx = indexByQualified.get(key);
            if (idx == null) throw new IllegalArgumentException("Unknown column: " + key);
            return idx.intValue();
        }

        String col = norm(column);
        List<Integer> hits = indexesByUnqualified.get(col);
        if (hits == null || hits.isEmpty()) throw new IllegalArgumentException("Unknown column: " + col);
        if (hits.size() > 1) throw new IllegalArgumentException("Ambiguous column: " + col);
        return hits.get(0).intValue();
    }

    private static String norm(String s) {
        return s.trim().toLowerCase(Locale.ROOT);
    }
}
