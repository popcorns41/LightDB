package ed.inf.adbs.lightdb.util;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ColumnIndexResolver implements ColumnResolver {
    private final Map<String,Integer> indexByRef = new HashMap<String,Integer>();

    /**
     * Build a resolver for a single table scan.
     * 
     * @param tableName
     * @param columns column names in tuple order
     */

    public ColumnIndexResolver(String tableName, List<String> columns){
        if (tableName == null) throw new NullPointerException("tableName cannot be null");
        if (columns == null) throw new NullPointerException("columns cannot be null");

        String t = norm(tableName);

        for (int i = 0; i < columns.size(); i++) {
            String col = norm(columns.get(i));

            indexByRef.put(t+"."+col,i);
            indexByRef.put(col,i);
        }
    }

    public int indexOf(String tableName, String columnName){
        String key;
        if (tableName == null || tableName.trim().isEmpty()) {
            key = norm(columnName);
        } else {
            key = norm(tableName) + "." + norm(columnName);
        }

        Integer idx = indexByRef.get(key);
        if (idx == null) {
            throw new IllegalArgumentException("Column not found: " + key);
        }
        return idx.intValue();
    }

    private String norm(String name) {
        return name.trim().toLowerCase(Locale.ROOT);
    }
}
