package ed.inf.adbs.lightdb.catalog;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class TableMeta {
    private final long tableId;
    private final String name;
    private final List<ColumnMeta> columns;
    private final String dataFile;

    public TableMeta(long tableId, String name, List<ColumnMeta> columns, String dataFile) {
        this.tableId = tableId;
        this.name = Objects.requireNonNull(name);
        this.columns = Collections.unmodifiableList(Objects.requireNonNull(columns,"columns cannot be null"));
        this.dataFile = Objects.requireNonNull(dataFile,"dataFile cannot be null");
    }

    public long getTableId() {
        return tableId;
    }

    public String getName() {
        return name;
    }

    public List<ColumnMeta> getColumns() {
        return columns;
    }

    public String getDataFile() {
        return dataFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableMeta)) return false;
        TableMeta tableMeta = (TableMeta) o;
        return tableId == tableMeta.tableId &&
                name.equals(tableMeta.name) &&
                columns.equals(tableMeta.columns) &&
                dataFile.equals(tableMeta.dataFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, name, columns, dataFile);
    }

    @Override
    public String toString() {
        return "TableMeta{" +
                "tableId=" + tableId +
                ", name='" + name + '\'' +
                ", columns=" + columns +
                ", dataFile='" + dataFile + '\'' +
                '}';
    }   
}