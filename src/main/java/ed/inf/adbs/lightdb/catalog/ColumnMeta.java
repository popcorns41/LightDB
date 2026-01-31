package ed.inf.adbs.lightdb.catalog;

import java.util.Objects;

public class ColumnMeta {
    private final String name;
    private final DataType type;
    private final boolean nullable;

    public ColumnMeta(String name, DataType type, boolean nullable) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.type = Objects.requireNonNull(type, "type cannot be null");
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return "ColumnMeta{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", nullable=" + nullable +
                '}';
    }
}