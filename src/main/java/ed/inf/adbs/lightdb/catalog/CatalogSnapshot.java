package ed.inf.adbs.lightdb.catalog;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class CatalogSnapshot {
    private final long lastTableId;
    private final List<TableMeta> tables;

    public CatalogSnapshot(long lastTableId, List<TableMeta> tables) {
        this.lastTableId = lastTableId;
        this.tables = Collections.unmodifiableList(Objects.requireNonNull(tables,"tables cannot be null"));
    }

    public long getLastTableId() {
        return lastTableId;
    }
    public List<TableMeta> getTables() {
        return tables;
    }
    
}
