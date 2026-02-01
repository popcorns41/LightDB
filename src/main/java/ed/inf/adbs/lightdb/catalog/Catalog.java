package ed.inf.adbs.lightdb.catalog;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;;

public final class Catalog {
    private static volatile Catalog INSTANCE;

    private final Path dbRoot;
    private final Path catalogFile;
    private final Path tablesDir;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final Map<Long, TableMeta> tablesById = new HashMap<>();
    private final Map<String, Long> idByName = new HashMap<>();

    private long lastTableId = 0L;

    private Catalog(Path dbRoot) {
        this.dbRoot = dbRoot;
        this.catalogFile = dbRoot.resolve("catalog.txt");
        this.tablesDir = dbRoot.resolve("tables");
    }

    public static Catalog init(Path dbRoot) throws IOException {
        if (dbRoot == null) throw new NullPointerException("dbRoot cannot be null");

        if (INSTANCE == null) {
            synchronized (Catalog.class) {
                if (INSTANCE == null) {
                    Catalog catalog = new Catalog(dbRoot);
                    catalog.loadIfExists();
                    INSTANCE = catalog;
                }
            }
        }
        return INSTANCE;
    }

    public static Catalog getInstance() {
        Catalog inst = INSTANCE;
        if (inst == null) {
            throw new IllegalStateException("Catalog not initialized. Call Catalog.init(Path dbRoot) first.");
        }
        return inst;
    }

    public Path getDbRoot() {
        return dbRoot;
    }

    public Optional<TableMeta> getTable(String tableName){
        if (tableName == null) throw new NullPointerException("tableName cannot be null");
        String norm = normaliseName(tableName);

        rwLock.readLock().lock();

        try {
            Long tableId = idByName.get(norm);
            if (tableId == null) {
                return Optional.empty();
            }
            return Optional.of(tablesById.get(tableId));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Optional<TableMeta> getTable(long tableId){
        rwLock.readLock().lock();

        try {
            return Optional.ofNullable(tablesById.get(tableId));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // Returns a list of all tables sorted by name

    public List<TableMeta> listTables(){
        rwLock.readLock().lock();
        try {
            List<TableMeta> tables = new ArrayList<>(tablesById.values());
            Collections.sort(tables, new Comparator<TableMeta>() {
                @Override
                public int compare(TableMeta t1, TableMeta t2) {
                    return t1.getName().compareTo(t2.getName());
                }
            });
            return tables;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // Creates a new table in the catalog

     public TableMeta createTable(String tableName, List<ColumnMeta> columns) throws IOException {
        if (tableName == null) throw new NullPointerException("tableName");
        if (columns == null) throw new NullPointerException("columns");
        if (columns.isEmpty()) throw new IllegalArgumentException("Table must have at least one column.");

        String norm = normaliseName(tableName);

        rwLock.writeLock().lock();
        try {
            if (idByName.containsKey(norm)) {
                throw new IllegalArgumentException("Table already exists: " + tableName);
            }

            Files.createDirectories(tablesDir);

            long tableId = ++lastTableId;

            // Keep file name deterministic and simple.
            // (You can rename table without renaming the file later if you want.)
            String filename = tableId + "_" + norm + ".tbl";
            Path tableFile = tablesDir.resolve(filename);

            TableMeta meta = new TableMeta(tableId, norm, columns, tableFile.toString());

            tablesById.put(tableId, meta);
            idByName.put(norm, tableId);

            flushInternal();
            return meta;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    // Registers an existing table in the catalog

    public TableMeta registerTable(String tableName, List<ColumnMeta> columns, Path dataFile) throws IOException {
        if (tableName == null) throw new NullPointerException("tableName");
        if (columns == null) throw new NullPointerException("columns");
        if (dataFile == null) throw new NullPointerException("dataFile");
        if (columns.isEmpty()) throw new IllegalArgumentException("Table must have at least one column.");

        String norm = normaliseName(tableName);

        rwLock.writeLock().lock();
        try {
            if (idByName.containsKey(norm)) {
                throw new IllegalArgumentException("Table already exists: " + tableName);
            }

            long tableId = ++lastTableId;

            TableMeta meta = new TableMeta(tableId, norm, columns, dataFile.toString());

            tablesById.put(tableId, meta);
            idByName.put(norm, tableId);

            flushInternal();
            return meta;
        } finally {
            rwLock.writeLock().unlock();
        }
    }
      /**
     * Drop table metadata. Optionally delete the underlying table data file.
     * Note: catalog is updated first, then file deletion attempted.
     */

    public void dropTable(String tableName, boolean deleteDataFile) throws IOException {
        if (tableName == null) throw new NullPointerException("tableName");
        String norm = normaliseName(tableName);

        TableMeta removed = null;

        rwLock.writeLock().lock();
        try {
            Long id = idByName.remove(norm);
            if (id == null) return;

            removed = tablesById.remove(id);
            flushInternal();
        } finally {
            rwLock.writeLock().unlock();
        }

        if (deleteDataFile && removed != null) {
            Files.deleteIfExists(Paths.get(removed.getDataFile()));
        }
    }

    public void flush() throws IOException {
        rwLock.readLock().lock();
        try {
            flushInternal();
        } finally {
            rwLock.readLock().unlock();
        }
    }


    // ---------------- Persistence ----------------

    private void loadIfExists() throws IOException {
        if (!Files.exists(catalogFile)) {
            return;
        }

        CatalogSnapshot snap = CatalogSnapshotIO.read(catalogFile);

        this.lastTableId = snap.getLastTableId();
        this.tablesById.clear();
        this.idByName.clear();

        for (TableMeta table : snap.getTables()) {
            tablesById.put(table.getTableId(), table);
            idByName.put(normaliseName(table.getName()), table.getTableId());
        }
    }

    private void flushInternal() throws IOException {
        CatalogSnapshot snap = new CatalogSnapshot(lastTableId, new ArrayList<>(tablesById.values()));

        Path tmp = catalogFile.resolveSibling(catalogFile.getFileName().toString() + ".tmp");

        CatalogSnapshotIO.write(tmp, snap);

        // Atomic move to replace the catalog file; fallback if not supported

        try{
            Files.move(tmp, catalogFile,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
            );
        } catch (AtomicMoveNotSupportedException e){
            Files.move(tmp, catalogFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static String normaliseName(String name){
        return name.trim().toLowerCase();
    }
    
}
