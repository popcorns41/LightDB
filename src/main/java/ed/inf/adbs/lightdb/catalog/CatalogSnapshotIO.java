package ed.inf.adbs.lightdb.catalog;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public final class CatalogSnapshotIO {
    private CatalogSnapshotIO() {
        // Prevent instantiation
    }

    public static CatalogSnapshot read(Path file) throws IOException {
        // Implementation for reading a CatalogSnapshot from the file
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))){
            long lastId = in.readLong();
            int nTables = in.readInt();

            List<TableMeta> tables = new ArrayList<>();
            for (int i = 0; i < nTables; i++) {
                long tableId = in.readLong();
                String tableName = in.readUTF();
                String dataFile = in.readUTF();
                int nColumns = in.readInt();

                List<ColumnMeta> columns = new ArrayList<>();
                for (int j = 0; j < nColumns; j++) {
                    String colName = in.readUTF();
                    DataType colType = DataType.valueOf(in.readUTF());
                    boolean nullable = in.readBoolean();
                    columns.add(new ColumnMeta(colName, colType, nullable));
                }

                tables.add(new TableMeta(tableId, tableName, columns, dataFile));

            }

            return new CatalogSnapshot(lastId, tables);
        }
    }

    public static void write(Path file, CatalogSnapshot snap) throws IOException{
        Files.createDirectories(file.getParent());

        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            out.writeLong(snap.getLastTableId());
            List<TableMeta> tables = snap.getTables();
            out.writeInt(tables.size());

            for (TableMeta table : tables) {
                out.writeLong(table.getTableId());
                out.writeUTF(table.getName());
                out.writeUTF(table.getDataFile());
                List<ColumnMeta> columns = table.getColumns();
                out.writeInt(columns.size());

                for (ColumnMeta column : columns) {
                    out.writeUTF(column.getName());
                    out.writeUTF(column.getType().name());
                    out.writeBoolean(column.isNullable());
                }
            }

            out.flush();
        }
    }
    
}
