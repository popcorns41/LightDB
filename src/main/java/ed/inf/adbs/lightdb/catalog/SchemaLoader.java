package ed.inf.adbs.lightdb.catalog;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public final class SchemaLoader {
    private SchemaLoader() {}

    /**
     * Loads the schema from the specified schema file and data directory into the Catalog.
     * The schema file should have lines in the format:
     * "TableName Column1 Column2 Column3"
     * For each table, it checks if the table already exists in the Catalog. If not, 
     * it registers the new table with the parsed columns and expected CSV file path. 
     * If it does exist, it verifies that the existing schema matches the parsed schema and expected data file path,
     *  throwing an exception if there is a mismatch.
     * @param schemaFile
     * @param dataDir
     * @throws IOException
     */

    public static void loadIntoCatalog(Path schemaFile, Path dataDir) throws IOException {

        if (schemaFile == null || dataDir == null) {
            throw new IllegalArgumentException("schemaFile and dataDir must not be null");
        }

        Catalog catalog = Catalog.getInstance();

        try (BufferedReader reader = Files.newBufferedReader(schemaFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;

                // Correct parsing for: "Student A B C D"
                String[] parts = line.split("\\s+");
                if (parts.length < 2) {
                    throw new IOException("Invalid schema file format: " + line);
                }

                String tableName = parts[0].trim();

                List<ColumnMeta> cols = new ArrayList<ColumnMeta>(parts.length - 1);
                for (int i = 1; i < parts.length; i++) {
                    String colName = parts[i].trim();
                    if (!colName.isEmpty()) {
                        cols.add(new ColumnMeta(colName, DataType.STRING, true));
                    }
                }

                if (cols.isEmpty()) {
                    throw new IllegalArgumentException("No columns parsed for line: " + line);
                }

                // Map table -> CSV path
                Path expectedCsv = dataDir.resolve(tableName + ".csv").normalize();

                TableMeta existing = catalog.getTable(tableName).orElse(null);

                if (existing == null) {
                    catalog.registerTable(tableName, cols, expectedCsv);
                } else {
                    if (!sameColumns(existing.getColumns(), cols)) {
                        throw new IllegalStateException(
                                "Schema mismatch for table '" + tableName + "'. " +
                                "catalog has " + existing.getColumns() + " but schema.txt has " + cols
                        );
                    }

                    // OS-friendly compare: compare Paths, not Strings
                    Path existingPath = Paths.get(existing.getDataFile()).normalize();

                    if (!existingPath.equals(expectedCsv)) {
                        throw new IllegalStateException(
                                "Data file mismatch for table '" + tableName + "'. " +
                                "catalog points to " + existingPath + " but expected " + expectedCsv
                        );
                    }
                }
            }
        }
    }
    
    // Helper method to compare two lists of ColumnMeta for equality. 
    // This checks that the columns have the same names, types, and nullability in the same order.
    
    private static boolean sameColumns(List<ColumnMeta> a, List<ColumnMeta> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).getName().equals(b.get(i).getName())) return false;
            if (a.get(i).getType() != b.get(i).getType()) return false;
            if (a.get(i).isNullable() != b.get(i).isNullable()) return false;
        }
        return true;
    }
}
