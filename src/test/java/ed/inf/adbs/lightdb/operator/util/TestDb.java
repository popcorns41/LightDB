package ed.inf.adbs.lightdb.operator.util;

import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.SchemaLoader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class for tests that need to create temporary databases with specific schemas and data.
 * Creates a temporary directory for the database, which is automatically cleaned up when closed.
 * Provides helper methods to write schema and table data, and to initialize the Catalog for testing.
 */

public final class TestDb implements AutoCloseable {

    private final Path tempRoot;

    public final Path dbRoot;
    public final Path schemaFile;
    public final Path dataDir;

    private TestDb(Path tempRoot, Path dbRoot, Path schemaFile, Path dataDir) {
        this.tempRoot = tempRoot;
        this.dbRoot = dbRoot;
        this.schemaFile = schemaFile;
        this.dataDir = dataDir;
    }

    public static TestDb create() throws IOException {
        Catalog.resetForTests();

        Path tempRoot = Files.createTempDirectory("lightdb_test_");
        Path dbRoot = tempRoot.resolve("db");
        Path dataDir = dbRoot.resolve("data");
        Path schemaFile = dbRoot.resolve("schema.txt");

        Files.createDirectories(dataDir);

        return new TestDb(tempRoot, dbRoot, schemaFile, dataDir);
    }

    public TestDb writeSchema(String schemaText) throws IOException {
        Files.write(schemaFile, schemaText.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    public TestDb writeTable(String tableName, String csvContents) throws IOException {
        Files.write(dataDir.resolve(tableName + ".csv"), csvContents.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    public void initCatalog() throws IOException {
        Catalog.init(dbRoot);
        SchemaLoader.loadIntoCatalog(schemaFile, dataDir);
    }

    @Override
    public void close() {
        try {
            Files.walk(tempRoot)
                    .sorted((a, b) -> b.compareTo(a)) // delete children first
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); }
                        catch (IOException ignored) {}
                    });
        } catch (IOException ignored) {
        }
    }
}