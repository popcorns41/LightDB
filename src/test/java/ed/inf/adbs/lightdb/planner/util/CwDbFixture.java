package ed.inf.adbs.lightdb.planner.util;

import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.SchemaLoader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class CwDbFixture {
    private static boolean loaded = false;

    private CwDbFixture() {}
    // Loads the coursework sample DB into the singleton Catalog exactly once. 
    // Tests can call this in their @BeforeClass to ensure the DB is loaded before any tests run,
    //  without worrying about whether some other test has already loaded it.
    public static void ensureLoaded() throws Exception {
        Path dbDir = Paths.get("samples", "db");
        Path schemaFile = dbDir.resolve("schema.txt");
        Path dataDir = dbDir.resolve("data");
        Path catalogTxt = dbDir.resolve("catalog.txt");
        Path catalogTmp = dbDir.resolve("catalog.txt.tmp");

        if (!Files.exists(schemaFile)) {
            throw new IllegalStateException("Missing schema file: " + schemaFile.toAbsolutePath());
        }
        if (!Files.isDirectory(dataDir)) {
            throw new IllegalStateException("Missing data dir: " + dataDir.toAbsolutePath());
        }

        // If already loaded *and* Catalog is still initialized, do nothing.
        // (This avoids the case where some other test reset the Catalog but loaded=true)
        if (loaded) {
            try {
                Catalog.getInstance();
                return;
            } catch (IllegalStateException ignored) {
                // Catalog was reset; fall through and re-init
            }
        }

        // Always start from a clean Catalog for fixture-based tests
        Catalog.resetForTests();

        // Kill persisted state so we don't load stale/OS-specific paths from catalog.txt
        Files.deleteIfExists(catalogTxt);
        Files.deleteIfExists(catalogTmp);

        Catalog.init(dbDir);
        SchemaLoader.loadIntoCatalog(schemaFile, dataDir);

        loaded = true;
    }
}