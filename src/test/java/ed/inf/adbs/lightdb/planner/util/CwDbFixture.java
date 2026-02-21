package ed.inf.adbs.lightdb.planner.util;

import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.SchemaLoader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Loads the coursework sample DB into the singleton Catalog exactly once. */
public final class CwDbFixture {
    private static boolean loaded = false;

    private CwDbFixture() {}

    public static void ensureLoaded() throws Exception {
        if (loaded) return;

        Path dbDir = Paths.get("samples", "db");
        Path schemaFile = dbDir.resolve("schema.txt");
        Path dataDir = dbDir.resolve("data");

        if (!Files.exists(schemaFile)) {
            throw new IllegalStateException("Missing schema file: " + schemaFile.toAbsolutePath());
        }
        if (!Files.isDirectory(dataDir)) {
            throw new IllegalStateException("Missing data dir: " + dataDir.toAbsolutePath());
        }

        Catalog.init(dbDir);
        // If Catalog already contains tables (due to previous tests),
        // SchemaLoader should be idempotent OR validate. Either way is fine.
        SchemaLoader.loadIntoCatalog(schemaFile, dataDir);

        loaded = true;
    }
}