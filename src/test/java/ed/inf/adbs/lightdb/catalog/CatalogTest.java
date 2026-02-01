package ed.inf.adbs.lightdb.catalog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class CatalogTest {

    private Path dbRoot;

    @Before
    public void setUp() throws Exception {
        resetCatalogSingleton();
        dbRoot = Files.createTempDirectory("lightdb_test_db");
        Catalog.init(dbRoot);
    }

    @After
    public void tearDown() throws Exception {
        resetCatalogSingleton();
    }

    @Test
    public void testCreateAndGetTableByName() throws Exception {
        Catalog catalog = Catalog.getInstance();

        List<ColumnMeta> columns = Arrays.asList(
                new ColumnMeta("id", DataType.INT, false),
                new ColumnMeta("name", DataType.STRING, true)
        );

        TableMeta tableMeta = catalog.createTable("users", columns);

        Optional<TableMeta> retrieved = catalog.getTable("users");
        assertTrue(retrieved.isPresent());
        assertEquals(tableMeta.getTableId(), retrieved.get().getTableId());
        assertEquals("users", retrieved.get().getName());
        assertEquals(2, retrieved.get().getColumns().size());

        assertTrue(retrieved.get().getDataFile().contains("tables"));
    }

    @Test
    public void testCreateDuplicateTableFails() throws Exception {
        Catalog catalog = Catalog.getInstance();

        List<ColumnMeta> columns = Arrays.asList(
                new ColumnMeta("id", DataType.INT, false)
        );

        catalog.createTable("products", columns);

        try {
            catalog.createTable("products", columns);
            fail("Expected IllegalArgumentException for duplicate table creation");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testListTablesSortedByName() throws Exception {
        Catalog catalog = Catalog.getInstance();

        List<ColumnMeta> columns = Arrays.asList(
                new ColumnMeta("id", DataType.INT, false)
        );

        catalog.createTable("z_table", columns);
        catalog.createTable("a_table", columns);
        catalog.createTable("m_table", columns);

        List<TableMeta> tables = catalog.listTables();
        assertEquals(3, tables.size());
        assertEquals("a_table", tables.get(0).getName());
        assertEquals("m_table", tables.get(1).getName());
        assertEquals("z_table", tables.get(2).getName());
    }

    @Test
    public void testDropTableRemovesMetadataButKeepsFileWhenRequested() throws Exception {
        Catalog catalog = Catalog.getInstance();

        List<ColumnMeta> columns = Arrays.asList(
                new ColumnMeta("id", DataType.INT, false)
        );

        TableMeta tableMeta = catalog.createTable("to_drop", columns);
        Path dataFile = Paths.get(tableMeta.getDataFile());
        Files.createDirectories(dataFile.getParent());
        Files.write(dataFile, new byte[]{1, 2, 3});

        catalog.dropTable("to_drop", false);

        assertFalse(catalog.getTable("to_drop").isPresent());
        assertTrue("Data file should still exist", Files.exists(dataFile));
    }

    @Test
    public void testDropTablesDeletesFileWhenRequested() throws Exception {
        Catalog catalog = Catalog.getInstance();

        List<ColumnMeta> columns = Arrays.asList(
                new ColumnMeta("id", DataType.INT, false)
        );

        TableMeta tableMeta = catalog.createTable("to_delete", columns);

        Path dataFile = Paths.get(tableMeta.getDataFile());
        Files.createDirectories(dataFile.getParent());
        Files.write(dataFile, new byte[]{1, 2, 3});

        catalog.dropTable("to_delete", true);

        assertFalse(catalog.getTable("to_delete").isPresent());
        assertFalse("Data file should be deleted", Files.exists(dataFile));
    }

    @Test
    
    public void testPersistenceAcrossRestart() throws Exception {
        Catalog catalog = Catalog.getInstance();

        List<ColumnMeta> columns = Arrays.asList(
                new ColumnMeta("id", DataType.LONG, false),
                new ColumnMeta("age", DataType.INT, false)
        );

        TableMeta tableMeta = catalog.createTable("persistent_table", columns);

        catalog.flush();

        resetCatalogSingleton();
        Catalog.init(dbRoot);

        Catalog newCatalog = Catalog.getInstance();
        Optional<TableMeta> retrieved = newCatalog.getTable("persistent_table");

        assertTrue(retrieved.isPresent());
        assertEquals(tableMeta.getTableId(), retrieved.get().getTableId());
        assertEquals("persistent_table", retrieved.get().getName());
        assertEquals(2, retrieved.get().getColumns().size());
        assertEquals(tableMeta.getDataFile(), retrieved.get().getDataFile());
    }




    /**
     * Reset Catalog.INSTANCE between tests so each test gets a clean singleton.
     * This avoids test ordering issues and state leakage.
     */
    private static void resetCatalogSingleton() throws Exception {
        Field f = Catalog.class.getDeclaredField("INSTANCE");
        f.setAccessible(true);
        f.set(null, null);
    }
}
