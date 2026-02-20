package ed.inf.adbs.lightdb.operator;


import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.SchemaLoader;


import org.junit.Before;


import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;


public class ScanOperatorTest {
    
    private Path tempDbRoot;
    private Path dbRoot;
    private Path dataDir;
    private Path schemaFile;

    @Before
    public void setUp() throws Exception {
        resetCatalogSingleton();

        tempDbRoot = Files.createTempDirectory("lightdb_test_");
        dbRoot = tempDbRoot.resolve("db");
        dataDir = dbRoot.resolve("tables");
        schemaFile = dbRoot.resolve("schema.txt");

        Files.createDirectories(dataDir);
        Files.createDirectories(dbRoot);

        String schema =
                "Student A B C D\n" +
                "Course E F G\n" +
                "Enrolled A E H\n";
        Files.write(schemaFile, schema.getBytes(StandardCharsets.UTF_8));

         Files.write(dataDir.resolve("Student.csv"),
                ("101,2,3,4\n" +
                 "102,5,6,7\n" +
                 "103,8,9,10\n").getBytes(StandardCharsets.UTF_8));

        Files.write(dataDir.resolve("Course.csv"),
                ("1,CS,2025\n" +
                 "2,DB,2026\n").getBytes(StandardCharsets.UTF_8));

        Files.write(dataDir.resolve("Enrolled.csv"),
                ("101,1,A+\n" +
                 "102,2,B\n").getBytes(StandardCharsets.UTF_8));

        // Initialise catalog + load schema into it
        Catalog.init(dbRoot);
        SchemaLoader.loadIntoCatalog(schemaFile, dataDir);





    }

    private static void resetCatalogSingleton() throws Exception {
        Field f = Catalog.class.getDeclaredField("INSTANCE");
        f.setAccessible(true);
        f.set(null, null);
}

}
