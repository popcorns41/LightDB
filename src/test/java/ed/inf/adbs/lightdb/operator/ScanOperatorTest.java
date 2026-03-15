package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.operator.util.TestDb;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ScanOperatorTest {

    private TestDb db;

    @Before
    public void setUp() throws Exception {
        db = TestDb.create()
                .writeSchema("Student A B C\n")
                .writeTable("Student",
                        "1,2,3\n" +
                        "4,5,6\n");

        db.initCatalog();
    }

    @Test
    public void scanReadsAllRows() {
        ScanOperator scan = new ScanOperator("Student");

        List<String> rows = new ArrayList<>();
        Tuple t;
        while ((t = scan.getNextTuple()) != null) {
            rows.add(t.toString());
        }

        assertEquals(2, rows.size());
        assertEquals("1, 2, 3", rows.get(0));
        assertEquals("4, 5, 6", rows.get(1));
    }

    @Test
    public void resetWorks() {
        ScanOperator scan = new ScanOperator("Student");

        assertNotNull(scan.getNextTuple());
        scan.reset();
        assertNotNull(scan.getNextTuple());
    }
}