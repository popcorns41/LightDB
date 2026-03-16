package ed.inf.adbs.lightdb.planner.util;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.operator.Operator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * TestUtils is a utility class that provides helper methods for testing the query planner and execution engine of LightDB.
 */
public final class TestUtils {
    public static File writeSqlTemp(File dir, String filename, String sql) throws Exception{
        File f = new File(dir, filename);
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        bw.write(sql);
        bw.newLine();
        bw.close();
        return f;
    }

    public static List<String> collectAll(Operator root){
        List<String> out = new ArrayList<String>();
        Tuple t;
        while ((t = root.getNextTuple()) != null){
            out.add(t.toString());
        }
        return out;
    }

    public static List<String> readAllLines(Path p) throws Exception{
        List<String> lines = Files.readAllLines(p);
        //normalise any accidental spaces after commas
        List<String> cleaned = new ArrayList<String>(lines.size());
        for (String s: lines){
            cleaned.add(s.trim());
        }
        return cleaned;
    }

    public static void assertBagEquals(List<String> expected, List<String> actual) {
    org.junit.Assert.assertEquals("Different row count", expected.size(), actual.size());

    java.util.Map<String, Integer> e = bag(expected);
    java.util.Map<String, Integer> a = bag(actual);

    if (!e.equals(a)) {
        org.junit.Assert.fail("Bag mismatch.\nExpected: " + e + "\nActual: " + a);
    }
    }

    private static java.util.Map<String, Integer> bag(List<String> lines) {
        java.util.Map<String, Integer> m = new java.util.HashMap<String, Integer>();
        for (String s : lines) {
            String key = s.trim();
            Integer c = m.get(key);
            m.put(key, c == null ? 1 : c + 1);
        }
        return m;
    }
}
