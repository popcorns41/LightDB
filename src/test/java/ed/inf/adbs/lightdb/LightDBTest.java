package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.PlainSelect;

import org.junit.Assert;

import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.junit.Test;

import java.nio.file.*;


import static org.junit.Assert.*;

public class LightDBTest {

    private static final Path DB_DIR = Paths.get("samples", "db");
    private static final Path INPUT_DIR = Paths.get("samples", "input");
    private static final Path EXPECTED_DIR = Paths.get("samples", "expected_output");

	//rigours testing going on here <3
	@Test
	public void mustBeTrue() {
		assertTrue(true);
	}

    @Test
    public void query1_matchesExpectedOutput() throws Exception {
        runAndCompare("query1.sql", "query1.csv");
    }

    @Test
    public void query2_matchesExpectedOutput() throws Exception {
        runAndCompare("query2.sql", "query2.csv");
    }

	@Test
	public void query3_matchesExpectedOutput() throws Exception {
		runAndCompare("query3.sql", "query3.csv");
	}

	@Test
	public void query4_matchesExpectedOutput() throws Exception {
		runAndCompare("query4.sql", "query4.csv");
	}

	@Test
	public void query5_matchesExpectedOutput() throws Exception {
		runAndCompare("query5.sql", "query5.csv");
	}

	@Test
	public void query6_matchesExpectedOutput() throws Exception {
		runAndCompare("query6.sql", "query6.csv");
	}

	@Test
	public void query7_matchesExpectedOutput() throws Exception {
		runAndCompare("query7.sql", "query7.csv");
	}

	@Test
	public void query8_matchesExpectedOutput() throws Exception {
		runAndCompare("query8.sql", "query8.csv");
	}

	@Test
	public void query9_matchesExpectedOutput() throws Exception {
		runAndCompare("query9.sql", "query9.csv");
	}

	@Test
	public void query10_matchesExpectedOutput() throws Exception {
		runAndCompare("query10.sql", "query10.csv");
	}

	@Test
	public void query11_matchesExpectedOutput() throws Exception {
		runAndCompare("query11.sql", "query11.csv");
	}

	@Test
	public void query12_matchesExpectedOutput() throws Exception {
		runAndCompare("query12.sql", "query12.csv");
	}

	private void runAndCompare(String sqlFile, String expectedFile) throws Exception {
		Path input = INPUT_DIR.resolve(sqlFile);
		Path expected = EXPECTED_DIR.resolve(expectedFile);
		Path tempOutput = Files.createTempFile("lightdb_test_", ".csv");

		// Run the engine
		LightDB.main(new String[] {
				DB_DIR.toString(),
				input.toString(),
				tempOutput.toString()
		});

		List<String> actualLines = Files.readAllLines(tempOutput);
		List<String> expectedLines = Files.readAllLines(expected);

		normalise(actualLines);
		normalise(expectedLines);

		boolean hasOrderBy = queryHasOrderBy(input);
		// If the query has an ORDER BY clause, we compare outputs as lists (order-sensitive). Otherwise, we compare as bags (order-insensitive).
		if (!hasOrderBy) {
			assertBagEquals("Mismatch for " + sqlFile + " (order-insensitive)", expectedLines, actualLines);
		} else {
			assertEquals("Mismatch for " + sqlFile + " (order-sensitive)", expectedLines, actualLines);
		}

		Files.deleteIfExists(tempOutput);
	}

	// Helper method to check if a query has an ORDER BY clause. We use this to determine whether to compare outputs as bags or lists.
	private boolean queryHasOrderBy(Path sqlFile) throws Exception {
		Statement st = CCJSqlParserUtil.parse(new FileReader(sqlFile.toFile()));

		if (!(st instanceof Select)) return false;

		Select sel = (Select) st;
		PlainSelect ps = sel.getPlainSelect();

		return ps.getOrderByElements() != null && !ps.getOrderByElements().isEmpty();
	}

	// Helper method to compare two lists of strings as bags (ignoring order and duplicates). Provides a helpful diff if they don't match.
	private void assertBagEquals(String msg, List<String> expected, List<String> actual) {
		Assert.assertEquals(msg + " (different row count)", expected.size(), actual.size());

		Map<String, Integer> e = bag(expected);
		Map<String, Integer> a = bag(actual);

		if (!e.equals(a)) {
			Map<String, Integer> missing = new HashMap<>(e);
			// We compute the difference between the expected and actual bags to find missing and extra rows.
			for (Map.Entry<String, Integer> en : a.entrySet()) {
				Integer cur = missing.get(en.getKey());
				if (cur == null) continue;
				int newVal = cur - en.getValue();
				if (newVal <= 0) missing.remove(en.getKey());
				else missing.put(en.getKey(), newVal);
			}
			
			// Anything left in 'missing' is an expected row that wasn't in the actual output, 
			// and anything left in 'extra' is an unexpected row that was in the actual output.
			Map<String, Integer> extra = new HashMap<>(a);
			for (Map.Entry<String, Integer> en : e.entrySet()) {
				Integer cur = extra.get(en.getKey());
				if (cur == null) continue;
				int newVal = cur - en.getValue();
				if (newVal <= 0) extra.remove(en.getKey());
				else extra.put(en.getKey(), newVal);
			}

			Assert.fail(msg + "\nMissing rows: " + missing + "\nExtra rows: " + extra);
		}
	}
	// Helper method to convert a list of strings into a bag (map of string to count).
	private Map<String, Integer> bag(List<String> lines) {
		Map<String, Integer> m = new HashMap<>();
		for (String s : lines) {
			String key = s.trim();
			Integer c = m.get(key);
			m.put(key, c == null ? 1 : (c + 1));
		}
		return m;
	}

    private void normalise(List<String> lines) {
        for (int i = 0; i < lines.size(); i++) {
            lines.set(i, lines.get(i).trim());
        }
    }
}
