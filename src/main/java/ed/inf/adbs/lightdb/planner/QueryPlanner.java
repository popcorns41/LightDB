package ed.inf.adbs.lightdb.planner;

import java.io.FileReader;

import ed.inf.adbs.lightdb.operator.Operator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

//TODO: definitely broken need to find how to fix <3

public final class QueryPlanner {
    private QueryPlanner() {}

     /**
     * Build a query plan (operator tree) from a SQL query read from the specified file. The SQL query must be a simple SELECT statement of the form:
     * SELECT [columns] FROM [table] WHERE [condition]
     * where:
     * - [columns] can be either "*" (select all) or a comma-separated list of column names (optionally prefixed with the table name, e.g., "Student.A").
     * - [table] is the name of a single table (no joins or subqueries).
     * - [condition] is an optional WHERE clause with simple conditions (e.g., "A > 5 AND B < 10").
     * @param fileName
     * @return the root operator of the query plan
     */

     public static Operator buildPlan(String fileName) {
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(fileName));

            if (!(statement instanceof Select)) {
                throw new IllegalArgumentException("Only SELECT statements are supported.");
            }

            Select select = (Select) statement;
            PlainSelect ps = select.getPlainSelect();

            if (ps == null) {
                throw new IllegalArgumentException("Only simple SELECT queries supported (no UNION/subqueries).");
            }

            return PlanBuilder.build(ps);

        } catch (Exception e) {
            throw new RuntimeException("Error building query plan from file: " + fileName, e);
        }
    }
}