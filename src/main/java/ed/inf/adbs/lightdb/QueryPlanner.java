package ed.inf.adbs.lightdb;

import java.io.FileReader;

import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.Operator;
import ed.inf.adbs.lightdb.operator.ScanOperator;
import ed.inf.adbs.lightdb.operator.SelectOperator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.Table;

public final class QueryPlanner {
    private QueryPlanner() {}

    public static Operator buildPlan(String fileName) {
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(fileName));

            if (!(statement instanceof Select)) {
                throw new IllegalArgumentException("Only SELECT statements are supported.");
            }

            Select select = (Select) statement;
            PlainSelect plainSelect = select.getPlainSelect();

            if (plainSelect == null) {
                throw new IllegalArgumentException("Only simple SELECT queries are supported (no UNION/subqueries).");
            }

            FromItem fromItem = plainSelect.getFromItem();

            if (!(fromItem instanceof Table)) {
                throw new IllegalArgumentException("Only table scans are supported in FROM clause.");
            }

            String tableName = ((Table) fromItem).getName();

            // Base operator
            Operator scan = new ScanOperator(tableName);

            // WHERE (selection)
            Expression where = plainSelect.getWhere();
            if (where == null) {
                return scan;
            }

            // Need schema to evaluate Column references in WHERE
            TableMeta tableMeta = Catalog.getInstance()
                    .getTable(tableName)
                    .orElseThrow(() -> new IllegalArgumentException("Table not found in catalog: " + tableName));

            return new SelectOperator(scan, where, tableMeta);

        } catch (Exception e) {
            throw new RuntimeException("Error building query plan from file: " + fileName, e);
        }
    }
}
