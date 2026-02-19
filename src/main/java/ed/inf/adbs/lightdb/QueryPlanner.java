package ed.inf.adbs.lightdb;

import java.io.FileReader;

import ed.inf.adbs.lightdb.catalog.Catalog;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.operator.Operator;
import ed.inf.adbs.lightdb.operator.ScanOperator;
import ed.inf.adbs.lightdb.operator.SelectOperator;
import ed.inf.adbs.lightdb.operator.ProjectOperator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.Table;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

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

            PlainSelect plainSelect = select.getPlainSelect();

            if (plainSelect == null) {
                throw new IllegalArgumentException("Only simple SELECT queries are supported (no UNION/subqueries).");
            }

            // ----- FROM clause: only support single table scans for now -----

            FromItem fromItem = plainSelect.getFromItem();

            if (!(fromItem instanceof Table)) {
                throw new IllegalArgumentException("Only table scans are supported in FROM clause.");
            }

            String tableName = ((Table) fromItem).getName();
            
            //Need TableMeta for selection/projection resolution
            TableMeta tableMeta = Catalog.getInstance()
                    .getTable(tableName)
                    .orElseThrow(() -> new IllegalArgumentException("Table not found in catalog: " + tableName));
            
            // Base operator
            Operator scan = new ScanOperator(tableName);

            // WHERE (selection)
            Expression where = plainSelect.getWhere();
            if (where != null) {
               TableMeta meta = Catalog.getInstance()
                        .getTable(tableName)
                        .orElseThrow(() -> new IllegalArgumentException("Table not found in catalog: " + tableName));

                scan = new SelectOperator(scan, where, meta);
            }

            List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
            
            // Projection: if not SELECT *, add a ProjectOperator on top of the scan/selection
            boolean isSelectStar =
                    selectItems.size() == 1 &&
                    (selectItems.get(0).getExpression() instanceof AllColumns
                    || selectItems.get(0).getExpression() instanceof AllTableColumns);

            if (!isSelectStar) {
                List<String> selectedColumns = new java.util.ArrayList<String>(selectItems.size());
                for (SelectItem<?> item : selectItems) {
                    Expression expr = item.getExpression();

                    if (!(expr instanceof Column)) {
                        throw new IllegalArgumentException("Only column references are supported in SELECT clause.");
                    }

                    Column col = (Column) expr;

                    if (col.getTable() != null && col.getTable().getName() != null) {
                        selectedColumns.add(col.getTable().getName() + "." + col.getColumnName());
                    } else {
                        selectedColumns.add(col.getColumnName());
                    }

                }

                scan = new ProjectOperator(scan, selectedColumns, tableMeta);
            }
        return scan;

        } catch (Exception e) {
            throw new RuntimeException("Error building query plan from file: " + fileName, e);
        }
    }
}
