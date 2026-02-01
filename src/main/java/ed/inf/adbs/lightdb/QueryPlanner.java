package ed.inf.adbs.lightdb;

import java.io.FileReader;

import ed.inf.adbs.lightdb.operator.Operator;
import ed.inf.adbs.lightdb.operator.ScanOperator;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.Table;

public final class QueryPlanner {
    private QueryPlanner() {}


    public static Operator buildPlan(String fileName){
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(fileName));

            if (!(statement instanceof Select)) {
                throw new IllegalArgumentException("Only SELECT statements are supported.");
            }

            Select select = (Select) statement;
            PlainSelect plainSelect = select.getPlainSelect();

            if (plainSelect == null) {
                throw new IllegalArgumentException("SELECT statement must have a FROM clause.");
            }

            FromItem fromItem = plainSelect.getFromItem();

            if (!(fromItem instanceof Table)) {
                throw new IllegalArgumentException("Only table scans are supported in FROM clause.");
            }

            String tableName = ((Table) fromItem).getName();

            return new ScanOperator(tableName);

        } catch (Exception e) {
            throw new RuntimeException("Error building query plan from file: " + fileName, e);
        }
    }
}
