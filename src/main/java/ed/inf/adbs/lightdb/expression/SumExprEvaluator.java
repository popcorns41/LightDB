package ed.inf.adbs.lightdb.expression;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.util.ColumnResolver;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.schema.Column;

/**
 * SumExprEvaluator is a utility class that evaluates an expression used as an argument to the SUM aggregate function. 
 * It supports expressions that are either:
 * - A LongValue (e.g., SUM(5))
 * - A Column reference (e.g., SUM(table.column))
 * - A Parenthesized expression (e.g., SUM((table.column)))
 * - A Multiplication of two expressions (e.g., SUM(table.column * 2))
 * The evaluation is performed in the context of a given tuple and a column resolver.
 * 
 * Please don't try break it <3
 */

public final class SumExprEvaluator {
    private final Tuple tuple;
    private final ColumnResolver resolver;

    public SumExprEvaluator(Tuple tuple, ColumnResolver resolver) {
        if (tuple == null) throw new NullPointerException("tuple");
        if (resolver == null) throw new NullPointerException("resolver");
        this.tuple = tuple;
        this.resolver = resolver;
    }

    public long eval(Expression e) {
        if (e == null) throw new IllegalArgumentException("SUM argument is null");

        if (e instanceof LongValue) {
            return ((LongValue) e).getValue();
        }

        if (e instanceof Column) {
            Column c = (Column) e;
            String table = (c.getTable() == null) ? null : c.getTable().getName();
            int idx = resolver.indexOf(table, c.getColumnName());
            return Long.parseLong(tuple.get(idx).trim());
        }

        if (e instanceof Parenthesis) {
            return eval(((Parenthesis) e).getExpression());
        }

        if (e instanceof Multiplication) {
            Multiplication m = (Multiplication) e;
            return eval(m.getLeftExpression()) * eval(m.getRightExpression());
        }

        throw new IllegalArgumentException("Unsupported SUM argument expression: " + e + " (" + e.getClass() + ")");
    }
}