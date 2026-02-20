package ed.inf.adbs.lightdb.expression;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.util.ColumnResolver;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.schema.Column;

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