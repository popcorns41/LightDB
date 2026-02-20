package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.catalog.ColumnMeta;
import ed.inf.adbs.lightdb.util.ColumnIndexResolver;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * SelectOperator is a unary operator that applies a selection predicate (WHERE clause) to the tuples produced by its child operator.
 * It uses a SelectionExpressionEvaluator to evaluate the predicate on each tuple, and only returns those tuples for which the predicate evaluates to true.
 * If the predicate is null, it simply passes through all tuples from the child. The operator also supports resetting, which resets the child operator
 *  to allow for re-scanning.
 */

public final class SelectOperator extends Operator {

    private final Operator child;
    private final Expression where;
    private final ColumnIndexResolver resolver;

    public SelectOperator(Operator child,
                          Expression where,
                          TableMeta tableMeta) {

        if (child == null) throw new NullPointerException("child");
        if (tableMeta == null) throw new NullPointerException("tableMeta");

        this.child = child;
        this.where = where;

        List<String> columnNames = new ArrayList<String>();
        for (ColumnMeta c : tableMeta.getColumns()) {
            columnNames.add(c.getName());
        }

        this.resolver =
                new ColumnIndexResolver(tableMeta.getName(), columnNames);
    }

    @Override
    public Tuple getNextTuple() {
        Tuple t;

        while ((t = child.getNextTuple()) != null) {

            if (where == null) return t;

            SelectionExpressionEvaluator evaluator =
                    new SelectionExpressionEvaluator(resolver,t);

            if (evaluator.eval(where)) {
                return t;
            }
        }

        return null;
    }

    @Override
    public void reset() {
        child.reset();
    }
}
