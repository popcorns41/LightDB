package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.util.MultiTableColumnIndexResolver;

import net.sf.jsqlparser.expression.Expression;

import java.util.List;

/**
 * FilterOperator is a unary operator that applies a selection predicate to the tuples produced by its child operator. 
 * It uses a SelectionExpressionEvaluator to evaluate the predicate on each tuple, and only returns those tuples for 
 * which the predicate evaluates to true. If the predicate is null, it simply passes through all tuples from the child. 
 * The operator also supports resetting, which resets the child operator to allow for re-scanning.
 */

public final class FilterOperator extends Operator {
    private final Operator child;
    private final Expression predicate;
    private final MultiTableColumnIndexResolver resolver;


    public FilterOperator(Operator child, Expression predicate, List<TableMeta> tablesInOrder){
        if (child == null) throw new NullPointerException("child");
        if (tablesInOrder == null) throw new NullPointerException("tablesInOrder");
        this.child = child;
        this.predicate = predicate;
        this.resolver = new MultiTableColumnIndexResolver(tablesInOrder);
    }

    @Override
    public Tuple getNextTuple(){
        Tuple t;
        while (((t = child.getNextTuple()) != null)){
            if (predicate == null) return t;
            SelectionExpressionEvaluator eval = new SelectionExpressionEvaluator(resolver, t);
            if (eval.eval(predicate)) return t;
        }
        return null;
    }

    @Override
    public void reset(){
        child.reset();
    }
}
