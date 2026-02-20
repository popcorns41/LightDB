package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.util.MultiTableColumnIndexResolver;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * JoinOperator is a binary operator that performs a nested loop join between two child operators (left and right) based on an
 * optional join condition. It iterates through each tuple from the left child and for each left tuple, it iterates through the right
 * child to find matching tuples based on the join condition.
 */

public final class JoinOperator extends Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    private final Expression joinCondition;

    //Needed to resolve Column refs over the concatenated tuple
    private final List<TableMeta> tableMetas;
    private final MultiTableColumnIndexResolver resolver;

    private Tuple currentLeft;

    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition, List<TableMeta> leftTables, List<TableMeta> rightTables) {
        if (leftChild == null) throw new NullPointerException("leftChild cannot be null");
        if (rightChild == null) throw new NullPointerException("rightChild cannot be null");
        if (leftTables == null) throw new NullPointerException("leftTables cannot be null");
        if (rightTables == null) throw new NullPointerException("rightTables cannot be null");

        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;


        this.tableMetas = new ArrayList<TableMeta>(leftTables.size() + rightTables.size());
        this.tableMetas.addAll(leftTables);
        this.tableMetas.addAll(rightTables);

        this.resolver = new MultiTableColumnIndexResolver(this.tableMetas);
        this.currentLeft = null;
    }

    @Override
    public Tuple getNextTuple() {
        while (true) {
            if (currentLeft == null) {
                currentLeft = leftChild.getNextTuple();
                if (currentLeft == null) return null; // no more tuples
                rightChild.reset(); // reset right child for next left tuple
            }

            Tuple rightTuple;
            while ((rightTuple = rightChild.getNextTuple()) != null) {
                Tuple combined = concat(currentLeft, rightTuple);
                
                if (joinCondition == null) {
                    return combined; // if no join condition, return cartesian product
                }

                SelectionExpressionEvaluator evaluator = new SelectionExpressionEvaluator(resolver, combined);

                if (evaluator.eval(joinCondition)) {
                    return combined;
                }
            }

            currentLeft = null;
        }
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        currentLeft = null;
    }

    private Tuple concat(Tuple left, Tuple right) {
        List<String> combinedValues = new ArrayList<String>(left.size() + right.size());
        combinedValues.addAll(left.asList());
        combinedValues.addAll(right.asList());
        return new Tuple(combinedValues);
    }

    public List<TableMeta> getOutputTablesInOrder() {
        return this.tableMetas;
    }
}
