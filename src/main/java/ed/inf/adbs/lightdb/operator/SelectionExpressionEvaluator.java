package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.util.ColumnResolver;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayDeque;
import java.util.Deque;


public final class SelectionExpressionEvaluator extends ExpressionDeParser {
    private final ColumnResolver resolver;
    private final Tuple tuple;

    private final Deque<Object> stack = new ArrayDeque<Object>();

    /**
     * Create an evaluator for a selection expression. Observe that the expression
     *  may contain Column references, which will be resolved using the provided resolver and tuple.
     * @param resolver
     * @param tuple
     */

    public SelectionExpressionEvaluator(ColumnResolver resolver, Tuple tuple) {
        if (resolver == null) throw new NullPointerException("resolver cannot be null");
        if (tuple == null) throw new NullPointerException("tuple cannot be null");

        this.resolver = resolver;
        this.tuple = tuple;
    }

    public boolean eval(Expression expr){
        if (expr == null){
            return true;
        }

        stack.clear();
        expr.accept(this);

        if (stack.isEmpty()) {
            throw new IllegalStateException("Evaluation stack is empty after evaluating expression: " + expr);
        }
        Object top = stack.pop();
        if (!(top instanceof Boolean)) {
            throw new IllegalStateException("Expected boolean result on top of stack after evaluating expression: " + expr + ", but got: " + top);
        }
        return ((Boolean) top).booleanValue();
    }

    @Override
    public void visit(LongValue longValue) {
        stack.push(Long.valueOf(longValue.getValue()));
    }

    @Override
    public void visit(Column column){
        String colName = column.getColumnName();
        String tableName = column.getTable() == null ? null : column.getTable().getName();

        int idx = resolver.indexOf(tableName, colName);
        long v = Long.parseLong(tuple.get(idx).trim());
        stack.push(Long.valueOf(v));
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    // -------------------------- Logical operators ------------------------------

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);

       boolean right = popBool();
       boolean left = popBool();

       stack.push(Boolean.valueOf(left && right));
    }

    @Override
    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        orExpression.getRightExpression().accept(this);

        boolean right = popBool();
        boolean left = popBool();
        stack.push(Boolean.valueOf(left || right));
    }

    // -------------------------- Comparison operators ------------------------------

    @Override
    public void visit(EqualsTo equalsTo) {
        evalCompare(equalsTo.getLeftExpression(), equalsTo.getRightExpression(), CompareOp.EQ);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        evalCompare(notEqualsTo.getLeftExpression(), notEqualsTo.getRightExpression(), CompareOp.NE);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        evalCompare(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), CompareOp.GT);
    }

    @Override
    public void visit(GreaterThanEquals expr){
        evalCompare(expr.getLeftExpression(), expr.getRightExpression(), CompareOp.GE);
    }

    @Override
    public void visit(MinorThan minorThan) {
        evalCompare(minorThan.getLeftExpression(), minorThan.getRightExpression(), CompareOp.LT);
    }

    @Override
    public void visit(MinorThanEquals expr){
        evalCompare(expr.getLeftExpression(), expr.getRightExpression(), CompareOp.LTE);
    }

    private void evalCompare(Expression left, Expression right, CompareOp op) {
        left.accept(this);
        right.accept(this);

        long rightVal = popLong();
        long leftVal = popLong();

        boolean result;
        switch (op) {
            case EQ: result = (leftVal == rightVal); break;
            case NE: result = (leftVal != rightVal); break;
            case GT: result = (leftVal > rightVal); break;
            case GE: result = (leftVal >= rightVal); break;
            case LT: result = (leftVal < rightVal); break;
            case LTE: result = (leftVal <= rightVal); break;
            default: throw new IllegalStateException("Unknown comparison operator: " + op);
        }
        stack.push(Boolean.valueOf(result));
    }

    private long popLong() {
        if (stack.isEmpty()) {
            throw new IllegalStateException("Evaluation stack is empty when expecting a long value");
        }
        Object o = stack.pop();
        if (!(o instanceof Long)) {
            throw new IllegalStateException("Expected long value on stack, but got: " + o);
        }
        return ((Long) o).longValue();
    }

    private boolean popBool() {
        if (stack.isEmpty()) {
            throw new IllegalStateException("Evaluation stack is empty when expecting a boolean value");
        }
        Object o = stack.pop();
        if (!(o instanceof Boolean)) {
            throw new IllegalStateException("Expected boolean value on stack, but got: " + o);
        }
        return ((Boolean) o).booleanValue();
    }

    private enum CompareOp {
        EQ, NE, GT, GE, LT, LTE
    }
    
}
