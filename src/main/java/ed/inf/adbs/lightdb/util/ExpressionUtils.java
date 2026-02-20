package ed.inf.adbs.lightdb.util;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.*;

public final class ExpressionUtils {
    private ExpressionUtils(){}

    /**Flatten AND into a list of conjunct expressions. */

    public static List<Expression> splitConjuncts(Expression where){
        List<Expression> out = new ArrayList<Expression>();
        if (where == null) return out;
        split(where,out);
        return out;
    }

    private static void split(Expression e, List<Expression> out){
        if (e instanceof AndExpression){
            AndExpression a = (AndExpression) e;
            split(a.getLeftExpression(),out);
            split(a.getRightExpression(),out);
        } else {
            out.add(e);
        }
    }

    /** Build an AND expression from a list; returns null if empty. */
    public static Expression andAll(List<Expression> exprs){
        if (exprs == null || exprs.isEmpty()) return null;
        Expression acc = exprs.get(0);
        for (int i = 1; i < exprs.size(); i++){
            acc = new AndExpression(acc,exprs.get(i));
        }
        return acc;
    }

    /** Collect table names referenced by Column nodes inside an expression. */
    public static Set<String> referencedTables(Expression e){
        final Set<String> tables = new HashSet<String>();

        if (e == null) return tables;
        
        // Walk the expression tree and collect table names from Column nodes
        e.accept(new ExpressionDeParser() {
            @Override
            public void visit(Column column){
                if (column.getTable() != null && column.getTable().getName() != null){
                    tables.add(column.getTable().getName());
                }
            }
        });

        return tables;
    }
}
