package ed.inf.adbs.lightdb.operator.util;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.operator.Operator;

import java.util.ArrayList;
import java.util.List;

public final class Helpers {

    private Helpers() {
        // prevent instantiation
    }

    /**
     * Collects all tuples from an operator into a List of String rows.
     */
    public static List<String> collectAll(Operator op) {
        if (op == null) throw new NullPointerException("operator cannot be null");

        List<String> out = new ArrayList<String>();
        Tuple t;

        while ((t = op.getNextTuple()) != null) {
            out.add(t.toString());
        }

        return out;
    }

    /**
     * Collects all tuples and resets the operator afterwards.
     * Useful for reuse inside the same test.
     */
    public static List<String> collectAllAndReset(Operator op) {
        List<String> result = collectAll(op);
        op.reset();
        return result;
    }

    /**
     * Collect raw Tuple objects (if a test needs structural inspection).
     */
    public static List<Tuple> collectTuples(Operator op) {
        List<Tuple> out = new ArrayList<Tuple>();
        Tuple t;

        while ((t = op.getNextTuple()) != null) {
            out.add(t);
        }

        return out;
    }
}