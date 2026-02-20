package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;

import java.util.*;



/**
 * ASSUMES ALL FIELDS ARE INTS 
 * 
 * This operator takes all tuples from the child operator, materializes them in memory, 
 * and sorts them according to the specified ORDER BY columns and sort directions.
 * 
 * NOTE: This implementation materializes the entire input before sorting, which is not memory efficient for large datasets.
 *  In a real system, you would want to implement an external sort that can handle data larger than memory.
 * 
 * @PARAM child the input operator to sort
 * @param orderByColumns the list of column references to sort by, in order of precedence (e.g. ["Course.cid", "Student.name"])
 * @param ascending a list of booleans indicating whether each corresponding column in orderByColumns should be sorted in ascending (true) or descending (false) order
 * @param outputRefsInOrder the list of column references that the child operator outputs, in the order they appear in the output tuples (e.g. ["Course.cid", "Student.name"])
 * 
*/

public final class SortOperator extends Operator {

    private final Operator child;
    private final int[] orderByIndexes;
    private final boolean[] ascending;

    private List<Tuple> sorted;
    private int pos;

    public SortOperator(Operator child,
                        List<String> orderByColumns,
                        List<Boolean> ascending,
                        List<String> outputRefsInOrder) {

        if (child == null) throw new NullPointerException("child");
        if (orderByColumns == null) throw new NullPointerException("orderByColumns");
        if (ascending == null) throw new NullPointerException("ascending");
        if (outputRefsInOrder == null) throw new NullPointerException("outputRefsInOrder");
        if (orderByColumns.size() != ascending.size()) {
            throw new IllegalArgumentException("orderByColumns and ascending must be same length");
        }

        this.child = child;

        this.ascending = new boolean[ascending.size()];
        for (int i = 0; i < ascending.size(); i++) this.ascending[i] = ascending.get(i).booleanValue();

        // Map projected output refs to indices
        Map<String, Integer> index = new HashMap<String, Integer>();
        for (int i = 0; i < outputRefsInOrder.size(); i++) {
            // store both qualified and unqualified if unambiguous (CW is usually simple)
            String ref = norm(outputRefsInOrder.get(i));
            index.put(ref, i);

            if (ref.contains(".")) {
                String unq = ref.substring(ref.indexOf('.') + 1);
                // only store unqualified if not already present (avoid ambiguity)
                if (!index.containsKey(unq)) index.put(unq, i);
            }
        }

        this.orderByIndexes = new int[orderByColumns.size()];
        for (int i = 0; i < orderByColumns.size(); i++) {
            String ref = norm(orderByColumns.get(i));
            Integer idx = index.get(ref);
            if (idx == null) {
                throw new IllegalArgumentException(
                        "ORDER BY column '" + ref + "' is not available after projection. " +
                        "Projected columns are: " + outputRefsInOrder
                );
            }
            orderByIndexes[i] = idx.intValue();
        }

        this.sorted = null;
        this.pos = 0;
    }

    @Override
    public Tuple getNextTuple() {
        if (sorted == null) materializeAndSort();
        if (pos >= sorted.size()) return null;
        return sorted.get(pos++);
    }

    @Override
    public void reset() {
        child.reset();
        sorted = null;
        pos = 0;
    }

    private void materializeAndSort() {
        List<Tuple> all = new ArrayList<Tuple>();
        Tuple t;
        while ((t = child.getNextTuple()) != null) all.add(t);

        Collections.sort(all, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple a, Tuple b) {
                for (int i = 0; i < orderByIndexes.length; i++) {
                    int idx = orderByIndexes[i];

                    long av = Long.parseLong(a.get(idx).trim());
                    long bv = Long.parseLong(b.get(idx).trim());

                    // Compare as longs for numeric sorting, based on assumption that all fields are ints.
                    int cmp = (av < bv) ? -1 : (av > bv) ? 1 : 0;
                    if (cmp != 0) return SortOperator.this.ascending[i] ? cmp : -cmp;
                }

                // deterministic tie-breaker
                int n = Math.min(a.size(), b.size());
                for (int i = 0; i < n; i++) {
                    int cmp = a.get(i).compareTo(b.get(i));
                    if (cmp != 0) return cmp;
                }
                return Integer.compare(a.size(), b.size());
            }
        });

        this.sorted = all;
        this.pos = 0;
    }

    private static String norm(String s) {
        return s.trim().toLowerCase(java.util.Locale.ROOT);
    }
}