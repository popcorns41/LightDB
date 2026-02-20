package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.catalog.TableMeta;
import ed.inf.adbs.lightdb.expression.SumExprEvaluator;
import ed.inf.adbs.lightdb.util.ColumnResolver;
import ed.inf.adbs.lightdb.util.MultiTableColumnIndexResolver;
import net.sf.jsqlparser.expression.Expression;

import java.util.*;


//TODO: Review 
public final class SumOperator extends Operator {

    private final Operator child;

    // used for grouping (from GROUP BY clause)
    private final int[] groupKeyIndexes;

    // printed in output before sums (from non-agg SELECT items)
    private final int[] outputKeyIndexes;

    private final List<Expression> sumArgs;
    private final ColumnResolver resolver;

    private List<Tuple> out;
    private int pos;

    public SumOperator(Operator child,
                       List<String> groupByRefs,
                       List<String> outputGroupRefs,
                       List<Expression> sumArgs,
                       List<TableMeta> tablesInOrder) {

        if (child == null) throw new NullPointerException("child");
        if (groupByRefs == null) throw new NullPointerException("groupByRefs");
        if (outputGroupRefs == null) throw new NullPointerException("outputGroupRefs");
        if (sumArgs == null) throw new NullPointerException("sumArgs");
        if (tablesInOrder == null) throw new NullPointerException("tablesInOrder");

        this.child = child;
        this.sumArgs = sumArgs;

        this.resolver = new MultiTableColumnIndexResolver(tablesInOrder);

        this.groupKeyIndexes = resolveIndexes(groupByRefs);
        this.outputKeyIndexes = resolveIndexes(outputGroupRefs);

        this.out = null;
        this.pos = 0;
    }

    private int[] resolveIndexes(List<String> refs) {
        int[] idxs = new int[refs.size()];

        for (int i = 0; i < refs.size(); i++) {
            String ref = refs.get(i);

            String table = null;
            String col = ref;

            if (ref.contains(".")) {
                String[] parts = ref.split("\\.");
                table = parts[0];
                col = parts[1];
            }

            idxs[i] = resolver.indexOf(table, col);
        }

        return idxs;
    }

    @Override
    public Tuple getNextTuple() {
        if (out == null) materialise();
        if (pos >= out.size()) return null;
        return out.get(pos++);
    }

    @Override
    public void reset() {
        child.reset();
        out = null;
        pos = 0;
    }

    private void materialise() {
        // For each group: store output key values + sums
        Map<GroupKey, AggState> acc = new LinkedHashMap<GroupKey, AggState>();

        Tuple t;
        while ((t = child.getNextTuple()) != null) {
            GroupKey key = makeGroupKey(t);

            AggState state = acc.get(key);
            if (state == null) {
                state = new AggState(extractOutputKeys(t), sumArgs.size());
                acc.put(key, state);
            }

            SumExprEvaluator eval = new SumExprEvaluator(t, resolver);
            for (int i = 0; i < sumArgs.size(); i++) {
                state.sums[i] += eval.eval(sumArgs.get(i));
            }
        }

        List<Tuple> result = new ArrayList<Tuple>(acc.size());
        for (AggState st : acc.values()) {
            List<String> row = new ArrayList<String>(st.outputKeys.size() + st.sums.length);

            // output group columns (maybe none)
            row.addAll(st.outputKeys);

            // sums
            for (int i = 0; i < st.sums.length; i++) {
                row.add(Long.toString(st.sums[i]));
            }

            result.add(new Tuple(row));
        }

        this.out = result;
        this.pos = 0;
    }

    private GroupKey makeGroupKey(Tuple t) {
        if (groupKeyIndexes.length == 0) {
            return GroupKey.EMPTY; // global aggregation single group
        }

        List<String> vals = new ArrayList<String>(groupKeyIndexes.length);
        for (int idx : groupKeyIndexes) {
            vals.add(t.get(idx));
        }
        return new GroupKey(vals);
    }

    private List<String> extractOutputKeys(Tuple t) {
        if (outputKeyIndexes.length == 0) return Collections.emptyList();
        List<String> vals = new ArrayList<String>(outputKeyIndexes.length);
        for (int idx : outputKeyIndexes) {
            vals.add(t.get(idx));
        }
        return vals;
    }

    private static final class AggState {
        final List<String> outputKeys;
        final long[] sums;

        AggState(List<String> outputKeys, int numSums) {
            this.outputKeys = outputKeys;
            this.sums = new long[numSums];
        }
    }

    private static final class GroupKey {
        static final GroupKey EMPTY = new GroupKey(Collections.<String>emptyList());

        final List<String> values;

        GroupKey(List<String> values) {
            this.values = Collections.unmodifiableList(new ArrayList<String>(values));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GroupKey)) return false;
            GroupKey other = (GroupKey) o;
            return values.equals(other.values);
        }

        @Override
        public int hashCode() {
            return values.hashCode();
        }
    }
}