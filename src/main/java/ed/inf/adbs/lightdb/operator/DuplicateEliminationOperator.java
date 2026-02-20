package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class DuplicateEliminationOperator extends Operator{

    private final Operator child;

    private List<Tuple> distinctTuples;
    private int pos;

    public DuplicateEliminationOperator(Operator child){
        if (child == null) throw new NullPointerException("child");
        this.child = child;
        this.distinctTuples = null;
        this.pos = 0;
    }

    @Override
    public Tuple getNextTuple(){
        if (distinctTuples == null){
            materialiseDistinct();
        }

        if (pos >= distinctTuples.size()) return null;
        return distinctTuples.get(pos++);
    }

    @Override
    public void reset(){
        child.reset();
        distinctTuples = null;
        pos = 0;
    }

    private void materialiseDistinct(){
        Set<Tuple> seen = new LinkedHashSet<Tuple>();
        Tuple t;

        while ((t = child.getNextTuple()) != null){
            seen.add(t);
        }

        distinctTuples = new ArrayList<Tuple>(seen);
        pos = 0;
    }
}
