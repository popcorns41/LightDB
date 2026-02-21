package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class Tuple {
    private final List<String> values;

    public Tuple(List<String> values){
        this.values = Collections.unmodifiableList(new ArrayList<String>(Objects.requireNonNull(values, "values cannot be null")));
    }

    public int size(){
        return values.size();
    }

    public String get(int index){
        return values.get(index);
    }

    public List<String> asList(){
        return values;
    }

    @Override
    public String toString(){
        return String.join(", ",values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tuple)) return false;
        Tuple tuple = (Tuple) o;
        return values.equals(tuple.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}