package ed.inf.adbs.lightdb.util;

/**
 * ColumnResolver is a simple interface that allows operators to resolve column references in expressions to their corresponding column indices in the operator's output schema.
 */
public interface ColumnResolver {
    int indexOf(String maybeTable, String column);
}
