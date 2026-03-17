# README

## Task 1: JOIN CONDITION EXTRACTION 

Join condition extraction is implemented in the class:

```text
    ed.inf.adbs.lightdb.planner.WhereClassifier
```

The WHERE clause is first decomposed into a list of conjunctive predictes using:

```text
    ExpressionUtils.splitConjuncts(...)
```

Each predicate is then classified based on the set of tables it references:

1. Single-table predicates
    - These predicates are pushed down and applied as early as possible.
2. Join predicates
    - These predicates are attached directly to the JoinOperator to avoid producing unecessary cross products.
3. Remaining predicates
    - Any predicates not extracted in the previous steps are applied at the top of the plan using FilterOperator.

This logic ensures:
- Early filtering of tuples
- Correct placement of join conditions
- Avoidance of unecessary intermediate results

---

## Task 2: Query optimisation

The system implements rule-based logical optimisation during query planning in:

```text
    ed.inf.adbs.lightdb.planner.PlanBuilder
    ed.inf.adbs.lightdb.planner.RequiredColumnsAnalyser
    ed.inf.adbs.lightdb.planner.QueryAnalyser
```

The following optimisation rules are applied:

### 1. Predicate Pushdown

Selection predicates that reference only a single table are applied immediately after the ScanOperator.

Correctness:
- Selection is commutative with joints when it only depends on one relation.

benefit:
- Reduces the number of tuples before joins
- Minimises join input size.

2. Join Predicate Placement

Joint predicates are applied at the JoinOperator rather than after the join. 

Correctness:
- Join conditions define the join itself and must be applied during combination.

Benefit:
- Avoids generating large cross products.
- Reduces intermediate result size signficantly.

### 3. Projection Pushdown

Only required columns are retained as early as possible using ProjectOperator.

Required columns are computed using `RequiredColumnsAnalyser` based on:

- SELECT clause
- WHERE clause
- JOIN conditions
- GROUP BY
- ORDER BY
- Aggregate expressions

Correctness:
- Projection is safe as long as all required attributes for later operators are preserved.

Benefit:
- Reduces tuple width.
- Reduces memory usage.
- Improves performance of joins and aggregation.

### 4. Late Projection and Sorting

Projection is applied before ORDER BY.

Correctness:
- ORDER BY only depends on output columns (validated during planning).

Benefit:
-Sorting is performed on smaller tuples, improving efficiency.

### 5. Aggregation Placement

Aggregation is applied after filtering and joins.

Correctness:
- GROUP BY semantics require aggregation over the final filtered dataset.

Benefit:
- Reduces the number of tuples before expensive operations like sorting.

### 6. DISTINCT via Hashing

Duplicate elimination is implemented using a LinkedHahSet.

Correctness:
- Ensures only unique tuples are retained.

Benefit:
- Linear-time duplicate removal
- Preserves deterministic output order

---

## Additional Information

### Added functionality

Although not required by the coursework specification, implementing this simple persistence mechanism for Catalog was a self assigned challenge. Using:

```text
    CatalogSnapshot
    CatalogSnapshotIO
```

This allows the catalog state (table metadata, schemas, and file locations) to
be serialised to disk and reloaded across executions.

Design:
- The catalog is written to a file (catalog.txt) using a snapshot representation
- Updates are flushed atomically to avoid partial writes
- On initialisation, the catalog attempts to reload any existing snapshot

Snapshots were utilised during testing and debugging to create repeatable test setups without rebuilding metadata manually.

Note: This functionality is not required for query execution correctness and is not
relied upon by the query planner or operators. It is included as an additional
extension.

### Generative AI use

Microsoft Copilot on VScode was utilised to aid in general debugging and documentation comments generation. 
