# Project B — CSV Mini Database & Query Engine

Header-only C++17 educational implementation. Covers CSV parsing, row storage, hash indexing, a SQL-subset grammar, and a simple query executor. Zero external dependencies.

Project scope (Project B): teach CSV parsing, indexing, query grammar, and performance trade-offs.

## Files

- `csvdb.hpp` — single-header library, namespace `csvdb`
- `main.cpp` — parser unit tests, functional demo, 100K-row benchmark

## Build & Run

```bash
g++ -std=c++17 -O2 main.cpp -o csvdb
./csvdb
```

Tested with g++ ≥ 7, clang++ ≥ 5. No vcpkg/Conan required; hand-rolled parser chosen over `fast-cpp-csv-parser`/`rapidcsv` for pedagogical transparency.

## Architecture

Five strictly layered components:

| # | Component | Responsibility |
|---|-----------|----------------|
| 1 | `CsvParser` | RFC 4180 tokenizer (row + stream) |
| 2 | `Table` | Row-major storage + column-name → index map |
| 3 | `HashIndex` | `unordered_map<string, vector<row_id>>` |
| 4 | `QueryParser` | Recursive-descent parser → `Query` AST |
| 5 | `Database::execute_query` | Planner → filter → sort → limit → project |

### 1. CSV parsing (RFC 4180 subset)

Handles quoted fields, embedded `,` and `\n`, escaped quotes (`""` → `"`), empty fields, configurable delimiter/quote. Multi-line records reassembled via quote-count parity (`count_quotes(acc) % 2`).

**Failure mode**: unescaped literal quote characters inside a field produce odd parity and corrupt record boundaries. Conformant RFC 4180 input always yields even counts per record.

Trade-off (documented in source): hand-rolled is adequate for <1 GB. Beyond that use mmap + SIMD (`simdcsv`) or equivalent.

### 2. Storage

Row-major `vector<Row>`, `Row = vector<string>`. Deliberately not columnar.

Columnar engines (DuckDB, ClickHouse) win on analytical scans: contiguous per-column memory, SIMD-friendly comparisons, superior cache locality, higher compression. Row-major is simpler and sufficient at this scope.

Types are **inferred per-value at query time** via `infer_type()`; not stored. Cost: two `strtol`/`strtod` probes per comparison — see *Performance notes*.

### 3. Indexing

`HashIndex`: `value → vector<row_id>`. O(1) average EQ lookup. Built eagerly on `create_index(col)`. Memory footprint estimated via `HashIndex::memory_estimate()`.

Not implemented:
- Sorted/B-tree index for range predicates (`<`, `>`, `BETWEEN`)
- Bitmap index for low-cardinality columns
- Composite (multi-column) indexes

### 4. Query grammar

```
SELECT (*|col[,col]...) [FROM tbl]
       [WHERE col op val (AND col op val)*]
       [ORDER BY col [ASC|DESC]]
       [LIMIT n]

op  := = | != | < | > | <= | >=
val := 'quoted string' | number | bare_word
```

Single-table only. `FROM` token consumed but ignored. Keywords case-insensitive; column names case-sensitive. Conjunction only — no `OR`, no parentheses.

### 5. Execution strategy

1. **Plan** — first `EQ` predicate whose column has an index → `INDEX SCAN`; else `FULL SCAN`.
2. **Filter** — evaluate residual predicates against candidate row-ids.
3. **Sort** — numeric comparison when both operand and literal parse as numbers, else lexicographic.
4. **Limit** — truncate.
5. **Project** — materialize selected columns.

Every `QueryResult` carries `exec_time_ms` and `plan` string.

## Usage

```cpp
#include "csvdb.hpp"
using namespace csvdb;

Database db;
db.load_csv("employees.csv");
db.create_index("department");

auto r = db.execute(
    "SELECT name, salary "
    "WHERE department = 'Engineering' AND salary > 100000 "
    "ORDER BY salary DESC LIMIT 5");
r.print();
```

`main.cpp` demonstrates:
- `test_parser()` — 5 parser assertions (quoting, escapes, embedded newlines, empty fields)
- `demo()` — 6 queries over a 10-row employees dataset, mixing full scan and index scan
- `benchmark()` — 100K-row synthetic table, EQ predicate on a 5-value column, full scan vs. index scan

## Performance notes

Benchmark harness in `main.cpp::benchmark` — 100K rows, single EQ predicate, 20% selectivity.

- Full scan: O(N) — every row parsed, type-inferred, compared.
- Index scan: O(1) hash probe + O(|match|) materialization.

Expected speedup on this workload: 5–10×. Unique-key queries: much larger. Low-selectivity queries (>50% match): index can lose to scan due to random row access.

### Known inefficiencies (points of failure under scale)

1. `Predicate::eval` calls `infer_type` per row — re-parses strings on the hot path. Fix: infer column types once at load, cache in `Table`.
2. Filter loop calls `table_.resolve_col(p.column)` per row per predicate. Fix: hoist column resolution above the loop.
3. `ORDER BY` on numeric column invokes `std::stod` inside the comparator — O(N log N) conversions. Fix: pre-convert sort keys.
4. No string interning — duplicate column values inflate index keys and row payloads.
5. Planner selects *first* indexed EQ predicate, not *most selective*. No statistics, no cost model.
6. Predicates evaluated in source order, not selectivity order. Short-circuit is correct but order is suboptimal.

### Other limitations

- No `OR`, parentheses, joins, aggregation (`COUNT`, `SUM`, `GROUP BY`), or `OFFSET`.
- String literals cannot contain `'` (no escape mechanism).
- Column identifiers cannot contain `,`, `=`, `<`, `>`, `!`, or whitespace.
- Not thread-safe.
- `Database::load_csv` writes progress to `stdout` — library-layer side effect.
- Unterminated quoted field throws at EOF; no recovery mode.

## Suggested extensions (ordered by difficulty)

1. Add `OR` and parenthesized predicate groups.
2. Cache inferred column types in `Table`; eliminate per-row `infer_type`.
3. Implement `SortedIndex` (std::map) for range scans; update planner.
4. Aggregations: `COUNT(*)`, `SUM`, `AVG`, `GROUP BY`.
5. Cost-based planner: index selectivity = `|posting| / |rows|`; pick most selective predicate.
6. Swap to columnar storage (`vector<vector<string>>` per column); remeasure scan throughput.
7. Replace parser with mmap + SIMD (`simdcsv`) for >1 GB inputs.
