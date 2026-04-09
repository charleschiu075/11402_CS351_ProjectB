# Project B: CSV Mini Database & Query Engine

## Build & Run

```bash
g++ -std=c++17 -O2 -o csvdb src/main.cpp -I src
./csvdb data/employees.csv
```

## Architecture

```
csv_parser.hpp   → RFC 4180 state-machine parser (quoted fields, escaped quotes, embedded newlines)
index.hpp        → HashIndex (O(1) exact match) + SortedIndex (O(log n) range queries)
query.hpp        → Tokenizer → Recursive-descent parser → AST → Executor with index selection
main.cpp         → REPL + benchmark harness
```

## Query Grammar

```
SELECT cols|* FROM table [WHERE col op val [AND ...]] [ORDER BY col [ASC|DESC]] [LIMIT n]
```

Operators: `=`, `!=`, `<`, `<=`, `>`, `>=`. Values: `'quoted string'` or unquoted number.

## REPL Commands

| Command  | Effect                              |
|----------|-------------------------------------|
| `schema` | Show column names and indices        |
| `bench`  | Run scan-vs-index benchmark          |
| `quit`   | Exit                                 |

## Teaching Points

### 1. CSV Parsing
- **State machine** with 4 states handles all RFC 4180 edge cases
- Trade-off: eager parse (entire file in memory) vs streaming iterator
- Production alternative: memory-map file + store (offset, length) per cell instead of `std::string`

### 2. Indexing
- **HashIndex**: `unordered_multimap<string, row_idx>` — O(1) lookup, no range support
- **SortedIndex**: sorted `(value, row_idx)` vector — O(log n) via `lower_bound`/`upper_bound`, supports range queries
- Trade-off: build cost + memory vs query speedup. Indices only pay off beyond ~1000 rows

### 3. Query Engine
- Hand-written **recursive descent** parser — no generator dependencies
- Executor checks available indices per predicate column; falls back to full scan
- Multi-predicate WHERE: intersect candidate sets from each predicate

### 4. Performance
- Run `bench` in REPL to see scan vs indexed timings
- `ExecStats` reports rows_scanned and index_lookups per query
- At 10 rows, overhead dominates. Generate 100K+ rows to see real differences

## Extension Ideas
- Add `OR` support (requires union instead of intersection)
- Add `LIKE` with glob matching
- Implement a B-tree index
- Add `JOIN` across two CSV files
- Stream-parse with coroutines (`co_yield` rows)
- Memory-mapped file backend
