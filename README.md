# csvdb

Minimal in-memory CSV mini-database with a SQL-subset REPL. C++17, STL only.

## Build

```
mkdir build && cd build
cmake ..
cmake --build .
./csvdb
```

Or directly:
```
g++ -std=c++17 -O2 -Wall -Wextra -Isrc src/main.cpp -o csvdb
```

Tests:
```
g++ -std=c++17 -O2 -Wall -Wextra -Isrc tests/test_main.cpp -o tests && ./tests
```

## REPL commands

```
.load <path> <table>      load a CSV (row 0 = headers) into <table>
.tables                   list tables
.schema <table>           columns + indices
.mode table|csv           output format (default: table)
.help / .exit
```

Optional preload from CLI: `./csvdb --load=/path/file.csv:tablename`

## SQL subset

```
SELECT <cols|*> FROM <table> [WHERE <pred>] ;
CREATE INDEX ON <table>(<col>) ;
```

Predicates:
- `col <op> 'literal'` where `<op>` is `= != <> < <= > >=`
- Numeric literals also accepted on RHS but stored/compared as strings
- `col IS NULL` / `col IS NOT NULL`
- Combine with `AND`, `OR`, parentheses
- Identifiers may be `"quoted"` to allow keywords/spaces; case-sensitive
- Keywords case-insensitive

## Architecture

```
Tokenizer  -> Token[]        (state machine over chars, handles 'lit' and "ident")
Parser     -> Statement AST  (recursive descent; operator precedence: OR < AND < primary)
CsvLoader  -> CsvData        (RFC 4180 state machine: "" escape, quoted comma/CRLF)
Database   -> Tables         (in-memory, hash-based column lookup, hash-based indices)
Executor   -> ResultSet      (index-aware planner for top-level AND chains)
Formatter  -> stdout         (aligned table or CSV output)
```

### Index planner

For `WHERE` trees that decompose into a flat top-level `AND` of conjuncts, the
executor scans the conjuncts for an indexable equality (`col = lit` where an
index exists on `col`), probes the index, then evaluates remaining predicates
against the candidate row set. Any other shape (top-level `OR`, no equality
predicate, no matching index) falls back to a full scan. NULL values are not
indexed, so `IS NULL` is always a full scan.

## Known limitations / design decisions

- **All values are strings.** Comparisons (`<`, `>`, etc.) are **lexicographic**, not
  numeric. `'95000' > '100000'` is true because `'9' > '1'`. Pad numeric data
  to fixed width if you need ordering, or wait for typed columns.
- **NULL semantics**: empty *unquoted* CSV field => NULL; empty *quoted* (`""`)
  => empty string. NULL never satisfies any comparison; use `IS NULL`.
- **No mutations** (`INSERT`/`UPDATE`/`DELETE`) — reload CSV to refresh.
- **No persistence** — tables and indices live only for the REPL session.
- **No `JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT`, aggregates, expressions** in
  v1. Predicate RHS must be a literal, not a column or expression.
- **Index is equality-only.** Range predicates always full-scan.
- **Duplicate header names in CSV are rejected** at load time.
- **Row width**: rows shorter than the header are NULL-padded, longer rows are
  truncated.

## Extension points

- Typed columns: add a `Type` enum + `Value` variant; promote `Cell` from
  `optional<string>` to `optional<Value>`. Comparisons become type-directed.
- Range index: replace `unordered_multimap` with `multimap` (or sorted vector
  with binary search); planner recognizes `<`, `<=`, `>`, `>=` as indexable.
- `ORDER BY` / `LIMIT`: post-processing pass on `ResultSet`.
- `JOIN`: nested-loop first; hash-join when an equality join key is indexed.
- Persistence: serialize `Database` (headers + rows + index metadata) to a
  binary file; rebuild indices lazily on load if integrity check fails.
