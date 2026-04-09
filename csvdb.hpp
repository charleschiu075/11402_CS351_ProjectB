#pragma once
// csvdb.hpp — Teaching-oriented CSV mini database & query engine
// Covers: CSV parsing, indexing, query grammar, performance trade-offs
// BUILD: g++ -std=c++17 -O2 main.cpp -o csvdb

#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <functional>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <cassert>
#include <numeric>
#include <iomanip>
#include <memory>

namespace csvdb {

// ============================================================================
// PART 1: CSV PARSING
// ============================================================================
// Design decision: hand-rolled parser vs. library.
//
// Hand-rolled pros:
//   - Zero dependencies, single header
//   - Full control over error handling and edge cases
//   - Educational value (understanding RFC 4180)
//
// Library (e.g., fast-cpp-csv-parser, rapidcsv) pros:
//   - Battle-tested, handles exotic encodings
//   - SIMD-accelerated parsing in some libs
//
// Trade-off: For < 1GB files, a correct hand-rolled parser is fine.
// Beyond that, consider memory-mapped I/O + SIMD (simdjson-style).
// ============================================================================

using Row = std::vector<std::string>;

/// RFC 4180-compliant CSV parser.
/// Handles: quoted fields, embedded commas, embedded newlines,
/// escaped quotes (""), leading/trailing whitespace in quoted fields.
class CsvParser {
public:
    explicit CsvParser(char delimiter = ',', char quote = '"')
        : delim_(delimiter), quote_(quote) {}

    std::vector<Row> parse_file(const std::string& path) {
        std::ifstream ifs(path);
        if (!ifs) throw std::runtime_error("Cannot open: " + path);
        return parse_stream(ifs);
    }

    std::vector<Row> parse_stream(std::istream& in) {
        std::vector<Row> rows;
        std::string line, accumulated;
        bool in_quotes = false;

        while (std::getline(in, line)) {
            if (in_quotes) {
                accumulated += '\n';
                accumulated += line;
            } else {
                accumulated = line;
            }
            in_quotes = (count_quotes(accumulated) % 2 != 0);
            if (in_quotes) continue;
            rows.push_back(parse_row(accumulated));
        }
        if (in_quotes)
            throw std::runtime_error("Unterminated quoted field at EOF");
        return rows;
    }

    Row parse_row(const std::string& line) {
        Row fields;
        std::string field;
        bool in_q = false;
        size_t i = 0;

        while (i < line.size()) {
            char c = line[i];
            if (in_q) {
                if (c == quote_) {
                    if (i + 1 < line.size() && line[i + 1] == quote_) {
                        field += quote_;
                        i += 2;
                    } else {
                        in_q = false;
                        ++i;
                    }
                } else {
                    field += c;
                    ++i;
                }
            } else {
                if (c == quote_ && field.empty()) {
                    in_q = true;
                    ++i;
                } else if (c == delim_) {
                    fields.push_back(std::move(field));
                    field.clear();
                    ++i;
                } else {
                    field += c;
                    ++i;
                }
            }
        }
        fields.push_back(std::move(field));
        return fields;
    }

private:
    char delim_, quote_;
    size_t count_quotes(const std::string& s) {
        return std::count(s.begin(), s.end(), quote_);
    }
};

// ============================================================================
// PART 2: TABLE STORAGE
// ============================================================================
// Row-major (vector<Row>) chosen for simplicity.
// Column-major would be better for analytical scans (contiguous memory,
// SIMD-friendly). Production analytical DBs (DuckDB, ClickHouse) use columnar.
// ============================================================================

enum class FieldType { STRING, INTEGER, FLOAT };

inline FieldType infer_type(const std::string& val) {
    if (val.empty()) return FieldType::STRING;
    char* end = nullptr;
    std::strtol(val.c_str(), &end, 10);
    if (end == val.c_str() + val.size()) return FieldType::INTEGER;
    std::strtod(val.c_str(), &end);
    if (end == val.c_str() + val.size()) return FieldType::FLOAT;
    return FieldType::STRING;
}

struct Table {
    Row header;
    std::vector<Row> rows;
    std::unordered_map<std::string, size_t> col_index;

    void build_col_index() {
        col_index.clear();
        for (size_t i = 0; i < header.size(); ++i)
            col_index[header[i]] = i;
    }

    static Table from_rows(std::vector<Row> all_rows) {
        if (all_rows.empty()) throw std::runtime_error("Empty CSV");
        Table t;
        t.header = std::move(all_rows[0]);
        t.rows.assign(std::make_move_iterator(all_rows.begin() + 1),
                       std::make_move_iterator(all_rows.end()));
        t.build_col_index();
        return t;
    }

    size_t resolve_col(const std::string& name) const {
        auto it = col_index.find(name);
        if (it == col_index.end())
            throw std::runtime_error("Unknown column: " + name);
        return it->second;
    }
};

// ============================================================================
// PART 3: INDEXING
// ============================================================================
// Hash index:  O(1) exact match, no range queries
// Sorted index: O(log n) lookup + range queries (BETWEEN, >=, <=)
// Bitmap index: ideal for low-cardinality cols (not implemented here)
// ============================================================================

class HashIndex {
public:
    void build(const Table& table, const std::string& column) {
        col_idx_ = table.resolve_col(column);
        col_name_ = column;
        index_.clear();
        index_.reserve(table.rows.size());
        for (size_t i = 0; i < table.rows.size(); ++i)
            index_[table.rows[i][col_idx_]].push_back(i);
    }

    const std::vector<size_t>* lookup(const std::string& value) const {
        auto it = index_.find(value);
        return (it != index_.end()) ? &it->second : nullptr;
    }

    size_t memory_estimate() const {
        size_t bytes = sizeof(*this);
        for (auto& [k, v] : index_)
            bytes += k.capacity() + v.capacity() * sizeof(size_t) + 64;
        return bytes;
    }

    const std::string& column_name() const { return col_name_; }

private:
    size_t col_idx_ = 0;
    std::string col_name_;
    std::unordered_map<std::string, std::vector<size_t>> index_;
};

// ============================================================================
// PART 4: QUERY ENGINE
// ============================================================================
// Grammar (minimal SQL subset):
//   SELECT fields FROM tbl [WHERE col op val [AND ...]] [ORDER BY col [ASC|DESC]] [LIMIT n]
//   op := = | != | < | > | <= | >=
//   value := 'quoted_string' | number
// ============================================================================

enum class Op { EQ, NEQ, LT, GT, LTE, GTE };

struct Predicate {
    std::string column;
    Op op;
    std::string value;

    bool eval(const Row& row, size_t col_idx) const {
        const std::string& field = row[col_idx];
        FieldType ft = infer_type(field), vt = infer_type(value);
        if ((ft == FieldType::INTEGER || ft == FieldType::FLOAT) &&
            (vt == FieldType::INTEGER || vt == FieldType::FLOAT)) {
            return compare(std::stod(field), std::stod(value));
        }
        return compare(field, value);
    }

private:
    template <typename T>
    bool compare(const T& a, const T& b) const {
        switch (op) {
            case Op::EQ:  return a == b;
            case Op::NEQ: return a != b;
            case Op::LT:  return a < b;
            case Op::GT:  return a > b;
            case Op::LTE: return a <= b;
            case Op::GTE: return a >= b;
        }
        return false;
    }
};

struct Query {
    std::vector<std::string> select_cols;
    bool select_all = false;
    std::vector<Predicate> predicates;
    std::string order_by;
    bool order_desc = false;
    int limit = -1;
};

class QueryParser {
public:
    Query parse(const std::string& input) {
        src_ = input; pos_ = 0;
        Query q;
        expect_keyword("SELECT");
        parse_select(q);
        if (peek_keyword("FROM")) { expect_keyword("FROM"); skip_word(); }
        if (peek_keyword("WHERE")) { expect_keyword("WHERE"); parse_where(q); }
        if (peek_keyword("ORDER")) { expect_keyword("ORDER"); expect_keyword("BY"); parse_order(q); }
        if (peek_keyword("LIMIT")) { expect_keyword("LIMIT"); q.limit = parse_int(); }
        return q;
    }

private:
    std::string src_;
    size_t pos_ = 0;

    void skip_ws() { while (pos_ < src_.size() && std::isspace(src_[pos_])) ++pos_; }

    std::string peek_word_upper() {
        skip_ws();
        size_t s = pos_;
        while (s < src_.size() && !std::isspace(src_[s]) && src_[s] != ','
               && src_[s] != '=' && src_[s] != '<' && src_[s] != '>' && src_[s] != '!') ++s;
        std::string w = src_.substr(pos_, s - pos_);
        std::transform(w.begin(), w.end(), w.begin(), ::toupper);
        return w;
    }

    bool peek_keyword(const std::string& kw) { return peek_word_upper() == kw; }

    std::string read_word() {
        skip_ws();
        size_t s = pos_;
        while (pos_ < src_.size() && !std::isspace(src_[pos_]) && src_[pos_] != ','
               && src_[pos_] != '=' && src_[pos_] != '<' && src_[pos_] != '>' && src_[pos_] != '!') ++pos_;
        return src_.substr(s, pos_ - s);
    }

    void skip_word() { read_word(); }

    void expect_keyword(const std::string& kw) {
        std::string w = read_word();
        std::string u = w;
        std::transform(u.begin(), u.end(), u.begin(), ::toupper);
        if (u != kw) throw std::runtime_error("Expected '" + kw + "', got '" + w + "'");
    }

    int parse_int() { return std::stoi(read_word()); }

    void parse_select(Query& q) {
        skip_ws();
        if (pos_ < src_.size() && src_[pos_] == '*') { q.select_all = true; ++pos_; return; }
        while (true) {
            q.select_cols.push_back(read_word());
            skip_ws();
            if (pos_ < src_.size() && src_[pos_] == ',') ++pos_; else break;
        }
    }

    Op parse_op() {
        skip_ws();
        if (src_[pos_] == '=' ) { ++pos_; return Op::EQ; }
        if (src_[pos_] == '!' && src_[pos_+1] == '=') { pos_ += 2; return Op::NEQ; }
        if (src_[pos_] == '<' && src_[pos_+1] == '=') { pos_ += 2; return Op::LTE; }
        if (src_[pos_] == '>' && src_[pos_+1] == '=') { pos_ += 2; return Op::GTE; }
        if (src_[pos_] == '<') { ++pos_; return Op::LT; }
        if (src_[pos_] == '>') { ++pos_; return Op::GT; }
        throw std::runtime_error("Expected operator at pos " + std::to_string(pos_));
    }

    std::string parse_value() {
        skip_ws();
        if (pos_ < src_.size() && src_[pos_] == '\'') {
            ++pos_;
            size_t s = pos_;
            while (pos_ < src_.size() && src_[pos_] != '\'') ++pos_;
            std::string val = src_.substr(s, pos_ - s);
            if (pos_ < src_.size()) ++pos_;
            return val;
        }
        return read_word();
    }

    void parse_where(Query& q) {
        while (true) {
            Predicate p;
            p.column = read_word();
            p.op = parse_op();
            p.value = parse_value();
            q.predicates.push_back(std::move(p));
            if (peek_keyword("AND")) expect_keyword("AND"); else break;
        }
    }

    void parse_order(Query& q) {
        q.order_by = read_word();
        if (peek_keyword("DESC")) { expect_keyword("DESC"); q.order_desc = true; }
        else if (peek_keyword("ASC")) { expect_keyword("ASC"); }
    }
};

// ============================================================================
// PART 5: EXECUTOR
// ============================================================================
// Strategy: index probe if first EQ predicate has an index, else full scan.
// Then filter remaining predicates, sort, limit, project.
// ============================================================================

struct QueryResult {
    Row header;
    std::vector<Row> rows;
    double exec_time_ms = 0;
    std::string plan;

    void print(std::ostream& out = std::cout, size_t max_w = 20) const {
        if (header.empty()) { out << "(empty result)\n"; return; }
        std::vector<size_t> widths(header.size());
        for (size_t i = 0; i < header.size(); ++i)
            widths[i] = std::min(header[i].size(), max_w);
        for (auto& row : rows)
            for (size_t i = 0; i < row.size() && i < widths.size(); ++i)
                widths[i] = std::max(widths[i], std::min(row[i].size(), max_w));

        auto sep = [&]() { out << "+"; for (auto w : widths) out << std::string(w+2, '-') << "+"; out << '\n'; };
        auto pr = [&](const Row& r) {
            out << "| ";
            for (size_t i = 0; i < r.size() && i < widths.size(); ++i)
                out << std::left << std::setw(widths[i]) << r[i].substr(0, max_w) << " | ";
            out << '\n';
        };
        sep(); pr(header); sep();
        for (auto& r : rows) pr(r);
        sep();
        out << rows.size() << " row(s) in " << std::fixed << std::setprecision(3)
            << exec_time_ms << " ms";
        if (!plan.empty()) out << "  [" << plan << "]";
        out << '\n';
    }
};

class Database {
public:
    void load_csv(const std::string& path, char delimiter = ',') {
        CsvParser parser(delimiter);
        table_ = Table::from_rows(parser.parse_file(path));
        indexes_.clear();
        std::cout << "Loaded " << table_.rows.size() << " rows, "
                  << table_.header.size() << " columns\n";
    }

    void load_csv_string(const std::string& csv_data, char delimiter = ',') {
        CsvParser parser(delimiter);
        std::istringstream ss(csv_data);
        table_ = Table::from_rows(parser.parse_stream(ss));
        indexes_.clear();
    }

    void create_index(const std::string& column) {
        auto idx = std::make_unique<HashIndex>();
        idx->build(table_, column);
        size_t mem = idx->memory_estimate();
        indexes_[column] = std::move(idx);
        std::cout << "Index on '" << column << "' (~" << mem/1024 << " KB)\n";
    }

    QueryResult execute(const std::string& query_str) {
        QueryParser parser;
        return execute_query(parser.parse(query_str));
    }

    const Table& table() const { return table_; }

private:
    Table table_;
    std::unordered_map<std::string, std::unique_ptr<HashIndex>> indexes_;

    QueryResult execute_query(const Query& q) {
        auto t0 = std::chrono::high_resolution_clock::now();
        std::vector<size_t> candidates;
        std::string plan;

        // Index probe
        int indexed_pred = -1;
        for (int i = 0; i < (int)q.predicates.size(); ++i) {
            if (q.predicates[i].op == Op::EQ && indexes_.count(q.predicates[i].column)) {
                indexed_pred = i;
                break;
            }
        }

        if (indexed_pred >= 0) {
            auto& pred = q.predicates[indexed_pred];
            auto* hits = indexes_[pred.column]->lookup(pred.value);
            if (hits) candidates = *hits;
            plan = "INDEX SCAN on " + pred.column;
        } else {
            candidates.resize(table_.rows.size());
            std::iota(candidates.begin(), candidates.end(), 0);
            plan = "FULL SCAN";
        }

        // Filter
        std::vector<size_t> filtered;
        filtered.reserve(candidates.size());
        for (size_t ri : candidates) {
            bool pass = true;
            for (int i = 0; i < (int)q.predicates.size(); ++i) {
                if (i == indexed_pred) continue;
                auto& p = q.predicates[i];
                if (!p.eval(table_.rows[ri], table_.resolve_col(p.column))) { pass = false; break; }
            }
            if (pass) filtered.push_back(ri);
        }

        // Sort
        if (!q.order_by.empty()) {
            size_t oi = table_.resolve_col(q.order_by);
            std::sort(filtered.begin(), filtered.end(), [&](size_t a, size_t b) {
                auto &va = table_.rows[a][oi], &vb = table_.rows[b][oi];
                auto ta = infer_type(va), tb = infer_type(vb);
                if ((ta == FieldType::INTEGER || ta == FieldType::FLOAT) &&
                    (tb == FieldType::INTEGER || tb == FieldType::FLOAT))
                    return q.order_desc ? std::stod(va) > std::stod(vb) : std::stod(va) < std::stod(vb);
                return q.order_desc ? va > vb : va < vb;
            });
        }

        // Limit
        if (q.limit >= 0 && (size_t)q.limit < filtered.size())
            filtered.resize(q.limit);

        // Project
        QueryResult result;
        std::vector<size_t> proj;
        if (q.select_all || q.select_cols.empty()) {
            result.header = table_.header;
            proj.resize(table_.header.size());
            std::iota(proj.begin(), proj.end(), 0);
        } else {
            for (auto& c : q.select_cols) {
                result.header.push_back(c);
                proj.push_back(table_.resolve_col(c));
            }
        }
        result.rows.reserve(filtered.size());
        for (size_t ri : filtered) {
            Row r; r.reserve(proj.size());
            for (size_t ci : proj) r.push_back(table_.rows[ri][ci]);
            result.rows.push_back(std::move(r));
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        result.exec_time_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        result.plan = plan;
        return result;
    }
};

} // namespace csvdb
