#pragma once
#include "CsvLoader.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include <stdexcept>

namespace csvdb {

struct Table {
    std::vector<std::string> headers;
    std::unordered_map<std::string, size_t> col_index; // header -> column position
    std::vector<Row> rows;
    // column name -> (value -> list of row indices). NULL not indexed.
    std::unordered_map<std::string, std::unordered_multimap<std::string, size_t>> indices;

    size_t col(const std::string& name) const {
        auto it = col_index.find(name);
        if (it == col_index.end()) throw std::runtime_error("unknown column: " + name);
        return it->second;
    }
};

class Database {
public:
    void load(const std::string& table_name, const std::string& csv_path) {
        CsvData d = CsvLoader::load(csv_path);
        Table t;
        t.headers = std::move(d.headers);
        for (size_t i = 0; i < t.headers.size(); ++i) {
            if (t.col_index.count(t.headers[i]))
                throw std::runtime_error("duplicate column in CSV: " + t.headers[i]);
            t.col_index[t.headers[i]] = i;
        }
        t.rows = std::move(d.rows);
        tables_[table_name] = std::move(t);
    }

    bool hasTable(const std::string& name) const { return tables_.count(name) > 0; }
    Table& table(const std::string& name) {
        auto it = tables_.find(name);
        if (it == tables_.end()) throw std::runtime_error("unknown table: " + name);
        return it->second;
    }
    const Table& table(const std::string& name) const {
        auto it = tables_.find(name);
        if (it == tables_.end()) throw std::runtime_error("unknown table: " + name);
        return it->second;
    }
    const std::unordered_map<std::string, Table>& tables() const { return tables_; }

    void createIndex(const std::string& table_name, const std::string& column) {
        Table& t = table(table_name);
        size_t c = t.col(column);
        if (t.indices.count(column)) return; // already exists
        std::unordered_multimap<std::string, size_t> idx;
        idx.reserve(t.rows.size());
        for (size_t r = 0; r < t.rows.size(); ++r) {
            const Cell& v = t.rows[r][c];
            if (v.has_value()) idx.emplace(*v, r);
        }
        t.indices.emplace(column, std::move(idx));
    }

private:
    std::unordered_map<std::string, Table> tables_;
};

} // namespace csvdb
