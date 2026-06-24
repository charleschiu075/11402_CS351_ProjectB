#pragma once
#include "Executor.hpp"
#include <iostream>
#include <string>
#include <vector>

namespace csvdb {

class Formatter {
public:
    static void printTable(std::ostream& os, const ResultSet& rs) {
        const size_t n = rs.columns.size();
        std::vector<size_t> w(n, 0);
        for (size_t i = 0; i < n; ++i) w[i] = rs.columns[i].size();
        for (const auto& row : rs.rows) {
            for (size_t i = 0; i < n; ++i) {
                size_t len = row[i].has_value() ? row[i]->size() : 4; // "NULL"
                if (len > w[i]) w[i] = len;
            }
        }
        auto sep = [&]() {
            os << '+';
            for (size_t i = 0; i < n; ++i) { os << std::string(w[i] + 2, '-') << '+'; }
            os << '\n';
        };
        sep();
        os << '|';
        for (size_t i = 0; i < n; ++i) {
            os << ' ' << rs.columns[i]
               << std::string(w[i] - rs.columns[i].size(), ' ') << " |";
        }
        os << '\n';
        sep();
        for (const auto& row : rs.rows) {
            os << '|';
            for (size_t i = 0; i < n; ++i) {
                std::string v = row[i].has_value() ? *row[i] : "NULL";
                os << ' ' << v << std::string(w[i] - v.size(), ' ') << " |";
            }
            os << '\n';
        }
        sep();
        os << "(" << rs.rows.size() << " row" << (rs.rows.size() == 1 ? "" : "s") << ")\n";
    }

    static void printCsv(std::ostream& os, const ResultSet& rs) {
        for (size_t i = 0; i < rs.columns.size(); ++i) {
            if (i) os << ',';
            os << escape(rs.columns[i]);
        }
        os << '\n';
        for (const auto& row : rs.rows) {
            for (size_t i = 0; i < row.size(); ++i) {
                if (i) os << ',';
                if (row[i].has_value()) os << escape(*row[i]);
                // NULL -> empty unquoted field
            }
            os << '\n';
        }
    }

private:
    static std::string escape(const std::string& v) {
        bool needs = v.find_first_of(",\"\r\n") != std::string::npos;
        if (!needs) return v;
        std::string out;
        out.reserve(v.size() + 2);
        out.push_back('"');
        for (char c : v) { if (c == '"') out.push_back('"'); out.push_back(c); }
        out.push_back('"');
        return out;
    }
};

} // namespace csvdb
