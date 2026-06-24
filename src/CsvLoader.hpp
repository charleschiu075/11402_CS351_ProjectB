#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <optional>

namespace csvdb {

// Cell: empty optional => NULL (empty unquoted field), otherwise the string.
// Convention: "" (empty quoted) is empty string, NOT NULL.
using Cell = std::optional<std::string>;
using Row  = std::vector<Cell>;

struct CsvData {
    std::vector<std::string> headers;
    std::vector<Row> rows;
};

class CsvLoader {
public:
    static CsvData load(const std::string& path) {
        std::ifstream in(path, std::ios::binary);
        if (!in) throw std::runtime_error("cannot open file: " + path);
        std::string content((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
        return parse(content);
    }

    static CsvData parse(const std::string& s) {
        std::vector<std::vector<Cell>> raw;
        std::vector<Cell> row;
        std::string field;
        bool in_quotes = false;
        bool field_was_quoted = false;
        size_t i = 0;
        const size_t n = s.size();

        auto flush_field = [&]() {
            if (!field_was_quoted && field.empty()) {
                row.push_back(std::nullopt);  // NULL
            } else {
                row.push_back(field);
            }
            field.clear();
            field_was_quoted = false;
        };
        auto flush_row = [&]() {
            flush_field();
            raw.push_back(std::move(row));
            row.clear();
        };

        while (i < n) {
            char c = s[i];
            if (in_quotes) {
                if (c == '"') {
                    if (i+1 < n && s[i+1] == '"') { field += '"'; i += 2; continue; }
                    in_quotes = false; ++i; continue;
                }
                field += c; ++i;
            } else {
                if (c == '"' && field.empty()) {
                    in_quotes = true; field_was_quoted = true; ++i;
                }
                else if (c == ',') { flush_field(); ++i; }
                else if (c == '\r') {
                    if (i+1 < n && s[i+1] == '\n') { flush_row(); i += 2; }
                    else { flush_row(); ++i; }
                }
                else if (c == '\n') { flush_row(); ++i; }
                else { field += c; ++i; }
            }
        }
        // trailing field/row if file did not end with newline
        if (!field.empty() || !row.empty() || field_was_quoted) {
            flush_row();
        }
        if (in_quotes) throw std::runtime_error("unterminated quoted field in CSV");

        CsvData out;
        if (raw.empty()) return out;
        // Header row -- coerce to strings (NULL header becomes empty string)
        for (auto& c : raw[0]) out.headers.push_back(c.value_or(""));
        out.rows.reserve(raw.size() - 1);
        for (size_t r = 1; r < raw.size(); ++r) {
            // skip fully-empty trailing rows (single null field, no real data)
            if (raw[r].size() == 1 && !raw[r][0].has_value()) continue;
            // pad / truncate to header width
            raw[r].resize(out.headers.size(), std::nullopt);
            out.rows.push_back(std::move(raw[r]));
        }
        return out;
    }
};

} // namespace csvdb
