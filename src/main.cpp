#include "Tokenizer.hpp"
#include "Parser.hpp"
#include "Database.hpp"
#include "Executor.hpp"
#include "Formatter.hpp"

#include <iostream>
#include <sstream>
#include <string>
#include <variant>

using namespace csvdb;

namespace {

std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

std::vector<std::string> splitWhitespace(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream is(s);
    std::string tok;
    while (is >> tok) out.push_back(tok);
    return out;
}

void printHelp() {
    std::cout <<
        "Commands:\n"
        "  .load <path> <table>      load a CSV (row 0 = headers) into <table>\n"
        "  .tables                   list tables\n"
        "  .schema <table>           show columns + indices for <table>\n"
        "  .mode table|csv           set output format (default: table)\n"
        "  .help                     this message\n"
        "  .exit                     quit\n"
        "SQL:\n"
        "  SELECT <cols|*> FROM <table> [WHERE <pred>] ;\n"
        "  CREATE INDEX ON <table>(<col>) ;\n"
        "Predicates: col <op> 'literal' | col IS [NOT] NULL ; combine with AND / OR ; ()\n"
        "Operators:  =  !=  <>  <  <=  >  >=\n";
}

bool handleDot(const std::string& line, Database& db, std::string& mode) {
    auto parts = splitWhitespace(line);
    if (parts.empty()) return true;
    const std::string& cmd = parts[0];
    if (cmd == ".exit" || cmd == ".quit") return false;
    if (cmd == ".help")  { printHelp(); return true; }
    if (cmd == ".mode") {
        if (parts.size() != 2 || (parts[1] != "table" && parts[1] != "csv")) {
            std::cout << "usage: .mode table|csv\n"; return true;
        }
        mode = parts[1];
        std::cout << "mode = " << mode << "\n";
        return true;
    }
    if (cmd == ".load") {
        if (parts.size() != 3) { std::cout << "usage: .load <path> <table>\n"; return true; }
        try {
            // .load <path> <table> ; Database::load(table_name, csv_path)
            db.load(parts[2], parts[1]);
        } catch (const std::exception& e) { std::cout << "error: " << e.what() << "\n"; }
        return true;
    }
    if (cmd == ".tables") {
        for (const auto& kv : db.tables()) std::cout << kv.first << "\n";
        return true;
    }
    if (cmd == ".schema") {
        if (parts.size() != 2) { std::cout << "usage: .schema <table>\n"; return true; }
        try {
            const Table& t = db.table(parts[1]);
            std::cout << "table " << parts[1] << " (" << t.rows.size() << " rows)\n";
            for (const auto& h : t.headers) {
                std::cout << "  " << h;
                if (t.indices.count(h)) std::cout << "  [indexed]";
                std::cout << "\n";
            }
        } catch (const std::exception& e) { std::cout << "error: " << e.what() << "\n"; }
        return true;
    }
    std::cout << "unknown command: " << cmd << " (try .help)\n";
    return true;
}

void runStatement(const std::string& sql, Database& db, const std::string& mode) {
    try {
        Tokenizer tk(sql);
        auto tokens = tk.tokenize();
        Parser p(std::move(tokens));
        Statement st = p.parseStatement();
        Executor ex(db);
        if (std::holds_alternative<SelectStmt>(st)) {
            ResultSet rs = ex.executeSelect(std::get<SelectStmt>(st));
            if (mode == "csv") Formatter::printCsv(std::cout, rs);
            else               Formatter::printTable(std::cout, rs);
        } else {
            ex.executeCreateIndex(std::get<CreateIndexStmt>(st));
            std::cout << "OK\n";
        }
    } catch (const std::exception& e) {
        std::cout << "error: " << e.what() << "\n";
    }
}

} // namespace

int main(int argc, char** argv) {
    Database db;
    std::string mode = "table";

    // Optional: preload tables from CLI: --load path:name
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a.rfind("--load=", 0) == 0) {
            std::string spec = a.substr(7);
            auto colon = spec.find(':');
            if (colon == std::string::npos) {
                std::cerr << "bad --load spec, expected path:name\n"; return 1;
            }
            try { db.load(spec.substr(colon+1), spec.substr(0, colon)); }
            catch (const std::exception& e) { std::cerr << "error: " << e.what() << "\n"; return 1; }
        }
    }

    std::cout << "csvdb v0.1  (.help for commands)\n";
    std::string buf;
    while (true) {
        std::cout << (buf.empty() ? "csvdb> " : "  ...> ") << std::flush;
        std::string line;
        if (!std::getline(std::cin, line)) { std::cout << "\n"; break; }

        std::string trimmed = trim(line);
        if (buf.empty() && !trimmed.empty() && trimmed[0] == '.') {
            if (!handleDot(trimmed, db, mode)) break;
            continue;
        }
        if (!buf.empty()) buf += " ";
        buf += line;
        // statements terminate on ';'
        size_t semi = buf.find(';');
        if (semi != std::string::npos) {
            std::string stmt = buf.substr(0, semi + 1);
            std::string rest = buf.substr(semi + 1);
            std::string s = trim(stmt);
            if (!s.empty() && s != ";") runStatement(s, db, mode);
            buf = trim(rest);
        }
    }
    return 0;
}
