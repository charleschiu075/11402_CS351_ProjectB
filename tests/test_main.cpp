// Self-contained smoke tests. Build:
//   g++ -std=c++17 -Wall -Wextra -O2 -Isrc tests/test_main.cpp -o build/tests
#include "../src/Tokenizer.hpp"
#include "../src/Parser.hpp"
#include "../src/Database.hpp"
#include "../src/Executor.hpp"
#include "../src/CsvLoader.hpp"
#include "../src/Formatter.hpp"

#include <cassert>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <variant>

using namespace csvdb;

static int failures = 0;
#define CHECK(cond) do { \
    if (!(cond)) { ++failures; std::cerr << "FAIL " << __FILE__ << ":" << __LINE__ << " " #cond "\n"; } \
} while (0)

static void test_csv_basic() {
    std::string csv = "id,name,age\n1,alice,30\n2,bob,\n3,\"o,k\",25\n";
    auto d = CsvLoader::parse(csv);
    CHECK(d.headers.size() == 3);
    CHECK(d.headers[0] == "id");
    CHECK(d.rows.size() == 3);
    CHECK(d.rows[0][1].value() == "alice");
    CHECK(!d.rows[1][2].has_value());        // empty unquoted -> NULL
    CHECK(d.rows[2][1].value() == "o,k");    // quoted comma preserved
}

static void test_csv_quote_escape() {
    std::string csv = "x\n\"a\"\"b\"\n\"\"\n";
    auto d = CsvLoader::parse(csv);
    CHECK(d.rows.size() == 2);
    CHECK(d.rows[0][0].value() == "a\"b");
    CHECK(d.rows[1][0].value() == "");       // quoted empty != NULL
}

static void test_tokenizer() {
    Tokenizer tk("SELECT a, b FROM t WHERE a = 'x' AND b != 3;");
    auto v = tk.tokenize();
    CHECK(v[0].kind == TokKind::KwSelect);
    CHECK(v[1].kind == TokKind::Ident && v[1].text == "a");
    CHECK(v[3].kind == TokKind::Ident && v[3].text == "b");
    CHECK(v[5].kind == TokKind::Ident && v[5].text == "t");
    CHECK(v[7].kind == TokKind::Ident && v[7].text == "a");
    CHECK(v[8].kind == TokKind::Eq);
    CHECK(v[9].kind == TokKind::StringLit && v[9].text == "x");
    CHECK(v[10].kind == TokKind::KwAnd);
    CHECK(v[12].kind == TokKind::Neq);
    CHECK(v[13].kind == TokKind::NumLit && v[13].text == "3");
    CHECK(v[14].kind == TokKind::Semicolon);
}

static void test_parser_select() {
    Tokenizer tk("SELECT id, name FROM users WHERE age > 21 OR name = 'x';");
    Parser p(tk.tokenize());
    auto st = p.parseStatement();
    auto& s = std::get<SelectStmt>(st);
    CHECK(!s.star);
    CHECK(s.columns.size() == 2);
    CHECK(s.table == "users");
    CHECK(s.where && s.where->kind == PredKind::Or);
}

static void test_parser_create_index() {
    Tokenizer tk("CREATE INDEX ON users(id);");
    Parser p(tk.tokenize());
    auto st = p.parseStatement();
    auto& s = std::get<CreateIndexStmt>(st);
    CHECK(s.table == "users" && s.column == "id");
}

static void write_file(const std::string& path, const std::string& content) {
    std::ofstream o(path); o << content;
}

static void test_executor_filtering_and_index() {
    std::string path = "/tmp/csvdb_users.csv";
    write_file(path,
        "id,name,age\n"
        "1,alice,30\n"
        "2,bob,25\n"
        "3,carol,\n"     // NULL age
        "4,bob,40\n");

    Database db;
    db.load("users", path);

    // SELECT *
    {
        Tokenizer tk("SELECT * FROM users;");
        Parser p(tk.tokenize());
        Executor ex(db);
        auto rs = ex.executeSelect(std::get<SelectStmt>(p.parseStatement()));
        CHECK(rs.rows.size() == 4);
    }
    // WHERE equality without index -> full scan
    {
        Tokenizer tk("SELECT id FROM users WHERE name = 'bob';");
        Parser p(tk.tokenize());
        Executor ex(db);
        auto rs = ex.executeSelect(std::get<SelectStmt>(p.parseStatement()));
        CHECK(rs.rows.size() == 2);
    }
    // IS NULL / IS NOT NULL
    {
        Tokenizer tk("SELECT name FROM users WHERE age IS NULL;");
        Parser p(tk.tokenize());
        Executor ex(db);
        auto rs = ex.executeSelect(std::get<SelectStmt>(p.parseStatement()));
        CHECK(rs.rows.size() == 1);
        CHECK(rs.rows[0][0].value() == "carol");
    }
    // NULL never satisfies comparison
    {
        Tokenizer tk("SELECT name FROM users WHERE age > '20';");
        Parser p(tk.tokenize());
        Executor ex(db);
        auto rs = ex.executeSelect(std::get<SelectStmt>(p.parseStatement()));
        // alice 30, bob 25, bob 40 -- carol excluded (NULL)
        CHECK(rs.rows.size() == 3);
    }
    // Build index, query through it
    {
        db.createIndex("users", "name");
        Tokenizer tk("SELECT id FROM users WHERE name = 'bob' AND age = '40';");
        Parser p(tk.tokenize());
        Executor ex(db);
        auto rs = ex.executeSelect(std::get<SelectStmt>(p.parseStatement()));
        CHECK(rs.rows.size() == 1);
        CHECK(rs.rows[0][0].value() == "4");
    }
    // Unknown column -> error
    {
        Tokenizer tk("SELECT id FROM users WHERE nope = 'x';");
        Parser p(tk.tokenize());
        Executor ex(db);
        bool threw = false;
        try { ex.executeSelect(std::get<SelectStmt>(p.parseStatement())); }
        catch (const std::exception&) { threw = true; }
        CHECK(threw);
    }
}

static void test_formatter_csv_roundtrip() {
    ResultSet rs;
    rs.columns = {"a", "b"};
    rs.rows.push_back({ Cell{"x,y"}, std::nullopt });
    rs.rows.push_back({ Cell{"q\"r"}, Cell{"z"} });
    std::ostringstream os;
    Formatter::printCsv(os, rs);
    std::string s = os.str();
    CHECK(s.find("\"x,y\",\n") != std::string::npos);
    CHECK(s.find("\"q\"\"r\",z\n") != std::string::npos);
}

int main() {
    test_csv_basic();
    test_csv_quote_escape();
    test_tokenizer();
    test_parser_select();
    test_parser_create_index();
    test_executor_filtering_and_index();
    test_formatter_csv_roundtrip();
    if (failures == 0) std::cout << "OK\n";
    else                std::cout << failures << " FAILURES\n";
    return failures == 0 ? 0 : 1;
}
