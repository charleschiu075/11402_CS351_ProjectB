// main.cpp — Demo driver for csvdb
// BUILD: g++ -std=c++17 -O2 main.cpp -o csvdb
#include "csvdb.hpp"
#include <cassert>

using namespace csvdb;

// --- Unit tests for the CSV parser ---
void test_parser() {
    CsvParser p;

    // Basic
    auto r = p.parse_row("a,b,c");
    assert(r.size() == 3 && r[0] == "a" && r[2] == "c");

    // Quoted fields with embedded commas
    r = p.parse_row("\"hello, world\",b,c");
    assert(r[0] == "hello, world");

    // Escaped quotes
    r = p.parse_row("\"she said \"\"hi\"\"\",b");
    assert(r[0] == "she said \"hi\"");

    // Embedded newlines (via stream)
    std::istringstream ss("name,bio\nAlice,\"line1\nline2\"\nBob,simple");
    auto rows = p.parse_stream(ss);
    assert(rows.size() == 3);           // header + 2 data rows
    assert(rows[1][1] == "line1\nline2");

    // Empty fields
    r = p.parse_row(",,,");
    assert(r.size() == 4);
    for (auto& f : r) assert(f.empty());

    std::cout << "Parser tests passed.\n";
}

// --- Functional demo ---
void demo() {
    // Sample dataset: employees
    const char* csv = R"(id,name,department,salary,city
1,Alice,Engineering,120000,Seattle
2,Bob,Engineering,115000,Portland
3,Carol,Marketing,95000,Seattle
4,Dave,Marketing,88000,Denver
5,Eve,Engineering,130000,Seattle
6,Frank,Sales,78000,Portland
7,Grace,Engineering,125000,Denver
8,Heidi,Sales,82000,Seattle
9,Ivan,Marketing,91000,Portland
10,Judy,Engineering,118000,Seattle)";

    Database db;
    db.load_csv_string(csv);

    std::cout << "\n=== Query 1: SELECT * WHERE department = 'Engineering' ===\n";
    db.execute("SELECT * WHERE department = 'Engineering'").print();

    std::cout << "\n=== Query 2: Top earners (salary > 100000, ordered) ===\n";
    db.execute("SELECT name, salary, city WHERE salary > 100000 ORDER BY salary DESC").print();

    std::cout << "\n=== Query 3: Full scan — Seattle employees ===\n";
    db.execute("SELECT name, department WHERE city = 'Seattle'").print();

    // Now create an index and re-run
    std::cout << "\n=== Creating index on 'city' ===\n";
    db.create_index("city");

    std::cout << "\n=== Query 4: Index scan — Seattle employees ===\n";
    db.execute("SELECT name, department WHERE city = 'Seattle'").print();

    std::cout << "\n=== Query 5: Multi-predicate — Engineering in Seattle ===\n";
    db.create_index("department");
    db.execute("SELECT name, salary WHERE department = 'Engineering' AND city = 'Seattle' ORDER BY salary DESC").print();

    std::cout << "\n=== Query 6: LIMIT ===\n";
    db.execute("SELECT * ORDER BY salary DESC LIMIT 3").print();
}

// --- Benchmark: index vs full scan ---
void benchmark() {
    // Generate a larger dataset
    std::ostringstream oss;
    oss << "id,category,value\n";
    const char* cats[] = {"A", "B", "C", "D", "E"};
    for (int i = 0; i < 100000; ++i) {
        oss << i << "," << cats[i % 5] << "," << (i * 7 % 1000) << "\n";
    }

    Database db;
    db.load_csv_string(oss.str());

    std::cout << "\n=== Benchmark: 100K rows ===\n";

    // Full scan
    auto r1 = db.execute("SELECT * WHERE category = 'C'");
    std::cout << "Full scan:  " << r1.exec_time_ms << " ms, "
              << r1.rows.size() << " rows  [" << r1.plan << "]\n";

    // With index
    db.create_index("category");
    auto r2 = db.execute("SELECT * WHERE category = 'C'");
    std::cout << "Index scan: " << r2.exec_time_ms << " ms, "
              << r2.rows.size() << " rows  [" << r2.plan << "]\n";

    double speedup = r1.exec_time_ms / r2.exec_time_ms;
    std::cout << "Speedup: " << std::fixed << std::setprecision(1) << speedup << "x\n";
}

int main() {
    test_parser();
    demo();
    benchmark();
    return 0;
}
