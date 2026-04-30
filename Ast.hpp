#pragma once
#include <string>
#include <vector>
#include <memory>
#include <variant>
#include "Tokenizer.hpp"

namespace csvdb {

enum class CmpOp { Eq, Neq, Lt, Lte, Gt, Gte };

struct Predicate;
using PredPtr = std::unique_ptr<Predicate>;

enum class PredKind { Cmp, IsNull, IsNotNull, And, Or };

// Cmp:        col <op> literal
// IsNull:     col IS NULL
// IsNotNull:  col IS NOT NULL
// And/Or:     left, right children
struct Predicate {
    PredKind kind;
    // Cmp / IsNull / IsNotNull
    std::string column;
    CmpOp op = CmpOp::Eq;
    std::string literal;
    // And / Or
    PredPtr left;
    PredPtr right;
};

struct SelectStmt {
    std::vector<std::string> columns; // empty => SELECT *
    bool star = false;
    std::string table;
    PredPtr where; // may be null
};

struct CreateIndexStmt {
    std::string table;
    std::string column;
};

using Statement = std::variant<SelectStmt, CreateIndexStmt>;

} // namespace csvdb
