#pragma once
#include "Ast.hpp"
#include "Database.hpp"
#include <string>
#include <vector>
#include <stdexcept>
#include <unordered_set>

namespace csvdb {

struct ResultSet {
    std::vector<std::string> columns;
    std::vector<std::vector<Cell>> rows;
};

class Executor {
public:
    explicit Executor(Database& db) : db_(db) {}

    ResultSet executeSelect(const SelectStmt& s) {
        const Table& t = db_.table(s.table);

        // Resolve projected columns
        std::vector<size_t> proj;
        std::vector<std::string> proj_names;
        if (s.star) {
            for (size_t i = 0; i < t.headers.size(); ++i) {
                proj.push_back(i);
                proj_names.push_back(t.headers[i]);
            }
        } else {
            for (const auto& c : s.columns) {
                proj.push_back(t.col(c));
                proj_names.push_back(c);
            }
        }

        // Validate columns referenced in WHERE before scanning
        if (s.where) validateColumns(*s.where, t);

        // Plan: try to find an indexable equality conjunct in a top-level AND chain
        std::vector<size_t> candidates;
        bool used_index = false;
        if (s.where) {
            std::vector<const Predicate*> conjuncts;
            collectAndConjuncts(s.where.get(), conjuncts);
            for (const Predicate* p : conjuncts) {
                if (p->kind == PredKind::Cmp && p->op == CmpOp::Eq) {
                    auto it = t.indices.find(p->column);
                    if (it != t.indices.end()) {
                        auto range = it->second.equal_range(p->literal);
                        for (auto rit = range.first; rit != range.second; ++rit)
                            candidates.push_back(rit->second);
                        used_index = true;
                        break;
                    }
                }
            }
        }

        ResultSet out;
        out.columns = proj_names;

        auto eval_and_emit = [&](size_t r) {
            if (s.where && !evalPredicate(*s.where, t, r)) return;
            std::vector<Cell> row;
            row.reserve(proj.size());
            for (size_t c : proj) row.push_back(t.rows[r][c]);
            out.rows.push_back(std::move(row));
        };

        if (used_index) {
            for (size_t r : candidates) eval_and_emit(r);
        } else {
            for (size_t r = 0; r < t.rows.size(); ++r) eval_and_emit(r);
        }
        return out;
    }

    void executeCreateIndex(const CreateIndexStmt& s) {
        db_.createIndex(s.table, s.column);
    }

private:
    Database& db_;

    static void collectAndConjuncts(const Predicate* p, std::vector<const Predicate*>& out) {
        if (!p) return;
        if (p->kind == PredKind::And) {
            collectAndConjuncts(p->left.get(), out);
            collectAndConjuncts(p->right.get(), out);
        } else {
            out.push_back(p);
        }
    }

    static void validateColumns(const Predicate& p, const Table& t) {
        switch (p.kind) {
            case PredKind::Cmp:
            case PredKind::IsNull:
            case PredKind::IsNotNull:
                (void)t.col(p.column); // throws if missing
                break;
            case PredKind::And:
            case PredKind::Or:
                if (p.left)  validateColumns(*p.left, t);
                if (p.right) validateColumns(*p.right, t);
                break;
        }
    }

    static bool evalPredicate(const Predicate& p, const Table& t, size_t r) {
        switch (p.kind) {
            case PredKind::And:
                return evalPredicate(*p.left, t, r) && evalPredicate(*p.right, t, r);
            case PredKind::Or:
                return evalPredicate(*p.left, t, r) || evalPredicate(*p.right, t, r);
            case PredKind::IsNull:
                return !t.rows[r][t.col(p.column)].has_value();
            case PredKind::IsNotNull:
                return  t.rows[r][t.col(p.column)].has_value();
            case PredKind::Cmp: {
                const Cell& v = t.rows[r][t.col(p.column)];
                if (!v.has_value()) return false; // NULL never satisfies a comparison
                int cmp = v->compare(p.literal);  // lexicographic
                switch (p.op) {
                    case CmpOp::Eq:  return cmp == 0;
                    case CmpOp::Neq: return cmp != 0;
                    case CmpOp::Lt:  return cmp <  0;
                    case CmpOp::Lte: return cmp <= 0;
                    case CmpOp::Gt:  return cmp >  0;
                    case CmpOp::Gte: return cmp >= 0;
                }
            }
        }
        return false;
    }
};

} // namespace csvdb
