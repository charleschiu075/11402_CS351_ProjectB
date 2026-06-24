#pragma once
#include "Tokenizer.hpp"
#include "Ast.hpp"
#include <stdexcept>

namespace csvdb {

class Parser {
public:
    explicit Parser(std::vector<Token> toks) : t_(std::move(toks)) {}

    Statement parseStatement() {
        const Token& tk = peek();
        if (tk.kind == TokKind::KwSelect) {
            auto s = parseSelect();
            expect(TokKind::Semicolon, "expected ';'");
            expect(TokKind::End, "trailing tokens after ';'");
            return s;
        }
        if (tk.kind == TokKind::KwCreate) {
            auto s = parseCreateIndex();
            expect(TokKind::Semicolon, "expected ';'");
            expect(TokKind::End, "trailing tokens after ';'");
            return s;
        }
        throw std::runtime_error("expected SELECT or CREATE");
    }

private:
    std::vector<Token> t_;
    size_t i_ = 0;

    const Token& peek(size_t k=0) const { return t_[i_+k]; }
    Token consume() { return t_[i_++]; }
    bool accept(TokKind k) { if (peek().kind == k) { ++i_; return true; } return false; }
    void expect(TokKind k, const std::string& msg) {
        if (peek().kind != k) throw std::runtime_error(msg + " (got '" + peek().text + "')");
        ++i_;
    }

    SelectStmt parseSelect() {
        expect(TokKind::KwSelect, "expected SELECT");
        SelectStmt s;
        if (accept(TokKind::Star)) {
            s.star = true;
        } else {
            s.columns.push_back(parseIdent());
            while (accept(TokKind::Comma)) s.columns.push_back(parseIdent());
        }
        expect(TokKind::KwFrom, "expected FROM");
        s.table = parseIdent();
        if (accept(TokKind::KwWhere)) {
            s.where = parseOr();
        }
        return s;
    }

    CreateIndexStmt parseCreateIndex() {
        expect(TokKind::KwCreate, "expected CREATE");
        expect(TokKind::KwIndex,  "expected INDEX");
        expect(TokKind::KwOn,     "expected ON");
        CreateIndexStmt s;
        s.table = parseIdent();
        expect(TokKind::LParen, "expected '('");
        s.column = parseIdent();
        expect(TokKind::RParen, "expected ')'");
        return s;
    }

    std::string parseIdent() {
        if (peek().kind != TokKind::Ident)
            throw std::runtime_error("expected identifier (got '" + peek().text + "')");
        return consume().text;
    }

    PredPtr parseOr() {
        auto left = parseAnd();
        while (accept(TokKind::KwOr)) {
            auto right = parseAnd();
            auto n = std::make_unique<Predicate>();
            n->kind = PredKind::Or;
            n->left = std::move(left);
            n->right = std::move(right);
            left = std::move(n);
        }
        return left;
    }

    PredPtr parseAnd() {
        auto left = parsePrimary();
        while (accept(TokKind::KwAnd)) {
            auto right = parsePrimary();
            auto n = std::make_unique<Predicate>();
            n->kind = PredKind::And;
            n->left = std::move(left);
            n->right = std::move(right);
            left = std::move(n);
        }
        return left;
    }

    PredPtr parsePrimary() {
        if (accept(TokKind::LParen)) {
            auto p = parseOr();
            expect(TokKind::RParen, "expected ')'");
            return p;
        }
        // <ident> ( IS [NOT] NULL | <op> <literal> )
        std::string col = parseIdent();
        if (accept(TokKind::KwIs)) {
            bool neg = accept(TokKind::KwNot);
            expect(TokKind::KwNull, "expected NULL");
            auto p = std::make_unique<Predicate>();
            p->kind = neg ? PredKind::IsNotNull : PredKind::IsNull;
            p->column = col;
            return p;
        }
        CmpOp op;
        const Token& t = peek();
        switch (t.kind) {
            case TokKind::Eq:  op = CmpOp::Eq;  break;
            case TokKind::Neq: op = CmpOp::Neq; break;
            case TokKind::Lt:  op = CmpOp::Lt;  break;
            case TokKind::Lte: op = CmpOp::Lte; break;
            case TokKind::Gt:  op = CmpOp::Gt;  break;
            case TokKind::Gte: op = CmpOp::Gte; break;
            default: throw std::runtime_error("expected comparison operator");
        }
        ++i_;
        const Token& lit = peek();
        if (lit.kind != TokKind::StringLit && lit.kind != TokKind::NumLit)
            throw std::runtime_error("expected literal on RHS of comparison");
        auto p = std::make_unique<Predicate>();
        p->kind = PredKind::Cmp;
        p->column = col;
        p->op = op;
        p->literal = lit.text;
        ++i_;
        return p;
    }
};

} // namespace csvdb
