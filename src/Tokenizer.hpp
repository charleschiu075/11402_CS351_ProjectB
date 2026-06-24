#pragma once
#include <string>
#include <vector>
#include <stdexcept>
#include <cctype>
#include <unordered_set>

namespace csvdb {

enum class TokKind {
    Ident, StringLit, NumLit,
    Star, Comma, LParen, RParen, Semicolon,
    Eq, Neq, Lt, Lte, Gt, Gte,
    KwSelect, KwFrom, KwWhere, KwAnd, KwOr,
    KwCreate, KwIndex, KwOn,
    KwIs, KwNot, KwNull,
    End
};

struct Token {
    TokKind kind;
    std::string text;
    size_t pos;
};

class Tokenizer {
public:
    explicit Tokenizer(std::string src) : s_(std::move(src)) {}

    std::vector<Token> tokenize() {
        std::vector<Token> out;
        while (i_ < s_.size()) {
            skip_ws();
            if (i_ >= s_.size()) break;
            size_t start = i_;
            char c = s_[i_];

            if (c == '*') { out.push_back({TokKind::Star, "*", start}); ++i_; }
            else if (c == ',') { out.push_back({TokKind::Comma, ",", start}); ++i_; }
            else if (c == '(') { out.push_back({TokKind::LParen, "(", start}); ++i_; }
            else if (c == ')') { out.push_back({TokKind::RParen, ")", start}); ++i_; }
            else if (c == ';') { out.push_back({TokKind::Semicolon, ";", start}); ++i_; }
            else if (c == '=') { out.push_back({TokKind::Eq, "=", start}); ++i_; }
            else if (c == '!') {
                if (i_+1 < s_.size() && s_[i_+1] == '=') {
                    out.push_back({TokKind::Neq, "!=", start}); i_ += 2;
                } else throw std::runtime_error("expected '!=' at " + std::to_string(i_));
            }
            else if (c == '<') {
                if (i_+1 < s_.size() && s_[i_+1] == '=') { out.push_back({TokKind::Lte, "<=", start}); i_ += 2; }
                else if (i_+1 < s_.size() && s_[i_+1] == '>') { out.push_back({TokKind::Neq, "<>", start}); i_ += 2; }
                else { out.push_back({TokKind::Lt, "<", start}); ++i_; }
            }
            else if (c == '>') {
                if (i_+1 < s_.size() && s_[i_+1] == '=') { out.push_back({TokKind::Gte, ">=", start}); i_ += 2; }
                else { out.push_back({TokKind::Gt, ">", start}); ++i_; }
            }
            else if (c == '\'') {
                out.push_back(read_string('\''));
            }
            else if (c == '"') {
                // double-quoted identifier
                ++i_;
                std::string id;
                while (i_ < s_.size() && s_[i_] != '"') { id += s_[i_]; ++i_; }
                if (i_ >= s_.size()) throw std::runtime_error("unterminated quoted identifier");
                ++i_;
                out.push_back({TokKind::Ident, id, start});
            }
            else if (std::isdigit(static_cast<unsigned char>(c)) || (c == '-' && i_+1 < s_.size() && std::isdigit(static_cast<unsigned char>(s_[i_+1])))) {
                out.push_back(read_number());
            }
            else if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
                out.push_back(read_ident_or_keyword());
            }
            else {
                throw std::runtime_error(std::string("unexpected char '") + c + "' at " + std::to_string(i_));
            }
        }
        out.push_back({TokKind::End, "", s_.size()});
        return out;
    }

private:
    std::string s_;
    size_t i_ = 0;

    void skip_ws() {
        while (i_ < s_.size() && std::isspace(static_cast<unsigned char>(s_[i_]))) ++i_;
        // -- line comment
        if (i_+1 < s_.size() && s_[i_] == '-' && s_[i_+1] == '-') {
            while (i_ < s_.size() && s_[i_] != '\n') ++i_;
            skip_ws();
        }
    }

    Token read_string(char q) {
        size_t start = i_;
        ++i_; // skip opening quote
        std::string val;
        while (i_ < s_.size()) {
            char c = s_[i_];
            if (c == q) {
                if (i_+1 < s_.size() && s_[i_+1] == q) { val += q; i_ += 2; continue; }
                ++i_;
                return {TokKind::StringLit, val, start};
            }
            val += c;
            ++i_;
        }
        throw std::runtime_error("unterminated string literal");
    }

    Token read_number() {
        size_t start = i_;
        std::string num;
        if (s_[i_] == '-') { num += '-'; ++i_; }
        bool dot = false;
        while (i_ < s_.size()) {
            char c = s_[i_];
            if (std::isdigit(static_cast<unsigned char>(c))) { num += c; ++i_; }
            else if (c == '.' && !dot) { dot = true; num += c; ++i_; }
            else break;
        }
        return {TokKind::NumLit, num, start};
    }

    Token read_ident_or_keyword() {
        size_t start = i_;
        std::string id;
        while (i_ < s_.size()) {
            char c = s_[i_];
            if (std::isalnum(static_cast<unsigned char>(c)) || c == '_') { id += c; ++i_; }
            else break;
        }
        std::string up = id;
        for (auto& ch : up) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
        if (up == "SELECT") return {TokKind::KwSelect, id, start};
        if (up == "FROM")   return {TokKind::KwFrom,   id, start};
        if (up == "WHERE")  return {TokKind::KwWhere,  id, start};
        if (up == "AND")    return {TokKind::KwAnd,    id, start};
        if (up == "OR")     return {TokKind::KwOr,     id, start};
        if (up == "CREATE") return {TokKind::KwCreate, id, start};
        if (up == "INDEX")  return {TokKind::KwIndex,  id, start};
        if (up == "ON")     return {TokKind::KwOn,     id, start};
        if (up == "IS")     return {TokKind::KwIs,     id, start};
        if (up == "NOT")    return {TokKind::KwNot,    id, start};
        if (up == "NULL")   return {TokKind::KwNull,   id, start};
        return {TokKind::Ident, id, start};
    }
};

} // namespace csvdb
