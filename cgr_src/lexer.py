"""The .cg lexer and token model."""
from __future__ import annotations
from cgr_src.common import *

class TT(Enum):
    IDENT=auto(); STRING=auto(); COMMAND=auto(); NUMBER=auto()
    LBRACE=auto(); RBRACE=auto(); EQUALS=auto(); COMMA=auto()
    LPAREN=auto(); RPAREN=auto(); DOT=auto(); GTE=auto()
    COMPAT=auto(); EOF=auto()  # GTE: >=  COMPAT: ~>

@dataclass(frozen=True)
class Token:
    type: TT; value: str; line: int; col: int
    def __repr__(self): return f"Tok({self.type.name},{self.value!r},{self.line}:{self.col})"

class LexError(Exception):
    def __init__(self, msg, line=0, col=0, src=""):
        self.msg=msg; self.line=line; self.col=col; self.src=src; super().__init__(msg)

def lex(source: str, filename: str = "<input>") -> list[Token]:
    tokens: list[Token] = []; lines = source.split("\n")
    i = 0; ln = 1; L = len(source)
    def col_of(p): return p - source.rfind("\n", 0, p)
    def srcl(): return lines[ln-1] if ln <= len(lines) else ""
    while i < L:
        ch = source[i]
        if ch in " \t\r": i+=1; continue
        if ch == "\n": ln+=1; i+=1; continue
        if ch == "#":
            e = source.find("\n", i); i = L if e < 0 else e; continue
        c = col_of(i)
        # Two-character operators: >=, ~>
        if ch == ">" and i+1<L and source[i+1]=="=":
            tokens.append(Token(TT.GTE, ">=", ln, c)); i+=2; continue
        if ch == "~" and i+1<L and source[i+1]==">":
            tokens.append(Token(TT.COMPAT, "~>", ln, c)); i+=2; continue
        simple = {"{":TT.LBRACE, "}":TT.RBRACE, "=":TT.EQUALS,
                  ",":TT.COMMA, "(":TT.LPAREN, ")":TT.RPAREN}
        if ch in simple:
            tokens.append(Token(simple[ch], ch, ln, c)); i+=1; continue
        if ch == '"':
            i+=1; parts=[]
            while i<L and source[i]!='"':
                if source[i]=="\\" and i+1<L:
                    esc=source[i+1]
                    parts.append({"n":"\n","t":"\t",'"':'"',"\\":"\\"}.get(esc, source[i:i+2]))
                    i+=2
                else:
                    if source[i]=="\n": ln+=1
                    parts.append(source[i]); i+=1
            if i>=L: raise LexError("Unterminated string", ln, c, srcl())
            i+=1; tokens.append(Token(TT.STRING, "".join(parts), ln, c)); continue
        if ch == "`":
            sl=ln; i+=1; parts=[]
            while i<L and source[i]!="`":
                if source[i]=="\n": ln+=1
                parts.append(source[i]); i+=1
            if i>=L: raise LexError("Unterminated command", sl, c, lines[sl-1] if sl<=len(lines) else "")
            i+=1; tokens.append(Token(TT.COMMAND, "".join(parts).strip(), sl, c)); continue
        if ch.isdigit():
            s=i
            while i<L and source[i].isdigit(): i+=1
            tokens.append(Token(TT.NUMBER, source[s:i], ln, c)); continue
        if ch.isalpha() or ch=="_":
            s=i
            while i<L and (source[i].isalnum() or source[i] in "_-"): i+=1
            tokens.append(Token(TT.IDENT, source[s:i], ln, c)); continue
        if ch==".":
            i+=1; tokens.append(Token(TT.DOT, ".", ln, c)); continue
        raise LexError(f"Unexpected char {ch!r}", ln, c, srcl())
    tokens.append(Token(TT.EOF, "", ln, col_of(L)))
    return tokens

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
