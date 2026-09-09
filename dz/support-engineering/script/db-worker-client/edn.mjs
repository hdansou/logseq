// Minimal EDN reader producing transit-js values, so Datascript queries and
// outliner ops can be written as EDN literals instead of nested constructor calls.
//
//   parseEdn('[:find ?e :where [?e :block/uuid]]')
//
// Supported: vectors, lists, maps, sets, strings (with escapes), numbers,
// booleans, nil, keywords, symbols, comments, commas-as-whitespace.
// Not supported: tagged literals (#inst, #uuid), character literals, ratios,
// metadata. Those never appear in the query and op shapes this client sends.

import transit from "transit-js";

const WS = /[\s,]/;
const DELIM = /[\s,\[\]\(\)\{\}"]/;

export function parseEdn(src) {
  let i = 0;

  const peek = () => src[i];
  const skipWs = () => {
    for (;;) {
      while (i < src.length && WS.test(src[i])) i++;
      if (src[i] === ";") {
        while (i < src.length && src[i] !== "\n") i++;
        continue;
      }
      return;
    }
  };

  const readSeq = (close) => {
    const out = [];
    for (;;) {
      skipWs();
      if (i >= src.length) throw new Error(`EDN: unterminated sequence, expected '${close}'`);
      if (src[i] === close) { i++; return out; }
      out.push(readForm());
    }
  };

  const readString = () => {
    i++; // opening quote
    let s = "";
    while (i < src.length && src[i] !== '"') {
      if (src[i] === "\\") {
        i++;
        const c = src[i++];
        s += c === "n" ? "\n" : c === "t" ? "\t" : c === "r" ? "\r" : c;
      } else {
        s += src[i++];
      }
    }
    if (src[i] !== '"') throw new Error("EDN: unterminated string");
    i++;
    return s;
  };

  const readToken = () => {
    const start = i;
    while (i < src.length && !DELIM.test(src[i])) i++;
    return src.slice(start, i);
  };

  const readForm = () => {
    skipWs();
    const c = peek();
    if (c === undefined) throw new Error("EDN: unexpected end of input");
    if (c === "[") { i++; return readSeq("]"); }
    if (c === "(") { i++; return transit.list(readSeq(")")); }
    if (c === "{") {
      i++;
      const kvs = readSeq("}");
      if (kvs.length % 2) throw new Error("EDN: map literal with odd number of forms");
      return transit.map(kvs);
    }
    if (c === "#" && src[i + 1] === "{") { i += 2; return transit.set(readSeq("}")); }
    if (c === '"') return readString();

    const tok = readToken();
    if (tok === "") throw new Error(`EDN: unexpected character '${c}' at ${i}`);
    if (tok === "nil") return null;
    if (tok === "true") return true;
    if (tok === "false") return false;
    if (tok[0] === ":") return transit.keyword(tok.slice(1));
    if (/^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/.test(tok)) return Number(tok);
    return transit.symbol(tok);
  };

  const value = readForm();
  skipWs();
  if (i < src.length) throw new Error(`EDN: trailing input at ${i}: ${src.slice(i, i + 20)}`);
  return value;
}
