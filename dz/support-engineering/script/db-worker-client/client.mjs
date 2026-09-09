// Client adapter for the Logseq db-worker-node HTTP + SSE API.
//
// This is the same transport Logseq's own Electron renderer uses
// (src/main/frontend/persist_db/remote.cljs). It gives a second frontend
// reads, writes, search, and a live change stream against a running graph.
//
// Design rules this module enforces so callers don't have to remember them:
//   - arguments always go over as `argsTransit` (plain-JSON `args` silently
//     turns keyword keys into inert strings)
//   - results are always transit; decoded here into plain JS
//   - the worker's `revision` is checked on connect; the thread-api is not
//     versioned, so an exact revision match is the only compatibility signal
//
// Zero dependencies beyond transit-js. Works in Node >= 18 and in any
// Electron/Tauri renderer with `fetch`. Browser tabs on other origins are
// blocked by the worker's CORS policy (lsp://logseq.com only).

import transit from "transit-js";
import { parseEdn } from "./edn.mjs";

export { parseEdn };
export const kw = (name) => transit.keyword(name);
export const sym = (name) => transit.symbol(name);

// ---------------------------------------------------------------------------
// transit encode / decode
// ---------------------------------------------------------------------------

const writer = transit.writer("json");

/** Convert plain JS into transit-friendly values: object keys -> keywords. */
export function toTransit(v) {
  if (v === null || v === undefined) return null;
  if (Array.isArray(v)) return v.map(toTransit);
  if (v instanceof Date || transit.isKeyword(v) || transit.isSymbol(v) ||
      transit.isMap(v) || transit.isSet(v) || transit.isList(v) || transit.isUUID(v)) return v;
  if (typeof v === "object") {
    const kvs = [];
    for (const [k, val] of Object.entries(v)) kvs.push(kw(k), toTransit(val));
    return transit.map(kvs);
  }
  return v;
}

export function encodeArgs(args) {
  return writer.write(args.map(toTransit));
}

/** Full "ns/name" for keywords and symbols; transit's .name() drops the namespace. */
const fullName = (k) => {
  const ns = k.namespace?.();
  return ns ? `${ns}/${k.name()}` : k.name();
};
const keyName = (k) => {
  if (transit.isKeyword(k) || transit.isSymbol(k)) return fullName(k);
  if (typeof k === "string") return k;
  if (typeof k === "object" && k !== null) return JSON.stringify(plain(k)); // composite keys (cmap)
  return String(k);
};

const reader = transit.reader("json", {
  // plain objects instead of transit maps; keyword keys become "ns/name" strings
  mapBuilder: {
    init: () => ({}),
    add: (m, k, v) => { m[keyName(k)] = v; return m; },
    finalize: (m) => m,
    fromArray: (arr) => {
      const m = {};
      for (let i = 0; i < arr.length; i += 2) m[keyName(arr[i])] = arr[i + 1];
      return m;
    },
  },
  // NB: the *reader* takes handlers as a plain object keyed by tag string
  // (the writer is the one that takes a transit.map keyed by constructor).
  handlers: {
    // transit-js's built-in set/list builders assume its own map type; with a
    // custom mapBuilder they crash, so decode both straight to arrays.
    set: (rep) => rep,
    list: (rep) => rep,
    cmap: (rep) => { const m = {}; for (let i = 0; i < rep.length; i += 2) m[keyName(rep[i])] = rep[i + 1]; return m; },
    "datascript/Entity": (rep) => rep,
    error: (rep) => Object.assign(new Error(rep?.message ?? "worker error"), { data: rep?.data }),
    "js/Error": (rep) => new Error(rep?.message ?? "worker error"),
  },
});

/** Recursively normalise decoded transit values into plain JS. */
function plain(v) {
  if (v === null || v === undefined) return v;
  if (Array.isArray(v)) return v.map(plain);
  if (transit.isKeyword(v) || transit.isSymbol(v)) return fullName(v);
  if (transit.isUUID(v)) return v.toString();
  if (transit.isSet(v)) return Array.from(v.values ? v.values() : v.rep).map(plain);
  if (transit.isList(v)) return Array.from(v.rep ?? v).map(plain);
  if (transit.isTaggedValue?.(v)) return { tag: v.tag, rep: plain(v.rep) };
  if (v instanceof Date || v instanceof Error) return v;
  if (typeof v === "object") {
    const out = {};
    for (const [k, val] of Object.entries(v)) out[k] = plain(val);
    return out;
  }
  return v;
}

export function decodeResult(str) {
  if (str === null || str === undefined || str === "") return null;
  return plain(reader.read(str));
}

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

export class WorkerError extends Error {
  constructor(message, { code, status, method, data } = {}) {
    super(message);
    this.name = "WorkerError";
    this.code = code;       // e.g. "repo-mismatch", "missing-repo", "invalid-query", "exception"
    this.status = status;   // HTTP status
    this.method = method;
    this.data = data;
  }
}

// ---------------------------------------------------------------------------
// health + discovery
// ---------------------------------------------------------------------------

const stripSlash = (u) => (u ?? "").replace(/\/$/, "");
const stripPrefix = (repo) => (repo ?? "").replace(/^logseq_db_/, "");

/** GET /healthz. Resolves even when the worker is still starting (status "starting"). */
export async function health(baseUrl, { fetch: f = globalThis.fetch, timeoutMs = 3000 } = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await f(`${stripSlash(baseUrl)}/healthz`, { signal: ctrl.signal });
    const body = await res.json();
    return { ...body, httpStatus: res.status, baseUrl: stripSlash(baseUrl) };
  } finally {
    clearTimeout(t);
  }
}

/**
 * Find running workers via <rootDir>/server-list ("<pid> <port>" per line).
 * Node only. Returns every reachable worker; `chosen` is the first ready one
 * matching `repo` (or the first ready one if repo is omitted).
 */
export async function discover({ rootDir, repo, fetch: f } = {}) {
  const [{ readFile }, os, path] = await Promise.all([
    import("node:fs/promises"), import("node:os"), import("node:path"),
  ]);
  const root = rootDir ?? process.env.LOGSEQ_ROOT_DIR ?? path.join(os.homedir(), "logseq");
  let text = "";
  try { text = await readFile(path.join(root, "server-list"), "utf8"); } catch { /* no workers yet */ }
  const ports = [...new Set(text.split("\n")
    .map((l) => l.trim().match(/^(\d+)\s+(\d+)$/))
    .filter(Boolean)
    .map((m) => Number(m[2])))];

  const workers = await Promise.all(ports.map(async (port) => {
    const baseUrl = `http://127.0.0.1:${port}`;
    try { return await health(baseUrl, { fetch: f }); }
    catch (e) { return { baseUrl, port, reachable: false, error: e.message }; }
  }));

  const ready = workers.filter((w) => w.status === "ready");
  const chosen = ready.find((w) => !repo || stripPrefix(w.repo) === stripPrefix(repo)) ?? null;
  return { rootDir: root, ports, workers, chosen };
}

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------

/**
 * Subscribe to GET /v1/events. Implemented over a streaming fetch so it works
 * in Node (no global EventSource before v23) and in renderers alike.
 * `onEvent(type, payload)` receives decoded events. Reconnects with backoff.
 */
export function subscribeEvents(baseUrl, onEvent, {
  fetch: f = globalThis.fetch, onError, onOpen, reconnectMs = 1000, maxReconnectMs = 15000,
} = {}) {
  let closed = false;
  let ctrl = null;
  let delay = reconnectMs;

  const decodeEvent = ({ type, payload }) => {
    let decoded = payload;
    if (typeof payload === "string") {
      try { decoded = decodeResult(payload); } catch { decoded = payload; }
    }
    // worker broadcasts are transit-encoded [type payload] pairs
    if (Array.isArray(decoded) && decoded.length === 2 && typeof decoded[0] === "string") {
      return [decoded[0], decoded[1]];
    }
    return [type, decoded];
  };

  const run = async () => {
    while (!closed) {
      ctrl = new AbortController();
      try {
        const res = await f(`${stripSlash(baseUrl)}/v1/events`, {
          headers: { Accept: "text/event-stream" }, signal: ctrl.signal,
        });
        if (!res.ok || !res.body) throw new Error(`events: HTTP ${res.status}`);
        onOpen?.();
        delay = reconnectMs;
        const dec = new TextDecoder();
        let buf = "";
        for await (const chunk of res.body) {
          buf += dec.decode(chunk, { stream: true });
          let idx;
          while ((idx = buf.indexOf("\n\n")) >= 0) {
            const block = buf.slice(0, idx); buf = buf.slice(idx + 2);
            const data = block.split("\n").filter((l) => l.startsWith("data:"))
              .map((l) => l.slice(5).trim()).join("\n");
            if (!data) continue;
            try { onEvent(...decodeEvent(JSON.parse(data))); }
            catch (e) { onError?.(e); }
          }
        }
      } catch (e) {
        if (closed) break;
        onError?.(e);
      }
      if (closed) break;
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, maxReconnectMs);
    }
  };
  run();

  return { close() { closed = true; ctrl?.abort(); } };
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

/**
 * Create a client bound to one worker and one graph.
 *
 *   const c = await connect({ baseUrl, repo, expectedRevision })
 *   await c.pull("[*]", [kw("block/uuid"), "..."])
 */
export function createClient({ baseUrl, repo, fetch: f = globalThis.fetch, timeoutMs = 60000 } = {}) {
  if (!baseUrl) throw new Error("baseUrl is required");
  if (!repo) throw new Error("repo is required");
  const base = stripSlash(baseUrl);

  async function invoke(method, args = []) {
    const m = String(method).replace(/^:/, "");
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    let res, body;
    try {
      res = await f(`${base}/v1/invoke`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ method: m, argsTransit: encodeArgs(args) }),
        signal: ctrl.signal,
      });
      body = await res.json().catch(() => ({}));
    } finally {
      clearTimeout(t);
    }
    if (!res.ok || body.ok === false) {
      const err = body.error ?? {};
      throw new WorkerError(err.message ?? `invoke ${m} failed (HTTP ${res.status})`, {
        code: err.code, status: res.status, method: m, data: err,
      });
    }
    return decodeResult(body.resultTransit);
  }

  const ednOr = (x) => (typeof x === "string" ? parseEdn(x) : x);

  return {
    baseUrl: base,
    repo,
    invoke,
    health: () => health(base, { fetch: f }),

    // -- reads ---------------------------------------------------------------
    /** Datascript query. `query` may be an EDN string. Extra args are query inputs. */
    q: (query, ...inputs) => invoke("thread-api/q", [repo, [ednOr(query), ...inputs.map(ednOr)]]),
    /** Pull one entity. `selector` may be EDN; `id` is a db/id, uuid lookup ref, etc. */
    pull: (selector, id) => invoke("thread-api/pull", [repo, ednOr(selector), ednOr(id)]),
    /** Pre-resolved view data: { basis-rev, slots, groups }. */
    renderSnapshots: (request) => invoke("thread-api/get-render-snapshots", [repo, request]),
    /** Fused keyword + vector search. Pass `feature/enable-semantic-search?: true` to opt in. */
    searchBlocks: (text, option = {}) => invoke("thread-api/search-blocks", [repo, text, option]),

    // -- writes --------------------------------------------------------------
    /** Validated outliner ops, e.g. [[kw("save-block"), block, {}]] or an EDN string. */
    applyOutlinerOps: (ops, opts = {}) => invoke("thread-api/apply-outliner-ops", [repo, ednOr(ops), opts]),
    undo: () => invoke("thread-api/undo-redo-undo", [repo]),
    redo: () => invoke("thread-api/undo-redo-redo", [repo]),

    // -- events --------------------------------------------------------------
    subscribe: (onEvent, opts) => subscribeEvents(base, onEvent, { fetch: f, ...opts }),
  };
}

/**
 * Health-check, verify repo binding and (optionally) revision, then return a client.
 * The thread-api has no version of its own; `expectedRevision` is the build
 * revision string from /healthz and is the only compatibility signal available.
 */
export async function connect({ baseUrl, repo, expectedRevision, onRevisionMismatch, fetch: f } = {}) {
  const h = await health(baseUrl, { fetch: f });
  if (h.status !== "ready") throw new Error(`worker at ${baseUrl} is not ready (status=${h.status})`);
  const bound = h.repo;
  const want = repo ?? bound;
  if (stripPrefix(bound) !== stripPrefix(want)) {
    throw new WorkerError(`worker is bound to ${bound}, not ${want}`, { code: "repo-mismatch" });
  }
  if (expectedRevision && h.revision !== expectedRevision) {
    const msg = `worker revision ${h.revision} != expected ${expectedRevision}`;
    if (onRevisionMismatch) onRevisionMismatch(h.revision, expectedRevision);
    else throw new WorkerError(msg, { code: "revision-mismatch", data: h });
  }
  const client = createClient({ baseUrl, repo: bound, fetch: f });
  client.revision = h.revision;
  return client;
}
