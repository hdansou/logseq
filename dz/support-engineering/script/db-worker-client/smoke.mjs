// Smoke test for client.mjs against a running db-worker-node (or the stub).
//
//   node smoke.mjs                       # discover via ~/logseq/server-list
//   LOGSEQ_ROOT_DIR=/path node smoke.mjs # discover under another root
//   node smoke.mjs http://127.0.0.1:PORT # explicit worker
//
// Exercises: discovery, health + revision check, EDN query round-trip, pull,
// search, render snapshots, SSE subscribe, a write, and the resulting
// :sync-db-changes event. Read-only unless SMOKE_WRITE=1.

import { discover, connect, kw, WorkerError } from "./client.mjs";

const arg = process.argv[2];
const WRITE = process.env.SMOKE_WRITE === "1";
let failures = 0;
const ok = (label, cond, extra = "") => {
  console.log(`${cond ? "PASS" : "FAIL"}  ${label}${extra ? "  " + extra : ""}`);
  if (!cond) failures++;
};

// -- discovery ---------------------------------------------------------------
let baseUrl = arg;
if (!baseUrl) {
  const d = await discover();
  console.log(`discover: ${d.ports.length} port(s) in ${d.rootDir}/server-list`);
  for (const w of d.workers) console.log(`  ${w.baseUrl}  ${w.status ?? "unreachable"}  ${w.repo ?? w.error ?? ""}`);
  if (!d.chosen) { console.log("no ready worker; start one with `logseq server start --graph <name>`"); process.exit(2); }
  baseUrl = d.chosen.baseUrl;
}

// -- connect + revision --------------------------------------------------------
const first = await connect({ baseUrl });
ok("connect", true, `repo=${first.repo} revision=${first.revision}`);
try {
  await connect({ baseUrl, expectedRevision: "definitely-not-this" });
  ok("revision mismatch is detected", false);
} catch (e) {
  ok("revision mismatch is detected", e instanceof WorkerError && e.code === "revision-mismatch");
}
const c = first;

// -- repo binding ------------------------------------------------------------
try {
  await (await import("./client.mjs")).createClient({ baseUrl, repo: "logseq_db_not_this_graph" }).q("[:find ?e :where [?e :block/uuid]]");
  ok("repo-mismatch surfaces as WorkerError", false);
} catch (e) {
  ok("repo-mismatch surfaces as WorkerError", e instanceof WorkerError && e.code === "repo-mismatch", `code=${e.code}`);
}

// -- q: EDN -> transit round trip -----------------------------------------
const qres = await c.q("[:find (pull ?e [:block/uuid :block/title]) :where [?e :block/uuid] [?e :block/title]]");
ok("q accepted an EDN query", qres !== undefined && qres !== null);
const echoed = Array.isArray(qres) && Array.isArray(qres[0]) && qres[0][0] === "find"; // stub echoes inputs back
if (echoed) {
  const q = qres[0];
  ok("EDN keywords/symbols survived encoding", q[0] === "find" && q[2] === "where", JSON.stringify(q).slice(0, 100));
  ok("namespaced keywords keep their namespace", JSON.stringify(q).includes('"block/uuid"'));
  ok("(pull ...) decoded to an array", Array.isArray(q[1]) && q[1][0] === "pull");
} else {
  ok("q returned rows", Array.isArray(qres), `${Array.isArray(qres) ? qres.length : "?"} rows`);
}

// -- pull --------------------------------------------------------------------
const pulled = await c.pull("[*]", 42).catch((e) => ({ __error: e }));
if (pulled?.__error) {
  ok("pull", false, pulled.__error.message);
} else {
  ok("pull decoded to a plain object", pulled && typeof pulled === "object" && !Array.isArray(pulled));
  ok("keyword keys are 'ns/name' strings", "block/title" in (pulled ?? {}) || "db/id" in (pulled ?? {}),
     Object.keys(pulled ?? {}).slice(0, 5).join(","));
  if (pulled?.["block/uuid"]) ok("uuid decoded to string", typeof pulled["block/uuid"] === "string", pulled["block/uuid"]);
  if (pulled?.["block/tags"]) ok("set decoded to array", Array.isArray(pulled["block/tags"]));
}

// -- search --------------------------------------------------------------------
const hits = await c.searchBlocks("alpha", { limit: 5, "feature/enable-semantic-search?": true });
const items = Array.isArray(hits) ? hits : hits?.items ?? [];
ok("searchBlocks returned a list", Array.isArray(items), `${items.length} hit(s)`);

// -- render snapshots ----------------------------------------------------------
const snap = await c.renderSnapshots({ blocks: [], children: [], resources: [[kw("journals")]] }).catch((e) => ({ __error: e }));
if (snap?.__error) ok("renderSnapshots", false, snap.__error.message);
else ok("renderSnapshots has basis-rev", typeof snap?.["basis-rev"] === "number", `basis-rev=${snap?.["basis-rev"]}`);

// -- SSE + write -----------------------------------------------------------------
const gotEvent = new Promise((resolve) => {
  const sub = c.subscribe((type, payload) => {
    if (type === "sync-db-changes") { sub.close(); resolve(payload); }
  }, { onError: (e) => console.log("  sse:", e.message) });
  setTimeout(() => { sub.close(); resolve(null); }, WRITE ? 5000 : 1500);
});

if (WRITE) {
  const block = { "block/uuid": "0f3a9e2c-1111-4222-8333-444455556666", "block/title": "Alpha block (edited)" };
  const wres = await c.applyOutlinerOps([[kw("save-block"), block, {}]]).catch((e) => ({ __error: e }));
  ok("applyOutlinerOps accepted [:save-block ...]", !wres?.__error, wres?.__error?.message ?? "");
  const ev = await gotEvent;
  ok("received :sync-db-changes over SSE after the write", ev !== null, ev ? `tx-id=${ev["tx-id"]}` : "timed out");
} else {
  await gotEvent;
  ok("SSE subscribe/close did not throw", true, "(set SMOKE_WRITE=1 to exercise a write + change event)");
}

console.log(failures ? `\n${failures} failure(s)` : "\nall checks passed");
process.exit(failures ? 1 : 0);
