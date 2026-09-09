# Headless MCP Server — Specification

> **Status:** Draft for review (rev 3 — 7-server survey, context budget, write verification)
> **Date:** 2026-09-08
> **Verified against:** `origin/master` @ `d2ab772` (2026-09-02)
> **Related:** [024 — db-worker HTTP API as a UI Backend](../../07-investigations/024-db-worker-http-api-as-ui-backend.md) · [db-worker HTTP API](../../ddocs/reference/api/db-worker-http-api.md) · [Generated thread-api reference](../../ddocs/reference/api/thread-api.generated.md)

## 1. Summary

A Model Context Protocol server exposing a Logseq DB graph to AI agents **without requiring the desktop application to run**, by speaking to `db-worker-node` over HTTP + SSE instead of to the Electron renderer.

Delivered in phases, standalone first, to prove value before asking for a place in the CLI.

## 2. Background

### 2.1 What exists today

`src/electron/electron/mcp_server.cljs` runs an MCP server inside the desktop app — streamable HTTP with sessions, mounted at `/mcp` on `127.0.0.1:12315`, gated by `:server/mcp-enabled?`. Six tools:

`listPages` · `getPage` · `upsertNodes` · `searchBlocks` · `listTags` · `listProperties`

Its tools call `logseq.api`, which runs in the renderer. **The app must be open**, which disqualifies it for agents on a server, in CI, or on a schedule.

### 2.2 The enabling observation

The existing tools are already thin wrappers that delegate to the worker (`src/main/logseq/api/db_based/cli.cljs`):

| MCP tool | Renderer fn | Worker call |
|---|---|---|
| `listPages` | `cli-based-api/list-pages` | `:thread-api/api-list-pages` |
| `listTags` | `cli-based-api/list-tags` | `:thread-api/api-list-tags` |
| `listProperties` | `cli-based-api/list-properties` | `:thread-api/api-list-properties` |
| `getPage` | `cli-based-api/get-page-data` | `:thread-api/api-get-page-data` |
| `upsertNodes` | `cli-based-api/upsert-nodes` | `:thread-api/api-build-upsert-nodes-edn` → `:batch-import-edn` op |
| `searchBlocks` | `logseq.app.search` | `:thread-api/search-blocks` |

The renderer contributes argument marshalling and a cosmetic `re-render-root!`. **Re-hosting, not reimplementation.**

### 2.3 Why the context complaints are real — and where they come from

The reported "extremely high context" problem is **not tool count**. Six tools is well inside the 15–20 ceiling the ecosystem recommends. The cost is **unbounded output**, and the codebase says so in its own comment (`src/main/logseq/api/db_based/tools.cljs:95-96`):

```clojure
;; Until there are options to limit pages, return minimal info to avoid
;; exceeding max payload size
```

Two specific defects:

- **`list-pages` has no limit, offset, or cursor.** It returns *every* page in the graph. The only mitigation is returning 2 fields instead of 4. On a 10,000-page graph that is a single tool call large enough to consume most of a context window.
- **`get-page-data` returns the page's entire block tree**, unbounded in depth and count, with no way to request a summary.

Neither is a design position; both are acknowledged gaps. A new server must fix them at the contract level rather than inherit them.

## 3. Phasing

Value is proven before scope grows, and before the CLI team is asked for anything.

| Phase | Scope | Exit criterion |
|---|---|---|
| **P0 — Spike** | Standalone, read-only, three tools (`search`, `get_node`, `get_backlinks`), stdio only, one graph | An agent answers a real question against a real graph with the desktop app **closed**, and again with it **open** on the same graph. Measured token cost per call recorded. |
| **P1 — Standalone server** | Full tool surface (§6), context budget (FR-7), write path, revision gate, packaging for agent launchers | Used by real users for two weeks. Token-per-task and task-success measured against the in-app server. No data-loss incident. |
| **P2 — CLI integration** | Folded in as `logseq mcp`, reusing `server_runtime.ml` lifecycle | Only entered if P1 shows a clear win. Requires CLI owner agreement. |

**P0 and P1 deliberately duplicate** the CLI's attach-or-spawn logic. That duplication is the price of proving the idea without blocking on another team, and it is repaid in P2. It must be isolated to one module so P2 is a swap, not a rewrite.

## 4. Goals / Non-goals

**Goals**

- **G1** Serve MCP over a DB graph with no Electron process running.
- **G2** Coexist with a running desktop app on the same graph — no lock contention, no divergence.
- **G3** Bounded, predictable context cost per tool call.
- **G4** Fail safe against an internal, unversioned, unauthenticated backend.
- **G5** A migration path from the in-app server for existing agent configs.

**Non-goals**

- Replacing the in-app server (it stays right for renderer-dependent tools).
- Exposing the full 163-method thread-api to agents.
- Rendering markdown, macros, or editor semantics beyond reference labels.
- Remote or multi-user access. Loopback, single user.
- File graphs. DB graphs only.

## 5. Architecture

```
   MCP client (agent)
        │  stdio (default) or streamable HTTP
        ▼
   ┌──────────────────────────────────────┐
   │ headless MCP server                  │
   │  • tool registry (curated allowlist) │
   │  • context budgeter (§FR-7)          │
   │  • transit codec                     │
   │  • ref-label resolver                │
   │  • graph resolver: attach-or-spawn   │  ← isolate; swapped in P2
   └───────────────┬──────────────────────┘
                   │ POST /v1/invoke · GET /v1/events
                   ▼
      db-worker-node  ──►  <graph-dir>/db.sqlite
       (app-owned worker, or one we spawn)
```

The server is a **client of the worker**, never a peer. No database handle, no lock of its own.

## 6. Tool surface

### 6.1 What other implementations converged on

Seven servers surveyed — six community, plus the in-app one. Tool counts in parentheses.

| Capability | joelhooks (11) | jimsynz (13) | hsiang119 (17) | ergut (19) | gustavo (22) | LoveSkylark (~44) | in-app (6) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| list pages | ● | ● | ● *paged* | ● | ● | ● | ● *unbounded* |
| get page content | ● | ● | ● | ● | ● | ● *selector* | ● *unbounded* |
| full-text search | ● *by name* | ● | ● | ● | ● | ○ | ● |
| create / update page | ● | ● | ● | ● | ● | ● | ● *via upsert* |
| create / update block | ○ | ● | ● | ● | ● | ● | ● *via upsert* |
| **backlinks** | ● | ○ | ● | ● | ● *×2* | ● | ○ |
| **delete page / block** | ○ | ○ | ● | ● | ● | ● | ○ |
| **rename page** | ○ | ○ | ● | ● | ● | ● | ○ |
| **datascript query** | ● *NL* | ● | ○ | ● | ● | ● | ○ |
| **namespace listing** | ○ | ○ | ● | ● *×2* | ● *×2* | ○ | ○ |
| **journals** | ● | ○ | ● | ○ | ○ | ● | ○ |
| tags / properties | ○ | ○ | ● | ● | ● | ● *×10* | ● |
| orphan / gap analysis | ● | ○ | ○ | ○ | ○ | ● | ○ |
| **fixed-size stats** | ○ | ○ | ○ | ○ | ○ | ● | ○ |
| **detail selector** | ○ | ○ | ○ | ○ | ○ | ● | ○ |
| **write read-back verify** | ○ | ○ | ○ | ○ | ○ | ● | ○ |
| pagination | ○ | ○ | ● | ○ | ○ | ○ | ○ |
| response size cap | ○ | ○ | ○ | ○ | ○ | ● *5 MB* | ○ |

**Universal core** — list pages, get page content, search, create/update. Present everywhere.

**Common elsewhere, missing in-app** — backlinks (5 of 6), delete and rename (4 of 6), datascript query (5 of 6), journals (3 of 6). **Backlinks is the standout gap**: it is the question a knowledge-graph agent most wants to ask, and Logseq's server cannot answer it.

**Better in-app than elsewhere** — `upsertNodes`. One consolidated batch write across pages, blocks, tags and properties, where others expose 6–10 separate CRUD tools. This is exactly Anthropic's advice to consolidate frequently-chained calls rather than wrap endpoints. **Keep it.**

**Tool count is a cautionary spread**: 6 → 44. LoveSkylark's 44 is more than double the recommended ceiling, and it got there by wrapping every endpoint one-for-one (ten separate `list*` tools; separate `getTagUUID`/`getTag`). That is the failure mode to avoid, not a target.

### 6.2 Two design philosophies, and which to borrow from

The survey splits cleanly:

- **API-shaped** (jimsynz, ergut, hsiang119, gustavo, LoveSkylark) — one tool per endpoint. Complete, mechanical, and the reason tool counts reach 44.
- **Task-shaped** (joelhooks) — `getJournalSummary`, `analyzeGraph`, `findKnowledgeGaps`, `analyzeJournalPatterns`, `suggestConnections`. Eleven tools, none a CRUD wrapper; each answers a question a person would actually ask.

Anthropic's guidance favours the second: *"build thoughtful tools targeting specific high-impact workflows"* rather than wrapping every endpoint. joelhooks is the only implementation that took that route, and it is also the smallest useful surface in the survey.

**Position for this spec:** P1 ships an API-shaped core — it is what a general agent needs and what parity requires — but deliberately keeps it small (ten tools), consolidates writes into one tool, and merges naturally co-called reads. **Task-shaped tools are a P2 opportunity**, not a P1 omission; `findKnowledgeGaps` and `getJournalSummary` are the two worth stealing first.

### 6.3 Three patterns worth adopting outright

`LoveSkylark/mcp-logseq-db` is the only surveyed server that targets **Logseq 2.x DB-mode**, and its README reads like a list of scars. Three of its patterns should become requirements here.

1. **A fixed-size stats tool.** `pageStats` returns *seven integers regardless of page size*. That lets an agent decide whether a page is worth fetching without paying for its content — the single highest-leverage context trick in the survey. Becomes FR-7.8.

2. **A graded detail selector, not a binary.** `getPage` accepts `page | blocks | tags | properties | declared | all` rather than concise/detailed. Strictly better: an agent asking "what tags does this page carry" should not pay for its block tree. Upgrades FR-7.2.

3. **Read-back verification on writes.** Its headline warning: *"This API returns success for calls that do nothing."* Every write is verified by re-reading and carries a `verified` flag. That specific hazard is a plugin-HTTP-API defect and does **not** apply to `apply-outliner-ops`, which validates against a malli schema and returns real errors — but the discipline is cheap and turns silent no-ops into visible failures. Becomes FR-6.7.

Also adopted from it: **acknowledgement caps on destructive operations** (delete tools require an argument listing what will be affected) — FR-6.8.

Not adopted: its 5 MB response cap. That is a transport guard, not a context guard — 5 MB is roughly 1.25 M tokens, far beyond any context window. Our cap is token-based (FR-7.3).

### 6.4 Proposed surface (P1)

Ten tools. Namespaced with a `logseq_` prefix per Anthropic's namespacing guidance; the six legacy names registered as hidden aliases in P1 for config compatibility, removed in P2.

| Tool | Purpose | Backing method |
|---|---|---|
Ten tools, derived from Logseq's own grain rather than from the survey's lowest common denominator.

**Retrieve (5)**

| Tool | Purpose | Backing |
|---|---|---|
| `logseq_search` | Hybrid keyword + semantic. The default entry point. | `search-blocks` |
| `logseq_get_node` | One page **or** block, graded `detail`, bounded subtree | `api-get-page-data`, `pull` |
| `logseq_get_backlinks` | What references this node — Logseq's defining relation | `get-block-refs` |
| `logseq_list_nodes` | **Filtered, sorted, paginated** enumeration — by tag, journal range, recency | `cli-list-nodes`, `api-list-pages` |
| `logseq_node_stats` | **Fixed-size** counts for many nodes at once — no content | `q`, `pull` |

**Understand the schema (1)**

| Tool | Purpose | Backing |
|---|---|---|
| `logseq_list_schema` | Tags and properties with their types, cardinality, allowed values | `api-list-tags`, `api-list-properties` |

**Write (3)**

| Tool | Purpose | Backing |
|---|---|---|
| `logseq_capture` | Append to today's journal, or a named target. One call. | `get-journal-page-by-day` → `:insert-blocks` |
| `logseq_upsert_nodes` | Structured batch write across pages, blocks, tags, properties | `api-build-upsert-nodes-edn` → `:batch-import-edn` |
| `logseq_delete_nodes` | Deletion, acknowledgement-gated (FR-6.8) | `apply-outliner-ops` |

**Escape hatch (1)**

| Tool | Purpose | Backing |
|---|---|---|
| `logseq_query` | Datalog, for the long tail no fixed tool covers | `q` |

#### Why these, and not the survey's set

- **`logseq_get_node`, not `get_page` + `get_block`.** In DB graphs a page *is* a block — `entity-util/page?` is a predicate over the same entity space, and `pull` accepts a uuid or a `[:block/name …]` lookup ref uniformly. Two tools here would be two names for one operation, exactly the overlap that forces a model into an avoidable choice. Logseq is an outliner; the node is the atom.
- **`logseq_list_nodes` is enumeration, not a dump.** Anthropic's canonical anti-pattern is shipping `list_contacts` when the agent needs `search_contacts`. Enumeration survives only because filtered, ordered questions are real — "journals in this range", "pages tagged #Project by recency" — and those are not search. Unfiltered listing of an entire graph is not a supported call.
- **`logseq_get_backlinks` is non-negotiable.** Bidirectional linking is the reason Logseq exists, it is the query an agent most wants, and the in-app server cannot answer it. Five of six community servers ship it.
- **`logseq_capture` earns its slot as the one workflow tool.** Capture-to-journal is the highest-frequency action in real Logseq use. Without it every capture is *resolve today's journal → find its uuid → upsert into it*; with it, one call. This is Anthropic's `schedule_event` argument applied to the one workflow Logseq users perform daily.
- **`logseq_list_schema` before any write.** DB-mode tags are classes with typed properties. An agent that writes without reading the schema invents property names and produces junk. Merged rather than split because the two are always called together.
- **`logseq_node_stats` is the context lever.** Triage fifty candidates for fifty integers instead of fifty block trees.

**Dropped from earlier revisions:** `logseq_get_graph_info` (a tool describing the server is pure schema overhead) and `logseq_get_journal` (folded into `list_nodes` with a journal filter for reads, and `logseq_capture` for writes).

**Deferred to P2:** move block, namespace tree, orphan/gap analysis, renderer-state tools (current page/block, user config — unavailable headlessly), vector-index management, and the task-shaped tools from §6.2.

#### Compliance check

| Guidance | This set |
|---|---|
| ≤15–20 tools | 10 |
| No overlapping tools | `get_node` unifies page/block; `list_schema` merges tags/properties; `capture` and `upsert_nodes` split cleanly on unstructured-append vs structured-batch |
| Namespaced | `logseq_` prefix throughout |
| Consolidate chained calls | `capture`, `upsert_nodes` |
| Search over list | `search` is the entry point; `list_nodes` requires a filter |
| Pagination defaults | 20 items, cursors (FR-7.1) |
| Graded detail, concise default | FR-7.2 |
| Semantic identifiers over uuids | titles resolved before return (FR-5) |
| Actionable errors | FR-6.6 |
| Destructive/read-only annotations | FR-6.5 |

## 7. Functional requirements

### FR-1 Transport

- **FR-1.1** MUST support MCP over **stdio** as the default.
- **FR-1.2** SHOULD support streamable HTTP with session ids, matching the in-app transport.
- **FR-1.3** HTTP mode MUST bind loopback only, on a port distinct from `12315`.

### FR-2 Graph resolution

- **FR-2.1** MUST accept a target graph by name (argument or environment variable).
- **FR-2.2** MUST discover workers via `<root-dir>/server-list` (`<pid> <port>` lines) and `GET /healthz`.
- **FR-2.3** MUST attach to a `ready` worker bound to the target graph **regardless of `owner-source`**, including app-owned workers.
- **FR-2.4** MUST spawn a worker when none exists, following `cli/lib/server_runtime.ml`.
- **FR-2.5** MUST NOT stop, kill, or seize the lock of a worker it did not spawn. SHOULD stop workers it did spawn on clean shutdown.
- **FR-2.6** `<root-dir>` defaults to `~/logseq`, overridable via `--root-dir` / `LOGSEQ_CLI_ROOT_DIR`.
- **FR-2.7** All of FR-2 MUST live in a single module with no other responsibilities, so P2 can replace it wholesale.

> FR-2.3 is what makes this worth building: agent and human on one graph, worker serialising writes, UI updating live.

### FR-3 Compatibility gate

- **FR-3.1** MUST read `revision` from `/healthz` on connect and compare against the revision it was tested against.
- **FR-3.2** MUST refuse to serve tools on mismatch by default, naming both revisions.
- **FR-3.3** MUST provide `--allow-revision-mismatch`, warning on every tool call while active.

> The thread-api has no version. `:thread-api/get-version` is listed in `non-repo-methods` but **defined nowhere** — invoking it fails. Build revision is the only signal.

### FR-4 Compatibility with the in-app server

- **FR-4.1** The six legacy tool names MUST be registered as aliases in P1, accepting the in-app input schemas.
- **FR-4.2** Alias results MUST be semantically equivalent for the same graph state; byte-identical output is not required.
- **FR-4.3** Aliases MUST be marked deprecated in their descriptions and removed in P2.
- **FR-4.4** Where an alias would return unbounded output, it MUST apply FR-7 defaults and say so in the truncation notice. **Bounded output takes precedence over bug-for-bug parity.**

### FR-5 Reference resolution

- **FR-5.1** Text returned to a client MUST have `[[uuid]]` references replaced by the referenced entity's title.
- **FR-5.2** Resolution MUST batch, following `cli/lib/uuid_refs_types.ml:168-192` (collect uuids, then `pull` labels).
- **FR-5.3** Unresolvable references MUST degrade to a readable placeholder — never a raw uuid, never an error.

> The worker returns storage form. The in-app server gets display form free via the renderer; this one does not. Skipping this makes every text result unusable to a model.

### FR-6 Write safety

- **FR-6.1** All writes MUST go through `apply-outliner-ops`, validated against the malli `op-schema` (`deps/outliner/src/logseq/outliner/op.cljs`).
- **FR-6.2** MUST NOT call `transact` directly — it bypasses validation and semantic undo.
- **FR-6.3** `logseq_upsert_nodes` MUST be `api-build-upsert-nodes-edn` followed by `:batch-import-edn`, mirroring `cli-based-api/upsert-nodes`.
- **FR-6.4** MUST support `dry_run`, returning the operation summary without transacting.
- **FR-6.5** Destructive tools MUST be annotated per the MCP spec (`destructiveHint`, `readOnlyHint`) so clients can gate them.
- **FR-6.6** Validation failures MUST surface as tool errors carrying the worker's message, with actionable correction guidance — not a traceback, not an opaque code.
- **FR-6.7** Every write MUST be verified by reading back the affected entities, and the result MUST carry an explicit `verified` boolean. A write that transacts without error but changes nothing MUST report `verified: false`, not success.
- **FR-6.8** Destructive tools MUST require an `acknowledge` argument enumerating what will be affected (page title, block count, dependent references). A call without it MUST fail with that enumeration in the error, so the second call is informed.

> FR-6.7 is borrowed from `mcp-logseq-db`, whose README leads with *"This API returns success for calls that do nothing."* That specific hazard is a plugin-HTTP-API defect and should not affect `apply-outliner-ops`, which validates and returns real errors — but read-back is cheap, and it converts a class of silent no-op into a visible failure. Under FR-2.3 another writer (the desktop app) may be touching the same graph, which is a second reason not to trust a bare success.

### FR-7 Context budget *(new — the core of this revision)*

- **FR-7.1** Every list-returning tool MUST accept `limit` and `cursor`, defaulting to **20 items**, and MUST return a `next_cursor` when more exist.
- **FR-7.2** Every tool returning entities MUST accept a **graded `detail` selector**, not a binary. For `logseq_get_node`: `summary | blocks | tags | properties | all`, defaulting to **`summary`**. Each level MUST be independently requestable, so an agent asking which tags a page carries never pays for its block tree. Summary returns only what is needed to decide what to fetch next — title, uuid, updated-at, counts.
- **FR-7.3** Every response MUST be capped at a configurable token budget, default **25,000 tokens**, matching the Claude Code tool-response ceiling.
- **FR-7.4** Truncated responses MUST carry a notice stating what was omitted and which narrower call would retrieve it. Truncation must teach, not just clip.
- **FR-7.5** `logseq_get_node` MUST accept `max_depth` and `max_blocks`, with finite defaults. It MUST NOT return an unbounded block tree under any argument combination.
- **FR-7.6** Total serialised size of all tool schemas MUST stay under **10,000 tokens**, asserted by a test. Descriptions are prompt-engineered prose, but they are also permanent context.
- **FR-7.7** Tool descriptions MUST steer toward narrow, iterative retrieval — many small searches over one broad dump.
- **FR-7.8** `logseq_node_stats` MUST return a **fixed-size** response regardless of node size or how many nodes are asked about — counts only, no content. It MUST accept a list of node identifiers so an agent can triage many candidates in one call.

> FR-7.8 is the highest-leverage item in this section. It lets an agent answer "is this page worth reading?" for fifty pages at the cost of fifty integers rather than fifty block trees. `mcp-logseq-db` is the only surveyed server that offers it.

> Rationale: seven MCP servers have been measured consuming 67,000 tokens of definitions before the first prompt; the GitHub server alone ~18,000 across 27 tools. Anthropic reports a concise/detailed split of 72 vs 206 tokens for the same record — roughly a third. FR-7.1 through FR-7.3 are where the reported context problem is actually solved.

### FR-8 Method allowlist

- **FR-8.1** Invokable thread-api methods MUST be a static, explicit allowlist in source.
- **FR-8.2** MUST NOT expose a generic "invoke arbitrary method" tool under any flag.
- **FR-8.3** `unsafe-unlink-db`, `import-db-binary`, `search-truncate-tables`, and any graph-deleting method MUST NOT appear in it.

> `/v1/invoke` has no allowlist of its own — every registered method is reachable, including graph deletion. This server is the only boundary between an agent and that surface.

### FR-9 Multi-graph

- **FR-9.1** One instance serves one graph for its lifetime, mirroring the worker's binding.
- **FR-9.2** A request naming a different graph MUST be rejected clearly, never silently retargeted.

### FR-10 Change awareness *(P2)*

- **FR-10.1** MAY subscribe to `/v1/events`, treating `:sync-db-changes` as cache invalidation.
- **FR-10.2** MUST invalidate any graph-derived cache on that event.
- **FR-10.3** MUST tolerate stream loss and reconnect with backoff.

## 8. Non-functional requirements

### NFR-1 Security

- Loopback only; no remote mode, no TLS story.
- No credentials in logs; graph content not logged above debug.
- MUST NOT read or transmit `~/logseq/auth.json` — no sync credentials are needed.
- Tool descriptions and results are model-visible; no paths outside the graph directory.

### NFR-2 Reliability

- Every `/v1/invoke` call MUST carry a client-side timeout. The worker logs slow calls at 10s but does **not** time them out.
- Worker errors MUST map to MCP errors preserving `code` (`missing-repo`, `repo-mismatch`, `repo-locked`, `invalid-query`, `validation-error`, `exception`).
- Worker loss MUST yield a clear tool error and re-resolution per FR-2, not a crash.

### NFR-3 Compatibility maintenance

- Tested revision recorded in source, not only in docs.
- CI SHOULD run the thread-api drift gate (`dz/support-engineering/script/thread-api-docs/gen_thread_api_docs.clj --check`) so a method rename fails the build.

### NFR-4 Observability

- Structured logs: attach/spawn decision and reason, revision check, each tool call with duration, result size in tokens, and truncation flag.
- `--diagnose` reporting discovered workers, chosen worker, revision comparison, allowlist size — without serving.

### NFR-5 Evaluation

- **NFR-5.1** A fixed task set MUST be run against both this server and the in-app server, recording tokens-to-completion and success rate.
- **NFR-5.2** That harness MUST run in CI so a schema or description change cannot silently regress context cost.

## 9. Failure modes

| Condition | Required behaviour |
|---|---|
| No worker, cannot spawn | Clear startup error naming `server-list` and root-dir. Do not serve. |
| Worker `starting` | Wait with timeout, then fail reporting the observed status. |
| Revision mismatch | Refuse per FR-3.2 unless overridden. |
| Graph locked elsewhere | Surface `repo-locked`; never break the lock. |
| Worker dies mid-session | Tool error, re-resolve per FR-2 on next call. |
| Malformed `server-list` line | Skip it, continue discovery. |
| Oversized `upsert_nodes` batch | Reject before transacting, with the limit stated. |
| Result exceeds budget | Truncate per FR-7.3–7.4; never silently drop. |

## 10. Acceptance criteria

1. Desktop app **closed**: an agent lists tools and completes a read against a real graph.
2. Desktop app **open on the same graph**: the server attaches to the existing worker (one worker in `/healthz`), and a write via `logseq_upsert_nodes` appears in the UI without restart.
3. Results containing block references show titles, not `[[uuid]]`.
4. `logseq_list_nodes` on a ≥5,000-page graph returns ≤20 items with a working `next_cursor`, and the response is under the token budget.
5. `logseq_get_node` on the deepest page in the graph respects `max_depth`/`max_blocks` and emits a truncation notice naming the narrower call.
6. `detail: "summary"` measurably reduces response tokens versus `"all"` for the same page — target ≥50% reduction. Each intermediate level costs strictly less than `all`.
7. `logseq_node_stats` over 50 nodes returns a response whose size does not vary with those pages' content length.
8. Total tool-schema size is under 10,000 tokens, asserted by test.
9. `dry_run` on `logseq_upsert_nodes` reports a summary and leaves the graph unchanged (verified by export diff).
10. A successful write returns `verified: true` and the read-back reflects it; a write that transacts but changes nothing returns `verified: false`.
11. `logseq_delete_nodes` without `acknowledge` fails and enumerates what would have been affected; the graph is unchanged.
12. An invalid upsert returns a tool error citing schema validation with correction guidance; graph unchanged.
13. Revision mismatch refuses to serve and names both revisions.
14. No tool, flag, or configuration reaches a method outside the allowlist — asserted over the registry.
15. Killing the worker mid-session yields a tool error and recovery on the next call.
16. The evaluation harness (NFR-5) shows tokens-to-completion no worse than the in-app server on the shared task set, on a graph of ≥1,000 pages.

## 11. Open decisions

| # | Decision | Recommendation |
|---|---|---|
| D1 | Standalone vs CLI | **Standalone for P0/P1**, per the user's phasing. Revisit at P2 with evidence. FR-2.7 keeps the door open. |
| D2 | Runtime | **Node/TypeScript** for P0/P1 — fastest to the MCP SDK and to users. Melange if P2 lands in the CLI. |
| D3 | Prefix vs bare tool names | **`logseq_` prefix canonical, legacy names as deprecated aliases.** Prefix aids selection when several servers are loaded; aliases protect existing configs. |
| D4 | Merge `listTags` + `listProperties` | **Merge into `logseq_list_schema`.** Always co-called; overlapping tools force avoidable model decisions. |
| D5 | Datascript escape hatch | **Include `logseq_query`.** Five of six community servers ship it; it prevents tool sprawl for long-tail questions. Read-only; must respect FR-7. |
| D6 | Task-shaped tools (§6.2) | **Defer to P2.** `findKnowledgeGaps` and `getJournalSummary` are the two worth stealing from joelhooks, once P1 has usage data showing which questions agents actually ask. |
| D7 | Should the in-app server later delegate here? | Out of scope, but if P1 succeeds the in-app server could become a thin proxy, collapsing two implementations into one. |

## 12. Appendix — method mapping

| Tool | thread-api methods |
|---|---|
| `logseq_search` | `search-blocks` |
| `logseq_get_node` | `api-get-page-data`, `pull` (+ `pull` for ref labels) |
| `logseq_get_backlinks` | `get-block-refs` |
| `logseq_list_nodes` | `cli-list-nodes`, `api-list-pages`, `get-journal-page-by-day` |
| `logseq_node_stats` | `q` (aggregate counts), `pull` |
| `logseq_list_schema` | `api-list-tags`, `api-list-properties` |
| `logseq_capture` | `get-journal-page-by-day` → `apply-outliner-ops` `:insert-blocks` |
| `logseq_upsert_nodes` | `api-build-upsert-nodes-edn` → `apply-outliner-ops` `:batch-import-edn` |
| `logseq_delete_nodes` | `apply-outliner-ops` (`:delete-blocks`, `:delete-page`) |
| `logseq_query` | `q` |

`cli-list-nodes` and `cli-list-tasks` have **no callers anywhere** — built, never wired up. `cli-list-nodes` backs `logseq_list_nodes` here; `cli-list-tasks` is a candidate for a later task-shaped tool.

## 13. Reusable prior art

| Need | Existing implementation |
|---|---|
| Discovery, transit, revision pinning, SSE | `dz/support-engineering/script/db-worker-client/client.mjs` |
| Attach-or-spawn policy | `cli/lib/server_runtime.ml` |
| Batched reference-label resolution | `cli/lib/uuid_refs_types.ml:168-192` |
| Tool schemas and prompt text | `src/electron/electron/mcp_server.cljs` |
| Method inventory + drift gate | `ddocs/reference/api/thread-api.generated.md`, `dz/support-engineering/script/thread-api-docs/` |

## 14. References

- [Writing effective tools for AI agents — Anthropic](https://www.anthropic.com/engineering/writing-tools-for-agents)
- [Effective context engineering for AI agents — Anthropic](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)
- [SEP-1576: Mitigating token bloat in MCP](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/1576)
- [MCP tool design: practical approaches and tradeoffs — AWS](https://aws.amazon.com/blogs/machine-learning/mcp-tool-design-practical-approaches-and-tradeoffs/)
- [4 rules to build an efficient MCP server — Bump.sh](https://bump.sh/blog/4-rules-to-build-an-efficient-mcp-server/)
**Surveyed implementations**

| Repo | Tools | Backend | Why it mattered here |
|---|---|---|---|
| [LoveSkylark/mcp-logseq-db](https://github.com/LoveSkylark/mcp-logseq-db) | ~44 | HTTP API, **DB-mode only** | Closest target match. Source of FR-6.7 (read-back verify), FR-6.8 (acknowledgement), FR-7.2 (detail selector), FR-7.8 (fixed-size stats). Also the cautionary tool count. |
| [joelhooks/logseq-mcp-tools](https://github.com/joelhooks/logseq-mcp-tools) | 11 | HTTP API | The only task-shaped surface — `analyzeGraph`, `findKnowledgeGaps`, `getJournalSummary`. The P2 model. |
| [gustavo-meilus/logseq-api-mcp](https://github.com/gustavo-meilus/logseq-api-mcp) | 22 | HTTP API | Split read/write namespacing; exclude-tags privacy filter; DB-mode-gated property writes. |
| [hsiang119/logseq-mcp-server](https://github.com/hsiang119/logseq-mcp-server) | 17 | HTTP API | Only server with pagination on list tools. `logseq_` prefix precedent. |
| [ergut/mcp-logseq](https://github.com/ergut/mcp-logseq) | 19 | HTTP API | Optional vector search; namespace flat + tree; access-control filtering. |
| [jimsynz/logseq-mcp-server](https://github.com/jimsynz/logseq-mcp-server) | 13 | HTTP API | Renderer-state tools (current page/block/config) — the category unavailable headlessly. |
| [apw124/logseq-mcp](https://github.com/apw124/logseq-mcp) | — | HTTP API | Python packaging reference. |

Every surveyed server talks to the **plugin HTTP API on `:12315`**, and therefore requires the desktop app. None is headless. That is the gap this spec fills.
