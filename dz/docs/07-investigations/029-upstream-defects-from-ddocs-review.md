# Upstream Defects Found During the ddocs Review Pass

> **Last revised:** 2026-09-12
> **Verified against:** `master` @ `be800f1711`, which is still current master on 2026-09-13 —
> both `origin` (the `hdansou/logseq` fork) and `upstream` (`logseq/logseq`) resolve
> `refs/heads/master` to that commit. **Source read only.** Nothing here was reproduced in a
> running app unless the entry says so.
> **Context:** waves 1–7 of the documentation review audited every page in `dz/docs/ddocs` against
> source. Along the way the audits found behaviour that looks like a product defect rather than a
> documentation error. They were recorded in commit messages and session memory; this page collects
> them so they can be triaged in one place. A separate session's Markdown Mirror and paste
> findings are folded in below, marked with their stronger evidence.

## How to use this list

Each entry names what looks wrong and where in source it was read. **Revalidate before filing
anything.** In an earlier exercise, 7 of 11 findings changed verdict once they were re-read against
both the tested revision and current master and then reproduced blind. The failure modes were:
code that had already changed, a call path that nothing reaches, and a "bug" that only existed in
the reader's model of the code.

A practical order: re-read the cited lines at current master, decide whether any caller reaches the
path, then try to reproduce in the app. Only then file.

Severity is not assigned here. What matters for triage is whether the path is reachable by a user.

## Security and access

| What looks wrong | Where | Notes |
|---|---|---|
| Before any API token is saved, the desktop HTTP/MCP server accepts **any** `Authorization` value, and CORS allows every origin | `src/electron/electron/server.cljs:86`, `:164` | A request with **no** header still gets 401, because `string/replace` on nil throws inside the try (`:96-103`). So the hole is "any value passes", not "no auth". **Reachable (checked 2026-09-12).** The token panel persists each key as `server/<name>` (`server.cljs:53-61`), so tokens can be saved; the hole is the state before any is |
| `graph backup remove --src` joins its argument onto the backup directory and removes the tree, with no path check | `cli/lib/graph.ml:469-470`, `:753-758` | The ClojureScript backup code guards the same operation with `child-path!` (`db_worker/graph_backup.cljs:23-37`). **Reachable (checked 2026-09-12).** `--src` reaches `remove_tree` as nothing but `String.trim opts.src` (`graph.ml:275-280`); `safe_backup_part` is applied only when building a *create* name (`:148`) **Reproduced 2026-09-13** with the installed CLI (build `f7362f07b0`, whose `cli/lib/graph.ml` and `cli_parse.ml` are byte-identical to `be800f1711`). In a throwaway graph: `--src ../does-not-exist-xyz` returns `backup not found: ../does-not-exist-xyz`, echoing the traversal verbatim with no validation; `--src ../decoy`, against a directory created beside `backup/`, printed `Removed backup ../decoy` and deleted it, leaving `db.sqlite` and the real backup intact. `--src ..` — which resolves to the graph folder itself and would take `db.sqlite` with it — was deliberately **not** run. |
| Inviting a user who has never set an E2EE password to an E2EE graph creates the membership first, then fails to wrap the key | `deps/db-sync/.../sync.cljs:392-398`, `crypt.cljs:678-679` | The invitee ends up a member who cannot decrypt; the manager sees a generic error |
| `custom.js` approval is keyed on the relative path `logseq/custom.js`, which is identical for every DB graph | `src/main/frontend/handler/ui.cljs:131-136`, `src/main/frontend/config.cljs:374-379` | Approving one graph's script approves every graph's for 7 days. Cancel stores `false` permanently with no UI to undo (`ui.cljs:139-140`, `:153-154`). **Reachable (checked 2026-09-12).** For a DB graph `get-custom-js-path` returns `logseq/custom.js` with no repo segment (`config.cljs:374-379`), and the installer runs on desktop (`plugins.cljs:1790-1793`) |

## Data loss and recovery

| What looks wrong | Where | Notes |
|---|---|---|
| Recycle promises deleted **blocks** can be restored, but deleting a block retracts it outright | `src/resources/dicts/en.edn:1824`; `deps/outliner/src/logseq/outliner/core.cljs:440-442` | `recycle-blocks-tx-data` (`recycle.cljs:127`) has only test callers. **Reachable (checked 2026-09-12).** **Sharper than written:** the Recycle list renders entities carrying `:logseq.property/deleted-at`, and only that test-only fn would set it on a block, so the block branch of `components/recycle.cljs:40-58` — its "deleted at" label and Restore button — never runs |
| A pending sync transaction whose rebase fails or becomes a no-op is marked non-pending and never uploaded | `src/main/frontend/worker/sync/apply_txs.cljs:1704-1710` | Only `:block/title` is conflict-tracked (`:1130-1131`), so edits to other attributes may be lost without a conflict entry. **Reachable (checked 2026-09-12).** The call sits in the rebase path (`:1710`), and title conflicts do surface a popup (`components/block.cljs:3375-3406`), so it is the non-title edits that vanish quietly |
| "Remove the file too" deletes only the asset block; the file stays in `assets/` | `src/main/frontend/handler/editor.cljs:1741-1753`; checkbox text `en.edn:97` | The platform function `:asset-delete!` exists in both adapters but has no callers. **Reachable (checked 2026-09-12).** Repo-wide, `asset-delete!` appears only as its definition and adapter registration in `worker/platform/{browser,node}.cljs`; nothing invokes it |
| Deleted assets are never removed from the sync server | `src/main/frontend/worker/sync/asset_db_listener.cljs:11-25` | Only `:update-asset` ops are generated, though `:remove-asset` is handled server-side |
| The sync client refuses asset uploads over 100 MiB even for E2EE graphs, whose server limit is 200 MiB | `src/main/frontend/worker/sync/assets.cljs:246-255`; server `deps/db-sync/.../handler/assets.cljs:9-10` | The larger allowance is unreachable from the app |

## Plugin API

| What looks wrong | Where | Notes |
|---|---|---|
| The six template methods are declared but have no host implementation | `libs/src/LSPlugin.ts:513`, `:533-541` | Implementations were deleted with `src/main/logseq/api/file_based.cljs` in `eaa5a57337` |
| `logseq.Git.*` is dead | `libs/src/LSPlugin.ts:1010-1026` | `036df25a17` removed the `git_*` exports; there is no `sdk/git.cljs` |
| `App.execGitCommand` is dead | — | The export was removed in `bcc478b5f7` |
| `getUserInfo` is declared with no export and no SDK-local handler | `libs/src/LSPlugin.ts:411` | |
| `getPagesFromNamespace` and `getPagesTreeFromNamespace` are dead | `file_based.cljs:69`, `:75` at `eaa5a57337^` | |
| `onGraphAfterIndexed` is declared, but nothing emits it | `libs/src/LSPlugin.ts:561` | |
| `upsertBlockProperty`'s `schema` option never reaches the property | `src/main/logseq/api/db_based.cljs:190-193` | The parameter is `key'`, but the map is built `{key schema}`, keyed by `cljs.core/key` |
| `registerPageMenuItem`, `registerBlockContextMenuItem` and `registerHighlightContextMenuItem` return `undefined` instead of an unregister function | `libs/src/LSPlugin.user.ts:502`, `:607`, `:631` | |
| `:thread-api/get-version` is named but never defined | `src/main/frontend/worker/db_worker_node.cljs:210` | **Checked 2026-09-12.** **Downgraded.** The keyword appears exactly once, in a list of method names; there is no `def-thread-api` for it anywhere and no caller. So it is an unused name, not a call that fails. Not worth filing unless that list is meant to be exhaustive |

## MCP server and CLI

| What looks wrong | Where | Notes |
|---|---|---|
| MCP error responses never set `isError` | `src/electron/electron/mcp_server.cljs:72-75`, `:84-87` | Clients cannot distinguish an error from a result |
| The MCP input schema accepts a numeric `id`, which server-side validation rejects | `mcp_server.cljs:194` vs `src/main/logseq/api/db_based/tools.cljs:272` | |
| The `upsertNodes` tool description demonstrates a page edit the tool refuses, and contains the typo "eing" | `mcp_server.cljs:155`, `:167-170` | Page, tag and property edits throw (`tools.cljs:333-334`) |
| The API-server autostart checkbox shows checked while the setting is unset, but autostart only runs when it is truthy | `src/main/frontend/components/server.cljs:106` vs `:142` | |
| The CLI does not honour `LOGSEQ_GRAPHS_DIR`; it uses `<root-dir>/graphs` | `cli/lib/graph.ml:312` | The Electron main process does honour it (`deps/common/src/logseq/common/graph.cljs:114`), while the renderer hard-codes `~/logseq/graphs` (`src/main/frontend/config.cljs:326-331`) |

## Editor and queries

| What looks wrong | Where | Notes |
|---|---|---|
| The `/Query` help popup still shows deprecated `{{query …}}` examples | `src/main/frontend/commands.cljs:32-49` | The macro renders a deprecation warning (`components/block.cljs:1870-1871`) |
| The second `(= prefix block-ref/left-parens)` branch is unreachable | `src/main/frontend/handler/editor.cljs:1949` and `:1954` | Its block-search step can never run |
| `replace-ref-with-embed!` still writes `{{embed ((uuid))}}` | `src/main/frontend/handler/editor.cljs:4313-4319` | Both forms are deprecated in DB graphs |
| Choosing a slash command compares against hard-coded English names, and `"Upload an image"` matches no label | `src/main/frontend/components/editor.cljs:133`, `:137`; label is `en.edn:568` | Also breaks in other languages |
| In a standalone `/Query`, `<% current page %>` resolves to today's journal | `src/main/frontend/worker/query_dsl.cljs:90` | `current-page-title` is set only for the journal default queries **Executed 2026-09-13**: with no opts the resolver returns `[[Sep 13th, 2026]]`, the run date's journal; with `:current-page-title` it returns `[[Some Page]]`. `parse-query` then resolves the property to `::no-property-found`, so such a query matches nothing. |
| A repeating task's "When" falls back to `status.done` when the checked property has no checkbox choice | `src/main/frontend/worker/commands.cljs:44-54` | The panel still shows "is: Done" |
| `mod+shift+h` inserts `==` while the rest of the app uses `^^` | `src/main/frontend/config.cljs:236-241` | **Checked 2026-09-12.** **Confirmed.** `:editor/highlight` (`shortcut/config.cljs:125`) calls `highlight-format!` (`handler/editor.cljs:171-174`), which wraps with `(config/get-highlight :markdown)` = `"=="`. Typing `^` twice inserts `^^` (`handler/editor.cljs:3424-3426`), and the shortcut help advertises `^^` (`components/shortcut_help.cljs:47`). One feature, two marks **Executed 2026-09-13**: `config/get-highlight :markdown` returns `"=="`, and `""` for `:org`. |
| `(between deadline …)` returns an empty query instead of filtering | `src/main/frontend/worker/query_dsl.cljs:300-310`; `deps/db/src/logseq/db/frontend/query_dsl.cljs:55-71` | **Checked 2026-09-12.** **Confirmed, with the mechanism.** The DSL's bare `deadline` becomes the unnamespaced `:deadline`. `property?` requires a namespace (`property.cljs:789-797`), and the fallback `case` maps only `created-at`/`updated-at`, so `get-timestamp-property` returns nil. The builder wraps everything in `when-let` (`:302`), so the whole clause returns nil. The built-in ident is `:logseq.property/deadline` (`property.cljs:340`) **Executed 2026-09-13** via `dz/scripts/query-dsl-probe/`: `parse-query "(between deadline -7d today)"` returns `{:query nil, :rules []}` — the whole query comes back empty, not merely unfiltered. |
| The Export screen's DB EDN export drops the options it passes | `deps/db/src/logseq/db/sqlite/export.cljs:1116-1119`, `:933-934` | **Checked 2026-09-12.** **Confirmed but narrower than written.** `export-repo-as-db-edn!` sends `{:export-type :graph :graph-options {:include-timestamps? true}}` (`handler/db_based/export.cljs:87-90`). `:graph` dispatches to `build-graph-datoms-export`, whose arity is `[db]` — no options. Only `:graph-human` passes `:graph-options` on. `:graph-options` is still read for `:catch-validation-errors?` (`:1122`), so it is not ignored wholesale |
| The Linked references **count** skips refs from the page's own blocks, but the **list** does not | `initial_data.cljs:235` vs `deps/db/src/logseq/db/common/reference.cljs:248-259` | A page referenced only by its own blocks hides the section |
| Whether the Unlinked references section appears is decided by the top 100 search hits, while its list scans every title | `src/main/frontend/worker/handler/block.cljs:524-534` vs `reference.cljs:309-326` | A stale index can hide real matches, or the section can render empty |
| The search group labelled "Recently updated" lists **visited** pages | `en.edn:259`; `src/main/frontend/components/cmdk/core.cljs:234-239`, `:300-311` | |

## Flashcards, PDF and code blocks

| What looks wrong | Where | Notes |
|---|---|---|
| The due-card query includes classes that extend `Card`, but the review UI's `card-block?` matches only `Card` itself | `src/main/frontend/worker/handler/flashcard.cljs:21-23` vs `src/main/frontend/extensions/fsrs.cljs:49-51` | Rating such a card likely saves nothing |
| `:logseq.property.fsrs/state` is still visible, with a "show for debug now, hide it later" TODO | `deps/db/src/logseq/db/frontend/property.cljs:615-619` | |
| Bracket matching in code blocks can never turn on | `src/main/frontend/extensions/code.cljs:374-389` vs `:422` | `text->cm-mode` returns a MIME type such as `text/x-clojure`; `lisp-like?` compares against `"clojure"` |
| A PDF highlight's colour change and area resize are not persisted | `src/main/frontend/extensions/pdf/assets.cljs:181-189` | `update-hl-block!` writes only `hl-color`, while the viewer reloads from `hl-value` (`:122-133`) |
| The PDF zoom level is written but never read back | `src/main/frontend/extensions/pdf/core.cljs:933-937` | Nothing reads `pdf-last-visit-scale/<id>` |
| Deleting an area highlight unlinks a file-graph path that DB graphs never create | `src/main/frontend/extensions/pdf/assets.cljs:191-201` | |

## Configuration keys with no effect

| What looks wrong | Where | Notes |
|---|---|---|
| `:quick-capture-options :prettify-url?` can never be set | `src/main/frontend/quick_capture.cljs:35-37` | The reader uses the singular `:quick-capture-option`; the schema uses the plural (`common_config.cljc:66`) |
| The Settings → Editor "Prefer pasting file" toggle does nothing | `settings.cljs:487-493`, `:910`; `state.cljs:732-734` | `11600c8dc3` removed its only reader from the paste path |
| `:ui/mask-email? false` never unmasks | `src/main/frontend/components/email.cljs:25-26`; `src/main/frontend/util/email.cljs:28-30` | The component passes the whole repo-keyed `:config` map, so the key is always nil |
| Keys with no reader anywhere: `:feature/enable-journals?`, `:ref/linkable-built-in-properties`, `:dwim/settings`, `:meta/version` (schema only), `:quick-capture-templates :media` | — | Confirmed by repo-wide grep for the keyword, the bare name and destructuring |
| The bundled `config.edn` template's comments give the wrong defaults for `:insert-today?` and `:default-page` | `deps/common/resources/templates/config.edn:232`, `:234` | Code defaults are `false` and unset (`quick_capture.cljs:29-57`) |

## Styling

| What looks wrong | Where | Notes |
|---|---|---|
| Two Tailwind accent fallbacks are missing their `var()` | `tailwind.config.js:8`, `:19` | They read `var(--lx-accent-01, --rx-gray-01)` and `…-12` |
| Eight CSS variables are read but defined nowhere in the repo | `--ls-error-text-color`, `--ls-bg-quaternary`, `--ls-quinary-background-color`, `--ls-senary-background-color`, `--ls-right-sidebar-topbar-color`, `--lx-red-11`, `--lx-popover-bg`, `--ls-win32-title-bar-height` | Each has one reader and zero definitions |
| `--lx-bg-override` is set twice but never read | `resources/css/shui.css:72`, `:123` | |
| A dead whiteboard rule still sets `--ls-*` backgrounds on the tldraw container | `resources/css/shui.css:111-116` | Whiteboards were removed in `bcc478b5f7` |

## Publishing

| What looks wrong | Where | Notes |
|---|---|---|
| Every published page links `/static/tabler-extension.css`, which no route serves | `deps/publish/src/logseq/publish/render.cljs:244`; definition commented out at `routes.cljs:18` | The request is **not** a 404: the router's `:else` builds a response from the HTML 404 page with no status (`routes.cljs:779-784`), so it returns **HTTP 200** with `text/html` |
| The Roam JSON exporter is inside a `(comment …)` form, yet the page still renders its hidden download anchor | `src/main/frontend/handler/export.cljs:180-191`; `components/container.cljs:594` | |

## Smaller UI and text issues

| What looks wrong | Where |
|---|---|
| "Submit a bug report" opens `github.com/logseq/db-test/issues/new`, not the public repo | `src/main/frontend/components/header.cljs:172` |
| Typo: "Please reload the app to in order to see the error" | `en.edn:683` |
| The mobile share fallback template contains a zero-width space and drops the colon | `src/main/frontend/mobile/intent.cljs:229` |
| Handbook search presses Enter with `(nth results selected)` and no empty-result guard | `src/main/frontend/extensions/handbooks/core.cljs:394` |
| The Assets settings panel exists but has no entry in the settings tab list | `settings.cljs:1535-1536` vs `:1476-1494` |
| Pro storage is "5GB each" in the account string but 10 GB in the settings calculation | `en.edn:52` vs `settings.cljs:951` |
| The pending-server-changes counter carries a `FIXME` | `src/main/frontend/components/rtc/indicator.cljs:213-214` |
| Non-managers see the invite field and get a generic error on the 403 | `deps/db-sync/.../handler/index.cljs:186-191` |
| The mobile Export route has no link; the only one is commented out | `src/main/mobile/components/header.cljs:65` |
| `list-backups` has no callers; `get-db-backups-path` is called only by tests | `db_worker/graph_backup.cljs:161`; `deps/db/src/logseq/db/common/sqlite.cljs:35-38` |
| The desktop SQLite export filename keeps the `logseq_db_` prefix while the zip export strips it | `src/main/frontend/handler/export.cljs:50-54`, `:148` vs `:80-82` |
| "DB backup successfully." is not translated | `src/main/frontend/persist_db.cljs:439` |
| The `built-in-properties` docstring lists only `:page :block :never` as view contexts; the schema also allows `:class` and `:property` | `property.cljs:30-32` vs `malli_schema.cljs:349` |
| The built-in `:block/closed-value-property` entry has no `:cardinality :many`, but the DataScript attribute is many | `schema.cljs:100-101` |
| A `/` page created by saving a block may not be split into namespace segments, while the popup path splits it | `deps/outliner/src/logseq/outliner/core.cljs:205-208` vs `src/main/frontend/handler/page.cljs:281` |
| The import page still renders "First you need to choose a folder…", which no longer matches DB graphs | `src/main/frontend/components/imports.cljs:564`; `onboarding/setups.cljs:19` |
| Opening a non-PDF Zotero attachment may silently do nothing on macOS/Linux | `src/electron/electron/window.cljs:134-165` |

## Markdown Mirror and paste

Found by a separate session while writing `ddocs/reference/syntax/markdown-mirror-syntax.md` and
`paste-and-type-syntax.md` (`22b3d5966c`, `0f2363985f`). **These carry better evidence than the rest
of this page**: the mirror bugs were reproduced on `be800f1711` *and* on desktop 2.0.1 (`b09316a`),
and the paste defects come from a probe run (`dz/scripts/markdown-mirror-probe/`), not from reading
alone. Re-run that probe before filing; the pages' own "Known bugs" and "Known issues" sections carry
the detail.

| What looks wrong | Where | Evidence |
|---|---|---|
| A math block, or a block whose text starts with a blank line, desyncs the whole rest of the page: later blocks lose their status word, tags and `1.` numbering, and block embeds stop expanding | `src/main/frontend/worker/markdown_mirror.cljs:364-379` | **Reproduced** on `be800f1711` and desktop 2.0.1 (blank-line case on `be800f1711`). The renderer matches each written line to its block by the block's first line of text; `$$` and a trimmed blank first line both fail to match, so every later block is compared against the wrong one |
| A tag on a code block is written inside the opening fence (```` ```js #Book ````), so other Markdown tools read it as part of the language | same page, "Known bugs" | **Reproduced** |
| "No priority" writes an empty value block (`* Priority::` followed by an empty item) | same page, "Known bugs" | **Reproduced** |
| Pasted `#tag` becomes a page link instead of a tag | `src/main/frontend/handler/paste.cljs:59-66` | **Probe**, not yet confirmed in the app |
| Pasted `[#A]` is mangled to `[[[A]]]` and creates a page named `A]` | same | **Probe** |
| Pasting `((uuid))` for a block that is not in the graph fails the entire paste | same | **Probe** |
| HTML containing `<figure>`, `<aside>`, `<header>`, `<footer>`, `<center>` or `<fieldset>` throws and falls back to plain text, losing code-block languages, numbered lists, checkboxes and more | same | **Probe** |

## Ruled out

- **`getBlockProperties` returning unconverted sets.** Raised in wave 4, withdrawn in wave 7. The
  plugin API audit followed cljs-bean's `->js`, which delegates to `clj->js` and turns sets into
  arrays. A source comment suggesting otherwise looks stale. Not reproduced either way.
- **A crash when cycling a task's status.** Raised by an agent in wave 3 and rejected: the `case`
  has a trailing default.
- **Typing `key:: value` breaking block saves.** Raised and withdrawn by the mirror/paste session
  (`0f2363985f`). The probe that "found" it ran without redefining `state/get-current-repo` to a
  `logseq_db_…` repo, so mldoc used **file-graph** parse options (drawers, markers, priorities on).
  The owner confirmed in the app that such text simply saves as text. Worth remembering for any
  future probe: parse options come from the current repo (`gp-mldoc/get-default-config`).

## Revalidation log

**2026-09-12 — four "re-locate before filing" entries resolved.** The wave-3 and wave-4 entries that
had no citation were tracked down: the highlight-mark mismatch and the `(between deadline …)` clause
are confirmed, with the mechanism now written down; the EDN export entry is narrowed to the `:graph`
export type; `:thread-api/get-version` is downgraded to an unused name, since nothing defines or
calls it. Details are in the rows above.

**2026-09-12 — reachability pass, 6 entries.** For the security and data-loss tables above, checked
whether a user-facing path reaches the cited line: who writes the config a check reads, whether an
argument is sanitised before use, and whether the function has a non-test caller. Results are folded
into those rows. One entry got sharper (Recycle: the block branch is unreachable, not merely
unhelpful); none was withdrawn.

**2026-09-13 — destructive entry reproduced against the CLI.** The `graph backup remove --src`
traversal is confirmed on the installed build, using a decoy directory rather than the graph itself:
a non-existent traversal target is echoed back unvalidated, and an existing one outside `backup/` is
deleted. The row above carries the detail. Method worth reusing: prove the dangerous half on
something disposable created for the purpose, never on real data.

**2026-09-13 — first executed evidence.** Three entries no longer rest on reading. A probe
(`dz/scripts/query-dsl-probe/`) confirmed the `(between deadline …)` empty query, the
`<% current page %>` journal resolution, and the `==` highlight mark. It ran on a checkout whose
copies of the exercised files are byte-identical to `be800f1711`, needs no graph on disk, and writes
nothing. The rows above carry the results.

**2026-09-13 — "current master" step satisfied, not pending.** `git fetch origin` brought nothing
new, and a direct remote query returns `be800f1711` for `refs/heads/master` on both `origin` (the
`hdansou/logseq` fork) and `upstream` (`logseq/logseq`, head dated 2026-09-08). Every line cited on
this page was therefore read at what is still the newest code. Repeat this check when upstream
advances: a defect fixed upstream should be struck from the list, not filed.

**Still not done for any entry:**

- **Blind live reproduction.** Nothing here has been run in the app. Several entries involve
  deleting data (Recycle, asset removal, `graph backup remove`), so reproduce them against a scratch
  graph, never a real one.

Live reproduction remains required before filing ([[feedback-revalidate-own-findings]] in session memory; the rule
is that 7 of 11 findings changed verdict the last time this was done properly).

## Related

- `dz/docs/TASK_LIST.md` — the audit state of every page.
- `dz/docs/LINK_TRIAGE.md` — the link backlog, closed 2026-09-12.
