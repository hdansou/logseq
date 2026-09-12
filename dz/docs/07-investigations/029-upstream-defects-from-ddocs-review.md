# Upstream Defects Found During the ddocs Review Pass

> **Last revised:** 2026-09-12
> **Verified against:** `origin/master` @ `be800f1711`. **Source read only.** Nothing here was
> reproduced in a running app unless the entry says so.
> **Context:** waves 1–7 of the documentation review audited every page in `dz/docs/ddocs` against
> source. Along the way the audits found behaviour that looks like a product defect rather than a
> documentation error. They were recorded in commit messages and session memory; this page collects
> them so they can be triaged in one place.

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
| Before any API token is saved, the desktop HTTP/MCP server accepts **any** `Authorization` value, and CORS allows every origin | `src/electron/electron/server.cljs:86`, `:164` | A request with **no** header still gets 401, because `string/replace` on nil throws inside the try (`:96-103`). So the hole is "any value passes", not "no auth" |
| `graph backup remove --src` joins its argument onto the backup directory and removes the tree, with no path check | `cli/lib/graph.ml:469-470`, `:753-758` | The ClojureScript backup code guards the same operation with `child-path!` (`db_worker/graph_backup.cljs:23-37`) |
| Inviting a user who has never set an E2EE password to an E2EE graph creates the membership first, then fails to wrap the key | `deps/db-sync/.../sync.cljs:392-398`, `crypt.cljs:678-679` | The invitee ends up a member who cannot decrypt; the manager sees a generic error |
| `custom.js` approval is keyed on the relative path `logseq/custom.js`, which is identical for every DB graph | `src/main/frontend/handler/ui.cljs:131-136`, `src/main/frontend/config.cljs:374-379` | Approving one graph's script approves every graph's for 7 days. Cancel stores `false` permanently with no UI to undo (`ui.cljs:139-140`, `:153-154`) |

## Data loss and recovery

| What looks wrong | Where | Notes |
|---|---|---|
| Recycle promises deleted **blocks** can be restored, but deleting a block retracts it outright | `src/resources/dicts/en.edn:1824`; `deps/outliner/src/logseq/outliner/core.cljs:440-442` | `recycle-blocks-tx-data` (`recycle.cljs:127`) has only test callers |
| A pending sync transaction whose rebase fails or becomes a no-op is marked non-pending and never uploaded | `src/main/frontend/worker/sync/apply_txs.cljs:1704-1710` | Only `:block/title` is conflict-tracked (`:1130-1131`), so edits to other attributes may be lost without a conflict entry |
| "Remove the file too" deletes only the asset block; the file stays in `assets/` | `src/main/frontend/handler/editor.cljs:1741-1753`; checkbox text `en.edn:97` | The platform function `:asset-delete!` exists in both adapters but has no callers |
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
| `:thread-api/get-version` is undefined | — | Found in wave 4; re-locate before filing |

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
| In a standalone `/Query`, `<% current page %>` resolves to today's journal | `src/main/frontend/worker/query_dsl.cljs:90` | `current-page-title` is set only for the journal default queries |
| A repeating task's "When" falls back to `status.done` when the checked property has no checkbox choice | `src/main/frontend/worker/commands.cljs:44-54` | The panel still shows "is: Done" |
| `mod+shift+h` inserts `==` while the rest of the app uses `^^` | — | Found in wave 3; re-locate before filing |
| `get-timestamp-property` nils `(between deadline …)` | — | Found in wave 3; re-locate before filing |
| EDN export ignores its options | — | Found in wave 3; re-locate before filing |
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

## Ruled out

- **`getBlockProperties` returning unconverted sets.** Raised in wave 4, withdrawn in wave 7. The
  plugin API audit followed cljs-bean's `->js`, which delegates to `clj->js` and turns sets into
  arrays. A source comment suggesting otherwise looks stale. Not reproduced either way.
- **A crash when cycling a task's status.** Raised by an agent in wave 3 and rejected: the `case`
  has a trailing default.

## Related

- `dz/docs/TASK_LIST.md` — the audit state of every page.
- `dz/docs/LINK_TRIAGE.md` — the link backlog, closed 2026-09-12.
