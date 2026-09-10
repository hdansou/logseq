# Documentation Changelog

Track all documentation updates performed during the March 2026 review cycle.

## Format

Each entry: `[DATE] [FILE] - [CHANGE SUMMARY]`

---

## Changes

### 2026-09-10 — New investigation

- [2026-09-10] `07-investigations/027-login-and-account-features.md` — **added**: why Logseq has a login when graphs are local, what each account-gated feature needs (sync, collaboration, encryption, publish, early access), and how far the plan limits are really enforced.
- [2026-09-10] `07-investigations/027-login-and-account-features.md` — **corrected** after a full re-verification: login goes through the Amplify modal (the cited Cognito password function is a test helper); `/file-sync/user_info` is the live user-info endpoint, not legacy; RSA keys are ensured for every logged-in user and can prompt for a password; sync graphs are always encrypted from the app; invites are manager-only on the server; the no-plan-enforcement claim is now verified directly instead of cited from 025.
- [2026-09-10] `07-investigations/027-login-and-account-features.md` — **updated**: added who is in `rtc_2025_07_10` today (Open Collective backers at $5 and sponsors at $15, until Pro launches; supplied, not in source), the Settings login prompt that names Open Collective, and that the dormant `pro-account?` check ignores that group.
- [2026-09-10] `07-investigations/028-default-image-size.md` — **added**: why images render at a hard-coded 250px, which asset properties resizing actually writes (`resize-metadata`, not `width`/`height`), why `config.edn` can't set a default, and a `custom.css` workaround that restyles only un-resized images.

### 2026-05-30 — Full Re-audit (Batch 17: ddocs/how-to tail + reference — 8 of 8 completed)

- [2026-05-30] `ddocs/how-to/integrations/custom-js.md` — **updated**: aligned custom-js guidance to current runtime surface, removed stale sandbox framing, and documented current built-in file/API capabilities.
- [2026-05-30] `ddocs/how-to/integrations/publishing.md` — **updated**: retained current publish flow and added current page-toolbar launch capability details.
- [2026-05-30] `ddocs/how-to/integrations/x-callback-url.md` — **updated**: corrected fallback/append behavior and refreshed x-callback options for current runtime.
- [2026-05-30] `ddocs/how-to/integrations/zotero.md` — **updated**: removed stale bulk-import/append assumptions and documented current profile-aware and attachment-resolution behavior.
- [2026-05-30] `ddocs/how-to/multimedia/asset-management.md` — **updated**: aligned asset schema/sync behavior to current source and documented current media/render/sync capabilities.
- [2026-05-30] `ddocs/how-to/multimedia/video-embed.md` — **updated**: refreshed provider support and removed stale Dropbox/Google Drive embed guidance.
- [2026-05-30] `ddocs/how-to/queries/advanced-queries.md` — **updated**: added current query grouping/date-time/result-transform capabilities and refreshed revision metadata.
- [2026-05-30] `ddocs/how-to/tasks/repeating-tasks.md` — **updated**: corrected stale setup wording and aligned repeat/repeat-type/checked-property semantics to current behavior.

### 2026-05-30 — Full Re-audit (Batch 16: ddocs/how-to + reference — 8 of 8 completed)

- [2026-05-30] `ddocs/how-to/backup/backup-restore.md` — **updated**: aligned backup/restore guide to current graph-backup layout, hourly scheduling, force/manual backup behavior, and source-aware pruning.
- [2026-05-30] `ddocs/how-to/content/create-flashcard-deck.md` — **updated**: refreshed flashcard deck creation/tutorial language for current DB FSRS behavior and removed legacy file-based scheduling guidance.
- [2026-05-30] `ddocs/how-to/content/favorites-recent.md` — **updated**: corrected recent-pages behavior and mobile navigation paths, removed stale property/restart assumptions.
- [2026-05-30] `ddocs/how-to/content/quick-capture.md` — **updated**: aligned quick-capture flow/config to current runtime (`insert-today?`, `page=TODAY`, `prettify-url?`) and removed stale in-app/widget claims.
- [2026-05-30] `ddocs/how-to/content/view-types.md` — **updated**: refreshed view examples and DB status/priority values, and documented current `:query/result-transforms` capability.
- [2026-05-30] `ddocs/how-to/integrations/cli-basic.md` — **updated**: replaced stale CLI install/command model with current `src/main/logseq/cli` command surface and current graph/sync/query workflows.
- [2026-05-30] `ddocs/how-to/integrations/create-theme.md` — **updated**: aligned theming guide to current token contract and runtime hooks, removing stale Radix/accent assumptions.
- [2026-05-30] `ddocs/how-to/integrations/custom-css.md` — **updated**: corrected custom-css integration behavior for current settings/file paths and documented current DB-graph custom.css capabilities.

### 2026-05-30 — Full Re-audit (Batch 15: ddocs explanation + tutorials — 8 of 8 completed)

- [2026-05-30] `ddocs/tutorials/collaboration/real-time-sync.md` — **updated**: corrected stale invite/presence/conflict/offline-sync assumptions and aligned tutorial flow with current member-based RTC/db-sync behavior.
- [2026-05-30] `ddocs/tutorials/getting-started/01-installation.md` — **updated**: refreshed platform minimums and removed stale iOS/Android support-floor claims.
- [2026-05-30] `ddocs/tutorials/getting-started/02-first-graph.md` — **updated**: aligned first-graph onboarding to current DB-first flows, removed stale graph-selection/performance absolutes, and documented current import options.
- [2026-05-30] `ddocs/tutorials/getting-started/03-basic-editing.md` — **updated**: corrected stale slash-command examples for references/embeds and added currently available editing shortcuts/commands.
- [2026-05-30] `ddocs/tutorials/getting-started/04-linking-pages.md` — **updated**: corrected block-ref/namespace/graph-view/linking claims to current behavior and marked stale merge/link assumptions as removed.
- [2026-05-30] `ddocs/tutorials/getting-started/semantic-search.md` — **updated**: replaced stale fixed-weight and positional CLI guidance with current RRF-based ranking and current CLI subcommand search behavior.
- [2026-05-30] `ddocs/tutorials/knowledge-management/flashcards-basics.md` — **updated**: aligned terminology/flow to current FSRS UI behavior and added current cloze cue/study-step behavior.
- [2026-05-30] `ddocs/tutorials/queries/charts-with-hiccup.md` — **updated**: removed stale SCI troubleshooting assumptions, corrected property-ident guidance, and added current reusable query-view/result-transform capability notes.

### 2026-05-30 — Full Re-audit (Batch 14: 08-processes tail + ddocs/explanation — 8 of 8 completed)

- [2026-05-30] `ddocs/explanation/architecture/data-architecture.md` — **updated**: corrected stale indexing/validation/performance claims, aligned storage/index behavior to current schema/runtime, and added current capability findings.
- [2026-05-30] `ddocs/explanation/architecture/system-overview.md` — **updated**: refreshed collaboration/sync framing to current RTC/db-sync behavior, removed stale file-sync-cloud assertions, and added newer sync/E2EE/backend capability notes.
- [2026-05-30] `ddocs/explanation/comparisons/rtc-vs-file-sync.md` — **updated**: marked legacy File Sync behavior as removed and aligned comparison to current RTC/db-sync conflict, presence, and protocol behavior.
- [2026-05-30] `ddocs/explanation/concepts/file-vs-db.md` — **updated**: tightened stale absolute claims, removed outdated migration details, and documented current DB sync/readiness/E2EE runtime capabilities.
- [2026-05-30] `ddocs/explanation/features/handbooks-system.md` — **updated**: removed stale shortcut/authoring guarantees, aligned deep-link/watch behavior to current implementation, and added currently supported handbook capabilities.
- [2026-05-30] `ddocs/explanation/features/how-embeddings-work.md` — **updated**: replaced outdated inference-worker/hnsw framing with current sidecar/vector/rrf implementation details and removed stale parameter/property claims.
- [2026-05-30] `ddocs/explanation/features/how-publishing-works.md` — **updated**: corrected stale trigger/location assertions, clarified current URL fallback and rendering behavior, and added current publishing capabilities.
- [2026-05-30] `ddocs/explanation/security/e2ee-encryption.md` — **updated**: corrected encrypted-field scope/storage/flow claims, removed stale OPFS/full-payload assumptions, and documented current password-rotation/key-reset/grant flows.

### 2026-05-30 — Full Re-audit (Batch 13: investigations tail + 08-processes — completed)

- [2026-05-30] `08-processes/changelog/readme.md` — **updated**: corrected stale canonical links and prompt/script references, removed legacy capability labels, and documented current changelog pipeline scope.
- [2026-05-30] `08-processes/changelog/system-summary.md` — **updated**: aligned summary to current changelog extension scripts and active capability areas; removed stale workflow/prompt/path references.
- [2026-05-30] `08-processes/changelog/workflow.md` — **updated**: removed stale from-file invocation guidance and documented current extension/no-merge/dry-run workflow behavior.
- [2026-05-30] `08-processes/dependency-management/eol-report.md` — **updated**: refreshed dependency versions/risk posture to current manifests and removed stale March-era upgrade urgency claims.
- [2026-05-30] `08-processes/documentation/update-process.md` — **updated**: refreshed process paths/commands/version assumptions to current repo reality, removed stale file references, and documented newer plugin API capabilities.
- [2026-05-30] `08-processes/documentation/plan.md` — **updated**: corrected stale completed-file assumptions, updated platform/runtime wording, and added current capability findings.

### 2026-05-30 — Full Re-audit (Batch 12: schema integrity tail — 8 of 8 completed)

- [2026-05-30] `07-investigations/block-node-history-query-scenarios.md` — **updated**: removed stale transport-detail assumptions, aligned worker/property-history references, and documented current validation/export/cleanup support around history data.
- [2026-05-30] `08-processes/import-export-best-practices.md` — **updated**: refreshed import/export model to current sqlite/export/build-import flows, current CLI backup/import/export capabilities, and removed stale JSON/version-dispatch guidance.
- [2026-05-30] `08-processes/rtc-sync-schema-changes.md` — **updated**: removed accidental duplicated document blocks/date markers, kept a single canonical revision header, and revalidated current sync/schema compatibility paths.
- [2026-05-30] `08-processes/schema-change-pr-checklist.md` — **updated**: refreshed checklist to current migration keys, validation/property-value paths, import/export ownership, and current sync/RTC assumptions.
- [2026-05-30] `08-processes/schema-change-testing.md` — **updated**: removed duplicated stale tail content, normalized revision markers, and revalidated all current schema/migration/validation/sync test anchors.
- [2026-05-30] `08-processes/schema-migration-best-practices.md` — **updated**: corrected stale user-facing property type list to include `:asset` and retained current migration/bootstrap guidance.
- [2026-05-30] `08-processes/validation-best-practices.md` — **updated**: aligned architecture and API references to current validation paths (`validate-tx-report`, `validate-db`, `validate-local-db!`) and removed stale flag/dispatch/benchmark claims.
- [2026-05-30] `08-processes/changelog/quick-reference.md` — **updated**: replaced removed prompt/workflow references, added current changelog format/tooling coverage, and documented contributor/noise-filter script support.

### 2026-05-30 — Full Re-audit (Batch 11: ios + schema integrity — 8 of 8 completed)

- [2026-05-30] `07-investigations/schema-integrity-03-migration-system.md` — **updated**: refreshed migration source-of-truth, runtime trigger, current migration keys, repair flows, and removed outdated helper/file assumptions.
- [2026-05-30] `07-investigations/schema-integrity-04-import-export-validation.md` — **updated**: aligned import/export validation to current sqlite export/build/import pipeline, current schema version, and current CLI/import/export capabilities.
- [2026-05-30] `07-investigations/schema-integrity-05-rtc-synchronization.md` — **updated**: replaced stale branch-based RTC narrative with current db-sync/worker-sync architecture, rebase/apply/client-op persistence flow, and removed obsolete RTC helper claims.
- [2026-05-30] `07-investigations/schema-integrity-06-failure-patterns-recommendations.md` — **updated**: corrected export/import/RTC failure narratives to current code, removed obsolete recovery APIs and legacy RTC references, and added current snapshot/checksum/tx-reject behavior.
- [2026-05-30] `07-investigations/schema-integrity-07-pr-review-standards.md` — **updated**: refreshed review guidance to current migration registry, validation APIs, major-version RTC compatibility checks, and current post-migration repair/export hooks.
- [2026-05-30] `07-investigations/schema-integrity-08-index.md` — **updated**: refreshed index references to current schema/migration/validation/sync files and current schema head 65.33, removing deleted file references.
- [2026-05-30] `07-investigations/transaction-failure-logseq-class-page-missing.md` — **updated**: aligned bootstrap/page-creation snippets to current code and documented current mitigation/repair behavior.
- [2026-05-30] `07-investigations/x-callback-url-comprehensive-investigation.md` — **updated**: refreshed desktop-only x-callback scope, current quickCapture/invokeCommand support, removed stale callback/mobile assumptions, and documented current deep-link distinctions.

### 2026-05-30 — Full Re-audit (Batch 10: 07-investigations continued — 8 of 8 completed)

- [2026-05-30] `07-investigations/pdf-feature-platform-compatibility-investigation.md` — **updated**: corrected stale platform/persistence assertions, aligned architecture to DB-centered annotation flow, and replaced issue-based claims with source-validated statuses.
- [2026-05-30] `07-investigations/published-url-property-validation-investigation.md` — **updated**: refreshed metadata, removed stale dispatch-guard claim, and documented current publish fallback URL persistence behavior.
- [2026-05-30] `07-investigations/quick-capture-callback-url-investigation.md` — **updated**: aligned desktop/mobile callback behavior, removed stale media-output guarantees, and documented invokeCommand and current quick-capture options.
- [2026-05-30] `07-investigations/rtc-e2ee-security-investigation.md` — **updated**: corrected crypto/transport defaults and legacy-path assumptions, and documented current PBKDF2 compatibility and db-sync architecture.
- [2026-05-30] `07-investigations/scheduled-deadline-db-graphs.md` — **updated**: corrected stale query patterns (between-based behavior), removed outdated task/property assumptions, and refreshed source references.
- [2026-05-30] `07-investigations/schema-evolution-and-data-migration.md` — **updated**: refreshed schema/version and migration references to current paths/version (65.33) and documented current migration/export capabilities.
- [2026-05-30] `07-investigations/schema-integrity-01-executive-summary.md` — **updated**: refreshed stale schema/version/RTC references, removed obsolete migration file assumptions, and added current integrity capability deltas.
- [2026-05-30] `07-investigations/schema-integrity-02-schema-system.md` — **updated**: refreshed schema-version/model/migration window to current implementation, replaced removed file paths, and added current schema-system capabilities.

### 2026-05-30 — Full Re-audit (Batch 9: 07-investigations mid — 8 of 8 completed)

- [2026-05-30] `07-investigations/LOGSEQ-THEME-CSS-REFERENCE.md` — **updated**: corrected stale token/variable claims, removed outdated mode/variable assertions, and documented current palette/alpha/semantic token bridges.
- [2026-05-30] `07-investigations/PLUGIN-UI-INTEGRATION-GUIDE.md` — **updated**: corrected stale source paths and API naming, removed unsupported icon-registration claims, and added current command-bus/style registration behavior.
- [2026-05-30] `07-investigations/cmd-r-reload-persistence.md` — **updated**: removed stale write-queue and periodic-export assumptions, aligned reload persistence investigation to current debounce/mirror/worker behavior.
- [2026-05-30] `07-investigations/cmd-s-save-db-persistence.md` — **updated**: aligned Cmd+S flow to current backup-oriented save pipeline and thread APIs; removed legacy force-save/periodic-export claims.
- [2026-05-30] `07-investigations/calculator-feature-investigation.md` — **updated**: refreshed stale process/tracking references, retained verified calculator behavior, and documented current mobile command-surface evidence.
- [2026-05-30] `07-investigations/custom-css-customization-guide.md` — **updated**: removed stale watcher/path assumptions, aligned to current trigger points and DB/API custom.css capability.
- [2026-05-30] `07-investigations/custom-js-customization-guide.md` — **updated**: removed stale watcher/UI path claims, aligned loading/permission behavior, and documented DB file API support and seeding.
- [2026-05-30] `07-investigations/database-backup-electron.md` — **updated**: reworked to current worker snapshot backup architecture, per-backup metadata layout, and source-aware retention behavior.

### 2026-05-29 — Full Re-audit (Batch 8: 06-user-guides + investigations early — 8 of 8 completed)

- [2026-05-29] `06-user-guides/cli-user-guide.md` — **updated**: refreshed CLI command surface, removed legacy install/positional syntax claims, and added current graph backup/SQLite export/sync/query utility capabilities.
- [2026-05-29] `06-user-guides/database-backup-guide.md` — **updated**: corrected backup locations/retention timing and removed stale scheduled-folder/close-time backup claims.
- [2026-05-29] `06-user-guides/logseq-db-markdown-guide.md` — **updated**: corrected DB block/page reference forms, removed duplicate-name claims, and aligned DB vs file behavior wording.
- [2026-05-29] `06-user-guides/root-tag-guide.md` — **updated**: clarified root-tag auto-assignment and bootstrap set, removed stale non-deletable-root claim, and documented current class-filter capability.
- [2026-05-29] `06-user-guides/view-guide.md` — **updated**: refreshed the archived view walkthrough with current query-result rendering, tables/lists/gallery, and input-resolution capabilities.
- [2026-05-29] `06-user-guides/x-callback-url-user-guide.md` — **updated**: aligned desktop x-callback docs to current quickCapture/invokeCommand behavior and removed stale mobile walkthrough claims.
- [2026-05-29] `07-investigations/022-logseq-cli-semantic-search.md` — **updated**: marked legacy semantic/hybrid CLI search claims as removed and aligned to current CLI subcommand search behavior.
- [2026-05-29] `07-investigations/LOGSEQ-DB-AST-GUIDE.md` — **updated**: removed stale JSON export claim, refreshed parser/export notes, and added current AST/export capabilities.

### 2026-05-29 — Full Re-audit (Batch 7: plugin + theme dev guides — 8 of 8 completed)

- [2026-05-29] `05-developer-guides/plugin-development/clojurescript-guide.md` — **updated**: refreshed SDK versioning, wrapper counts, dependency prerequisites, regeneration flow, and documented current command/app/db/ui/asset APIs.
- [2026-05-29] `05-developer-guides/plugin-development/database-guide.md` — **updated**: corrected DB-plugin API version/behavior claims, added current property/tag/query helpers, and removed outdated coercion/limitations guidance.
- [2026-05-29] `05-developer-guides/plugin-development/db-graphs.md` — **updated**: added current DB plugin API coverage (`addPropertyValueChoices`, `setPropertyNodeTags`, tag-extents, `DB.customQuery`) and removed stale limitations.
- [2026-05-29] `05-developer-guides/plugin-development/file-graphs.md` — **updated**: refreshed file-graph plugin API coverage, current query/property helpers, and removed obsolete datascript/query claims.
- [2026-05-29] `05-developer-guides/plugin-development/index.md` — **updated**: corrected DB-collaboration wording, added current tag/property/icon APIs, and refreshed the plugin API matrix.
- [2026-05-29] `05-developer-guides/plugin-development/typescript-guide.md` — **updated**: refreshed SDK versioning, command bus/additional namespaces, and asset capability examples.
- [2026-05-29] `05-developer-guides/theme-development/guide.md` — **updated**: corrected removed mode-hook/CSS-var claims, updated plugin-theme package examples, and added current theme API capabilities.
- [2026-05-29] `05-developer-guides/theme-development/overview.md` — **updated**: aligned overview language to current Shui/token-layer theme implementation and added publish-theme capabilities.

### 2026-05-29 — Full Re-audit (Batch 6: 05-developer-guides — 8 of 8 completed)

- [2026-05-29] `05-developer-guides/cli-development/developer-guide.md` — **updated**: replaced stale `deps/cli` and old token/API command model with current `src/main/logseq/cli` architecture, current command families, output modes, backup/auth/server/agent capabilities.
- [2026-05-29] `05-developer-guides/cli-development/edn-format-reference.md` — **updated**: refreshed EDN import/export command surface, export payload shape, property/class schema keys, user property types, and documented current EDN/SQLite dual import-export plus property-history payloads.
- [2026-05-29] `05-developer-guides/cli-development/onboarding.md` — **updated**: aligned onboarding to current CLI v2 architecture, root build/test workflow, and removed stale `deps/cli` entrypoint guidance.
- [2026-05-29] `05-developer-guides/core-development/contents-page-guide.md` — **updated**: corrected graph-scope auto-creation claims and added current Contents toggle shortcut/command integration.
- [2026-05-29] `05-developer-guides/mcp-development/testing-guide.md` — **updated**: replaced stale CLI-MCP testing flow with current Desktop API `/mcp` streamable-HTTP/session lifecycle testing and current tool surface.
- [2026-05-29] `05-developer-guides/mcp-development/what-is-mcp.md` — **updated**: aligned MCP explanation with current desktop Electron `/mcp` implementation, settings integration, and removed stale CLI/SSE/tool-name claims.
- [2026-05-29] `05-developer-guides/plugin-development/README.md` — **updated**: refreshed tutorial navigation, SDK/runtime capabilities, DB tag/class APIs, renderer caveats, and package-manager guidance.
- [2026-05-29] `05-developer-guides/plugin-development/clojurescript-detective.md` — **updated**: corrected CLJS wrapper/generator behavior, namespace/version counts, regeneration flow, and documented newer commands/experiments/net/UI/app-event wrappers.

### 2026-05-29 — Full Re-audit (Batch 5: task-management + 04-api-reference — 8 of 8 completed)

- [2026-05-29] `03-features/task-management/system.md` — **updated**: replaced stale dual-engine and shortcut/workflow claims with current DB-native task model, validated task cycling/slash/property actions, and documented current task CLI/query helpers.
- [2026-05-29] `04-api-reference/command-palette-reference.md` — **updated**: corrected built-in command total (145), marked removed command IDs (`:command/run`, `:command/toggle-favorite`), and added current command-palette extension/runtime notes.
- [2026-05-29] `04-api-reference/config-edn-db-reference.md` — **updated**: corrected deprecated DB-only config counts/behavior, removed stale `:dwim/settings` guidance, and added `:quick-capture-options/:prettify-url?`.
- [2026-05-29] `04-api-reference/config-edn-reference.md` — **updated**: marked removed `:graph/settings`, `:graph/forcesettings`, and `:dwim/settings`, documented `:prettify-url?`, and clarified `:feature/enable-journals?` as legacy/no-op.
- [2026-05-29] `04-api-reference/http-api/server-guide.md` — **updated**: corrected desktop `/mcp` session behavior and auth wording, removed stale resources/prompts claim, and refreshed examples to current tool names.
- [2026-05-29] `04-api-reference/mcp-server/guide.md` — **updated**: aligned guide to current desktop MCP implementation, tool surface (6 tools), session lifecycle, DNS-rebinding protection, and removed stale CLI/tooling references.
- [2026-05-29] `04-api-reference/plugin-api/changes-2024-2025.md` — **updated**: marked removed stale App/template APIs, corrected `createTag` options, and documented newer property/tag/icon APIs.
- [2026-05-29] `04-api-reference/plugin-api/overview.md` — **updated**: corrected marketplace update behavior and added current DB-graph/plugin editor capabilities.

### 2026-05-29 — Full Re-audit (Batch 4: query-system tail + features — 8 of 8 completed)

- [2026-05-29] `03-features/query-system/query-performance.md` — **updated**: corrected DB graph property/text/index guidance, removed stale indexed-vs-unindexed claims and unverifiable measurements, and added current rule-injection/domain-binder behavior.
- [2026-05-29] `03-features/query-system/result-transform-dev.md` — **updated**: refreshed developer guidance for current SCI-backed transform pipeline, config registry, grouping behavior, helper bindings, and removed fabricated debug/benchmark claims.
- [2026-05-29] `03-features/query-system/result-transform-user.md` — **updated**: corrected `remove-block-children` defaults and SCI/JS FAQ claims, added keyword-transform lookup, helper bindings, and graph-type child-pruning behavior.
- [2026-05-29] `03-features/query-system/rules.md` — **updated**: removed stale file-vs-DB split/rule-count narrative, aligned `add-rules-to-query` behavior to current code, and scoped `or-join`/`not-join` limitations accurately.
- [2026-05-29] `03-features/query-system/syntax-db-vs-file.md` — **updated**: corrected `{{query}}` support, task-marker normalization, `/Query` DB-native authoring flow, and removed references to deleted file-based query component paths.
- [2026-05-29] `03-features/quick-capture/guide.md` — **updated**: added `prettify-url?`, corrected active template/runtime behavior, documented current config-key/runtime mismatch, and refreshed mobile/desktop quick-capture flow details.
- [2026-05-29] `03-features/semantic-search/developer-guide.md` — **updated**: replaced stale `vec-search` and inference-worker architecture with current hybrid keyword+vector search, `:thread-api/search-*` APIs, runtime-dependent embedding/indexing, and macOS-gated sidecar behavior.
- [2026-05-29] `03-features/tagging-classification/system-guide.md` — **updated**: removed speculative sections, refreshed schema/version references, and documented current comments/query bootstrap behaviors plus asset property support.

### 2026-05-29 — Full Re-audit (Batch 3: 03-features continued — 8 of 8 completed)

- [2026-05-29] `03-features/graph-validation/developer-guide.md` — **updated**: corrected stale thread-API and shortcut line references, `validate-db!`/`validate-tx-report!` names, `:skip-validate-db?` tx-meta key, removed fabricated `:block/warning` auto-fix claim, and documented `validate-local-db!` plus broader `fix-invalid-blocks!` coverage.
- [2026-05-29] `03-features/namespaces-libraries/developer-guide.md` — **updated**: replaced stale file-based namespace model and plugin API claims with current outliner-driven slash namespace creation, parent-type validation rules, current Library UI/search behavior, and removed obsolete migration/runtime references.
- [2026-05-29] `03-features/namespaces-libraries/system-overview.md` — **updated**: rewrote overview to current implementation truth; removed obsolete `file_based` rules/UI/model paths and namespace query-operator claims; added outliner `:split-namespace?`, search/library behavior, and current test-backed namespace creation flow.
- [2026-05-29] `03-features/namespaces-libraries/user-guide.md` — **updated**: corrected DB slash-page behavior (automatic parent materialization in create flows), removed stale file-based hierarchy component guidance, clarified rename expectations, and documented current Library add-page/search behavior.
- [2026-05-29] `03-features/plugin-ecosystem/overview.md` — **updated**: corrected SDK surface (`Editor.registerSlashCommand`, `UI.showMsg`, `Experiments.register*Renderer`), removed fabricated APIs (`registerExtensionEnhancer`, feature-flag helpers, `searchV2`, extended async-storage methods), and added `Commands`, `DB.customQuery`, DB-graph detection, and newer experiments renderers.
- [2026-05-29] `03-features/query-system/developer-guide.md` — **updated**: refreshed parser/execution/rules documentation to current `query_dsl.cljs`, `query_react.cljs`, `react.cljs`, and builder/custom-query code; removed stale filters and unverifiable benchmark claims; added current tag-descendant expansion, built-in query detection, and raw Datalog mixing behavior.
- [2026-05-29] `03-features/query-system/overview.md` — **updated**: revalidated overview with current query types and implementation paths; refreshed revision metadata.
- [2026-05-29] `03-features/query-system/query-inputs.md` — **updated**: fixed reversed `:-Xd-ms` / `:+Xd-ms` semantics, corrected page-ref input syntax, and added the `:today/+Xd` namespaced keyword form.

### 2026-05-29 — Full Re-audit (Batch 2: 03-features completed)

- [2026-05-29] `03-features/database-graphs/schema-guide.md` — **updated**: corrected `entity-dispatch-key`/`Data` schema snippets and line references, documented `reaction-entity`, updated asset-block schema fields, and clarified whiteboard/page classification drift.
- [2026-05-29] `03-features/database-graphs/schema-reference.md` — **updated**: refreshed schema statistics (118 built-in properties, 18 classes, 19 property types), corrected page-class and internal-type claims, and documented new comments/recycle/reaction/agent/sync properties.
- [2026-05-29] `03-features/dynamic-variables/developer-guide.md` — **updated**: corrected minor line-reference drift and refreshed revision metadata; core architectural claims remain valid.

### 2026-05-28 — Full Re-audit (Batch 2: 03-features partial — 5 of 8 completed)

- [2026-05-28] `03-features/dynamic-variables/syntax-compatibility-reference.md` — **updated**: chrono-node version 2.2.4→2.9.0; corrected logseq.Editor.insertTemplate() variable resolution (partial, not none). Added outliner/template.cljs resolver as new capability.
- [2026-05-28] `03-features/flashcards/implementation.md` — **updated**: #card→#Card, /cards command is unimplemented TODO (only /cloze works), keyboard shortcut n→s for reveal, phase order cloze-first-then-answer. Added context-menu card creation, "+" button, collection selector, class inheritance as new capabilities.
- [2026-05-28] `03-features/garbage-collection/developer-guide.md` — **updated**: 35 stale line numbers and interval values corrected (3-day→30-day throughout), gc-sqlite-dbs! signature fixed (no client-ops-db param), transit-read→read-transit-str, removed nonexistent :rtc/ignore-entity-when-init-download key.
- [2026-05-28] `03-features/git-backup/versioning-db-mode.md` — **updated**: live DB filename logseq.sqlite→db.sqlite, list-backups return key size→size-bytes, stale line numbers corrected, metadata.edn fields expanded (name, db-path added).
- **PENDING** (rate-limited): `03-features/database-graphs/schema-guide.md`, `03-features/database-graphs/schema-reference.md`, `03-features/dynamic-variables/developer-guide.md`

### 2026-05-28 — Full Re-audit (Batch 1: onboarding + 02-architecture)

- [2026-05-28] `01-onboarding/rtc-onboarding.md` — **updated**: yarn→pnpm, replaced WebSocket protocol (action/:req-id → :type-based), replaced op model (tuple-based → SQLite outliner-ops), removed gen_client_op.cljs/remote_update.cljs, corrected sync/ directory listing (16 files), updated test paths. Added 10+ new capabilities (apply_txs, transport, checksum, tx_sanitize, E2EE, graph schema versioning, node/ adapter).
- [2026-05-28] `02-architecture/README.md` — **updated**: fixed broken "Back to Docs Home" link (target dz/docs/README.md does not exist).
- [2026-05-28] `02-architecture/application-overview.md` — **updated**: removed Whiteboard (no tldraw in deps), removed OCR, removed PDF export, corrected PWA claim. Added Logseq CLI, AgentBridge, Cloud Publishing, Mobile Audio Transcription as new capabilities.
- [2026-05-28] `02-architecture/data-architecture.md` — **updated**: :block/content→:block/title, :block/collapsed→:block/collapsed?, integer order→fractional index string, removed :block/namespace, removed fabricated encrypt-content/persist-block-change/certificate-pinning. Fixed migration framework (defmulti → schema-version->updates). Fixed export/import claims. Added schema v65.33, class-based type system, logseq.property/\* namespace.
- [2026-05-28] `02-architecture/executive-summary.md` — **updated**: DB graph/SQLite mode added alongside DataScript, Whiteboard marked in-removal, file-format portability qualified for DB graphs. Added FSRS flashcards, shadcn UI, logseq-schema dep.
- [2026-05-28] `02-architecture/rtc-vs-sync.md` — **updated**: File Sync (fs/sync.cljs) marked removed. Corrected WS timeout (10s→600s), retries (10→unlimited+backoff), message-size thresholds removed (not found in code). Added large-title offloading, snapshot chunking, presence tracking, block-level conflict resolution.
- [2026-05-28] `02-architecture/system-architecture.md` — **updated**: transit-write→sqlite-util/write-transit-str, GC interval 3 days→30 days, fixed gc-sqlite-dbs! signature, :block/content→:block/title in DB entity, CodeMirror label corrected (only for code blocks, not main editor).
- [2026-05-28] `02-architecture/technical-stack.md` — **updated**: Shadow-CLJS 2.28→3.4, TypeScript 4.4→5.9, Capacitor 7→8, Gulp 4→5, Webpack 5.98→5.106, Playwright 1.51→1.58; Karma removed; build scripts yarn→pnpm. Added SQLite WASM, Pixi.js, comlink, Vite, Sentry, better-sqlite3, @zvec/zvec, Node≥22.20 requirement.

- [2026-05-28] Final active-header consistency pass — Added top-level `Investigation Date` metadata to `07-investigations/LOGSEQ-DB-AST-GUIDE.md` and normalized legacy bold-label punctuation in `07-investigations/pages-under-root-tag.md`.
- [2026-05-28] Date-label normalization — Standardized active investigation header labels from `Date` to `Investigation Date` in `07-investigations/pages-under-root-tag.md`, `07-investigations/quick-capture-callback-url-investigation.md`, and `07-investigations/graph-edn-import-validation-errors.md`.
- [2026-05-28] `07-investigations/graph-view-setup-and-relations-investigation.md` — Re-validated archived redirect targets (agent guide, view types, source/tests) and refreshed `lastVerified` metadata.
- [2026-05-28] Metadata consistency pass — Standardized verification metadata labels in `07-investigations/LOGSEQ-DB-AST-GUIDE.md` and added missing top-level investigation metadata in `07-investigations/published-url-property-validation-investigation.md`.
- [2026-05-28] Follow-up link fixes — Updated `07-investigations/PLUGIN-UI-INTEGRATION-GUIDE.md` to point at current `libs/src/common.ts` helpers and removed stale deleted test-doc references from `07-investigations/calculator-feature-investigation.md`; full 07-investigations local-link check now reports zero missing targets.
- [2026-05-28] `07-investigations` link-integrity maintenance — Repaired stale internal references caused by doc and source relocations across: `ios-development-00-index.md`, `quick-capture-callback-url-investigation.md`, `LOGSEQ-DB-AST-GUIDE.md`, `graph-edn-import-validation-errors.md`, and `transaction-failure-logseq-class-page-missing.md`.
- [2026-05-27] `07-investigations/ios-development-04-device-compatibility-appendix.md` — Verified and kept; added last-verified metadata and updated sync references to maintained db-sync guide/protocol docs.
- [2026-05-27] `07-investigations/ios-development-03-sync.md` — Archived and redirected after major implementation-path drift (`worker/rtc` -> `worker/sync`); now points to maintained db-sync guide/protocol and local setup docs.
- [2026-05-27] `07-investigations/ios-development-02-features.md` — Verified and kept; corrected stale iOS target/deployment and Capacitor version matrix values and refreshed references.
- [2026-05-27] `07-investigations/ios-development-01-setup.md` — Verified and kept; updated stale yarn/npx command examples to pnpm/bb task usage and refreshed references/metadata.
- [2026-05-27] `07-investigations/ios-development-00-index.md` — Verified and kept; added last-verified metadata, fixed stale local sync reference path, and clarified known-gap wording.
- [2026-05-27] `07-investigations/block-node-history-query-scenarios.md` — Verified against current code and kept; refreshed last-verified metadata and normalized code-reference links.
- [2026-05-27] `07-investigations/schema-integrity-08-index.md` — Archived and redirected to maintained architecture/process documentation for schema integrity.
- [2026-05-27] `07-investigations/schema-integrity-07-pr-review-standards.md` — Archived and redirected to maintained schema PR checklist and review process documentation.
- [2026-05-27] `07-investigations/schema-integrity-06-failure-patterns-recommendations.md` — Archived and redirected to maintained schema testing/migration/validation process documentation.
- [2026-05-27] `07-investigations/schema-integrity-05-rtc-synchronization.md` — Archived and redirected to maintained RTC sync/process/protocol documentation.
- [2026-05-27] `07-investigations/schema-integrity-04-import-export-validation.md` — Archived and redirected to maintained import/export and validation process documentation.
- [2026-05-27] `07-investigations/schema-integrity-03-migration-system.md` — Archived and redirected to maintained migration/process documentation.
- [2026-05-27] `07-investigations/schema-integrity-02-schema-system.md` — Archived and redirected to maintained schema/integrity architecture and feature docs.
- [2026-05-27] `07-investigations/schema-integrity-01-executive-summary.md` — Archived and redirected to maintained schema/integrity architecture and feature docs.
- [2026-05-27] `07-investigations/schema-evolution-and-data-migration.md` — Archived and redirected to maintained schema-change process docs.
- [2026-05-27] `07-investigations/rtc-e2ee-security-investigation.md` — Archived and redirected to maintained E2EE/security explanation docs.
- [2026-05-26] `07-investigations/scheduled-deadline-db-graphs.md` — Archived and redirected to maintained query-system/task-management docs.
- [2026-05-26] `07-investigations/graph-view-setup-and-relations-investigation.md` — Archived and redirected to maintained graph-view guide and current implementation/test references.
- [2026-05-26] `07-investigations/calculator-feature-investigation.md` — Kept and amended: updated stale docs-gap claim after confirming calculator reference doc now exists.
- [2026-05-26] `07-investigations/block-node-history-query-scenarios.md` — Kept and amended: refreshed verification metadata and corrected stale RTC worker reference path.
- [2026-05-26] `07-investigations/custom-js-customization-guide.md` — Archived and redirected to maintained custom JavaScript and plugin API docs.
- [2026-05-25] `07-investigations/custom-css-customization-guide.md` — Archived and redirected to maintained custom CSS and theme guides.
- [2026-05-25] `07-investigations/PLUGIN-UI-INTEGRATION-GUIDE.md` — Kept and amended: fixed machine-specific absolute links and added canonical-doc/verification notes.
- [2026-05-25] `07-investigations/LOGSEQ-THEME-CSS-REFERENCE.md` — Archived and redirected to maintained theme guide and canonical CSS variable source files.
- [2026-05-25] `07-investigations/LOGSEQ-DB-AST-GUIDE.md` — Kept and amended: refreshed verification metadata and clarified that line-level citations are historical and may drift.
- [2026-05-25] `07-investigations/022-logseq-cli-semantic-search.md` — Archived and redirected after audit confirmed stale semantic-search claims in CLI search flow.
- [2026-05-25] `07-investigations/published-url-property-validation-investigation.md` — Kept and amended: corrected stale dispatch-guard claim and documented current namespace-based classification behavior.
- [2026-05-25] `07-investigations/pdf-feature-platform-compatibility-investigation.md` — Archived and redirected to maintained PDF reference/compatibility docs.
- [2026-05-24] `07-investigations/import-validation-error.md` — Archived and redirected to maintained schema-system and database-integrity architecture docs.
- [2026-05-24] `07-investigations/graph-embedding-schema-mismatch-error.md` — Archived and redirected after confirming the investigated mismatch was superseded by later migration cleanup.
- [2026-05-24] `07-investigations/graph-edn-import-validation-errors.md` — Kept and amended: refreshed drifted code references, added implementation-status notes (`validate-export` path), and marked schema-version appendix as historical context.
- [2026-05-24] `07-investigations/transaction-failure-logseq-class-page-missing.md` — Kept and amended: added 2026 verification status, replaced broken absolute links with workspace-relative links, refreshed key line references, and linked related investigations.
- [2026-05-24] `07-investigations/cmd-r-reload-persistence.md` — Archived and redirected after drift audit found core references to removed persistence internals.
- [2026-05-24] `07-investigations/cmd-s-save-db-persistence.md` — Archived and redirected after code drift review (outdated save/export flow assumptions no longer matched implementation).
- [2026-05-24] `07-investigations/database-backup-electron.md` — Archived and redirected to maintained DB backup docs after code-path refactor invalidated legacy investigation details.
- [2026-05-24] `07-investigations/pages-under-root-tag.md` — Kept and amended: refreshed stale code references, marked implementation as pending, and added explicit pointer to the active `^:fix-me` test.
- [2026-05-24] `07-investigations/quick-capture-callback-url-investigation.md` — Kept and amended: updated stale code line references, added iOS verification-scope caveat, and noted current desktop handler action coverage.
- [2026-05-24] `07-investigations/x-callback-url-comprehensive-investigation.md` — Archived and replaced with redirects to maintained x-callback/quick-capture docs after drift findings (desktop/mobile scope mixing and missing current action coverage).
- [2026-05-25] `06-user-guides/logseq-db-markdown-guide.md` — Archived and replaced with redirects to maintained DB graph docs (schema guide, slash commands, properties reference, and DB properties guide).
- [2026-05-25] `06-user-guides/x-callback-url-user-guide.md` — Rewrote to implementation truth: desktop-only x-callback scope, both supported actions (`quickCapture` and `invokeCommand`), corrected quick-capture defaults and config key naming.
- [2026-05-25] `06-user-guides/root-tag-guide.md` — Kept and amended: removed non-existent migration function references, updated root-tag behavior to pipeline/property-based truth, and refreshed stale code line references.
- [2026-05-25] `06-user-guides/database-backup-guide.md` — Rewrote the guide from implementation truth: desktop backup path is `~/logseq/graphs/.../backup`, desktop auto backups are hourly with auto-source retention of 12, browser scheduled backups require folder permission and keep latest 12 snapshots.
- [2026-05-24] `06-user-guides/cli-user-guide.md` — Rewrote the guide to current CLI behavior (v0.4.3), corrected local/API syntax, removed stale appendix content, and added a revision date.
- [2026-05-24] `06-user-guides/view-guide.md` — Archived the outdated :view investigation page and redirected readers to the maintained view-function and advanced-query docs.

- [2026-03-10] `ddocs/tutorials/getting-started/01-installation.md` — Fixed macOS min version to 10.15+ (Catalina), Android min to 6.0 (SDK 23). Added revision date.
- [2026-03-10] `05-developer-guides/plugin-development/*` (10 files) — Node.js to 18+, CLJS SDK to 0.0.11, ClojureScript to 1.12.42, fixed broken links, corrected db/q usage, fixed upsert-block-property arity. Added revision dates.
- [2026-03-10] `08-processes/*` (16 files) — Corrected 20+ file paths across schema/migration docs, fixed script paths (dz/scripts/), updated Electron 38 EOL status, corrected property-ignore-rtc definition. Added revision dates.
- [2026-03-10] `03-features/plugin-ecosystem,graph-validation,git-backup,garbage-collection,database-graphs,namespaces-libraries,README` (10 files) — Fixed plugin lifecycle states, schema version to 65.22, corrected graph-has-changed? logic, updated validation pipeline. Added revision dates.
- [2026-03-10] `05-developer-guides/core-dev,mcp-dev` + `plugin-dev-starter/*` + `dev/*` (16 files) — SDK version to 0.2.12, corrected MCP tools in testing guide, fixed API namespaces, corrected line references. Added revision dates.
- [2026-03-10] `04-api-reference/*` (7 files) — Removed fabricated MCP resources/prompts, corrected MCP tools (upsertNodes), removed deleted TLDraw section, fixed @logseq/libs version to 0.2.12. Added revision dates.
- [2026-03-10] `05-developer-guides/cli-development/*` + `theme-development/*` (8 files) — CLI version to 0.4.3, corrected EDN attribute names, removed file-graph attrs, fixed broken links, noted is-file-graph removal. Added revision dates.
- [2026-03-10] `eng-documentation/*` + `01-onboarding/rtc-onboarding.md` (5 files) — CLI version to 0.4.3, corrected RTC codebase structure, removed fabricated CLI commands, fixed API token flags, corrected file paths. Added revision dates.
- [2026-03-10] `03-features/dynamic-variables,quick-capture,flashcards,task-mgmt,semantic-search,tagging` (7 files) — Removed fabricated template conditionals, corrected flashcard impl (FSRS only), fixed task states for DB graphs, corrected schema version/class lists. Added revision dates.
- [2026-03-10] `03-features/query-system/*` (8 files) — Fixed parse-query signature, :page/name→:block/name, private property syntax, DB graph property refs, DataScript predicates, broken markdown. Added revision dates.
- [2026-03-10] `02-architecture/*` (7 files) — React Native→Capacitor everywhere, corrected page entity schema (pages are blocks), fixed SQLite schema (kvs table), corrected all file paths, fixed GC interval to 3 days. Added revision dates.
- [2026-03-10] `ddocs/reference/syntax/code-blocks.md` — Updated with code investigation (agent hit rate limit before summary). Revision date added.
- [2026-03-10] `ddocs/reference/api/plugin-api.md` — Updated with code investigation (176 lines changed). Revision date added.
- [2026-03-10] `ddocs/reference/api/http-api.md` — Updated with code investigation (40 lines changed). Revision date added.
- [2026-03-10] `ddocs/reference/syntax/dynamic-variables.md` — Fixed time format (hardcoded 24h not locale), corrected query example (DSL not broken Datalog), added NLP expression caveat. Added revision date.
- [2026-03-10] `ddocs/reference/api/semantic-search-api.md` — Major rewrite: replaced non-existent thread APIs, corrected Inference Worker API, fixed CLI flags/limits, removed fabricated config settings, corrected HNSW params. Added revision date.
- [2026-03-10] `ddocs/reference/features/flashcards.md` — Major rewrite: removed obsolete SM-5 (now FSRS only), removed fabricated features (#reversed, numbered clozes, plugin APIs), corrected properties/shortcuts, scoped to DB graphs. Added revision date.
- [2026-03-10] `ddocs/reference/api/mcp-server.md` — Major: corrected tools (upsertNodes not addToPage/updateBlock), fixed CLI port (12315), removed fabricated Resources/Prompts sections, fixed output schemas, corrected imports. Added revision date.
- [2026-03-10] `ddocs/reference/features/pdf-annotations.md` — 12 fixes: corrected toolbar shortcuts, fixed search UI (no incremental/whole-words), updated zoom controls, corrected all line references, marked file-based as legacy, fixed asset properties. Added revision date.
- [2026-03-10] `ddocs/reference/database/schema-overview.md` — Updated schema version to 65.22, fixed property types (13 corrections), added 4 missing classes, expanded DataScript schema table, removed obsolete FSRS properties, updated view properties. Added revision date.
- [2026-03-10] `ddocs/reference/configuration/config-edn-db.md` — Fixed deprecated key count to 23, corrected favorites replacement ($$$favorites page not property), added missing supported options (export, outliner, commands). Added revision date.
- [2026-03-10] `ddocs/reference/configuration/config-edn.md` — Fixed DB graph config location, removed invalid feature toggle, added 7 missing file-graph-only keys, added deprecated :arweave/gateway. Added revision date.
- [2026-03-10] `ddocs/reference/features/user-profiles.md` — Major cleanup: removed 13+ fabricated features (settings UI, Gravatar, CLI commands, plugin APIs, idle detection), corrected profile creation, fixed color generation. Added revision date.
- [2026-03-10] `ddocs/reference/configuration/keyboard-shortcuts.md` — Fixed 15 bindings (platform-specific, missing alternates), added 10+ missing commands, updated counts. Added revision date.
- [2026-03-10] `ddocs/reference/configuration/css-variables.md` — Removed 6 fabricated status color variables, corrected 4 non-existent source file paths, removed non-existent highlight variable. Added revision date.
- [2026-03-10] `ddocs/explanation/security/e2ee-encryption.md` — Critical: encryption only covers block/title and block/name (not all data), private keys stored on server not OS keychain, password IS persisted, added mobile support. Added revision date.
- [2026-03-10] `ddocs/explanation/features/handbooks-system.md` — 11 fixes: corrected data format (single handbooks.edn), removed non-existent ? shortcut, fixed UI type (draggable popup), corrected all line references. Added revision date.
- [2026-03-10] `ddocs/explanation/architecture/data-architecture.md` — Corrected class hierarchy (added 10+ missing classes), fixed closed values (6 statuses not 3), expanded indexed attributes, corrected validation timing, removed fabricated tx-meta. Added revision date.
- [2026-03-10] `ddocs/explanation/concepts/file-vs-db.md` — Fixed file graph prefix (logseq*local*), corrected priority enum values, fixed query features table, updated migration to built-in importer, corrected schema examples. Added revision date.
- [2026-03-10] `ddocs/explanation/architecture/system-overview.md` — Updated version to 2.0.x, added DB Worker layer, added Shui/Comlink to tech stack, corrected scalability claim. Added revision date.
- [2026-03-10] `ddocs/explanation/comparisons/rtc-vs-file-sync.md` — Fixed PBKDF2 iterations (600k not 100k), corrected conflict resolution (transaction-level merge not OT/CRDT), fixed presence (block-level not cursor), corrected flush batch limit (50 txs). Added revision date.
- [2026-03-10] `ddocs/explanation/features/how-embeddings-work.md` — Fixed model size (~23MB not ~80MB), storage mechanism (Emscripten FS not OPFS), indexed content (no property values), added HNSW config params. Added revision date.
- [2026-03-10] `ddocs/README.md` — Major cleanup: fixed 30+ broken links, corrected wrong paths, added 18 unlisted files, removed phantom sections. Added revision date.
- [2026-03-10] `ddocs/how-to/integrations/custom-js.md` — Major: corrected that plugin SDK is NOT available in custom.js, fixed permission flow (confirm dialog not settings toggle), replaced invalid API examples with DOM-based alternatives. Added revision date.
- [2026-03-10] `ddocs/how-to/multimedia/asset-management.md` — 15 fixes: corrected asset type (extension not MIME), removed fabricated properties, added 6 missing real properties, fixed code references, removed fabricated query. Added revision date.
- [2026-03-10] `ddocs/how-to/backup/backup-restore.md` — Fixed graph path (~/logseq/ not ~/.logseq/), corrected backup timing (1-hour minimum interval), fixed CLI export commands (-f not -o), replaced dangerous cloud sync symlink advice. Added revision date.
- [2026-03-10] `ddocs/how-to/integrations/zotero.md` — Major rewrite: corrected premise (Logseq HAS built-in Zotero integration via Web API), removed fabricated plugin reference, added accurate setup with all 11 settings, fixed page naming with @ prefix. Added revision date.
- [2026-03-10] `ddocs/how-to/multimedia/video-embed.md` — Corrected video formats (added FLV/MKV, removed OGG/OGV), fixed timestamp to numeric seconds only, corrected CSS selector, added Loom/YouTube-nocookie platforms. Added revision date.
- [2026-03-10] `ddocs/how-to/integrations/create-theme.md` — Removed non-existent --ls-editor-font variable, updated CodeMirror selectors for CM5/CM6, added DB graph tag selectors, added data-color context. Added revision date.
- [2026-03-10] `ddocs/how-to/integrations/custom-css.md` — Fixed Settings UI instructions, added DB-graph custom.css storage guidance, added mobile platform. Added revision date.
- [2026-03-10] `ddocs/how-to/integrations/x-callback-url.md` — Corrected platform to desktop-only, removed misleading mobile section, added undocumented invokeCommand action, added config/template docs. Added revision date.
- [2026-03-10] `ddocs/how-to/content/favorites-recent.md` — 12 fixes: corrected favorites mechanism ($$$favorites system page), removed fabricated attributes, fixed recent pages limit to 15, corrected 5 keyboard shortcuts. Added revision date.
- [2026-03-10] `ddocs/how-to/integrations/cli-basic.md` — Updated CLI version to 0.4.3, fixed 15 issues: corrected command syntax/options for search/query/export/show/validate/append, added missing mcp-server and export-edn commands, fixed output formats. Added revision date.
- [2026-03-10] `ddocs/how-to/integrations/publishing.md` — Major rewrite: removed 6 fabricated config keys, fixed export UI flow, corrected output structure (SPA not per-page HTML), removed non-existent CLI flags and npm package. Added revision date.
- [2026-03-10] `ddocs/how-to/queries/advanced-queries.md` — Rewrote all queries for DB graphs: block/marker→logseq.property/status, block/properties→direct attributes, updated date inputs, added missing query inputs, fixed result-transform navigation. Added revision date.
- [2026-03-10] `ddocs/how-to/tasks/repeating-tasks.md` — Major rewrite: corrected completion behavior (in-place modification not copy), removed fabricated features (chained tasks, template variables, /repeat command), fixed queries and property types. Added revision date.
- [2026-03-10] `ddocs/how-to/content/view-types.md` — Added DB graph vs file graph distinction throughout, split field tables and access patterns, documented correct DB attribute names (logseq.property/status, etc). Added revision date.
- [2026-03-10] `ddocs/how-to/content/quick-capture.md` — Removed false keyboard shortcut, corrected config key to :default-page, removed non-existent iOS widget, added URL parameter reference and template docs. Added revision date.
- [2026-03-10] `ddocs/how-to/content/create-flashcard-deck.md` — 10 fixes: #card→#Card, removed non-existent {{cards}} macro, fixed scheduling properties to FSRS, removed broken queries, corrected access methods. Added revision date.
- [2026-03-10] `ddocs/tutorials/task-management/todo-basics.md` — Major: corrected task states to DB model (Backlog/Todo/Doing/In Review/Done/Canceled), fixed priorities to named values, corrected scheduling to date picker, updated queries. Added revision date.
- [2026-03-10] `ddocs/tutorials/collaboration/real-time-sync.md` — Extensive rewrite: corrected architecture to Cloudflare Durable Objects, replaced conflict resolution with optimistic concurrency control, removed fabricated git markers, fixed E2EE key management, corrected WebSocket protocol. Added revision date.
- [2026-03-10] `ddocs/tutorials/task-management/project-planning.md` — Critical: updated all queries from obsolete schema (block/marker/priority) to DB properties (logseq.property/status/priority), fixed template syntax and creation workflow. Added revision date.
- [2026-03-10] `ddocs/tutorials/getting-started/semantic-search.md` — 10 fixes: corrected enable flow, indexing status location, removed non-existent slash command, fixed storage mechanism (IDBFS not OPFS), clarified indexed content. Added revision date.
- [2026-03-10] `ddocs/tutorials/knowledge-management/flashcards-basics.md` — Major rewrite: #card→#Card, removed non-existent shortcuts/cloze syntax/settings/stats, corrected deck UI, scoped to DB graphs only. Added revision date.
- [2026-03-10] `ddocs/tutorials/getting-started/04-linking-pages.md` — Fixed Graph View shortcut (g g not Cmd+Shift+G), corrected tag vs page reference for DB graphs, fixed backlinks location (bottom of page not right sidebar). Added revision date.
- [2026-03-10] `ddocs/tutorials/getting-started/03-basic-editing.md` — Fixed highlight shortcut (Mod+Shift+H), removed non-existent code shortcut, fixed delete block instructions, rewrote slash commands to match actual codebase, corrected TODO cycling behavior. Added revision date.
- [2026-03-10] `ddocs/tutorials/getting-started/02-first-graph.md` — Fixed 3 incorrect keyboard shortcuts (Cmd+K label, removed non-existent Ctrl+J, corrected Cmd+Shift+F which was toggle-favorite not search). Added revision date.
