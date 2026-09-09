# Documentation Update Task List

**Created:** 2026-03-10
**Objective:** Update all documentation in `dz/docs/` with code-validated accuracy and revision dates.

## Legend

- [ ] Not started
- [~] In progress
- [x] Completed
- [S] Skipped (template/prompt/non-doc)

---

## 1. ddocs/ - Diátaxis Framework Documentation (47 files)

### 1.1 Templates (5 files)

- [S] `ddocs/_templates/frontmatter.md` — Template, no code validation needed
- [S] `ddocs/_templates/explanation-template.md` — Template
- [S] `ddocs/_templates/reference-template.md` — Template
- [S] `ddocs/_templates/how-to-template.md` — Template
- [S] `ddocs/_templates/tutorial-template.md` — Template

### 1.2 Tutorials (9 files)

- [x] `ddocs/tutorials/getting-started/01-installation.md`
- [x] `ddocs/tutorials/getting-started/02-first-graph.md`
- [x] `ddocs/tutorials/getting-started/03-basic-editing.md`
- [x] `ddocs/tutorials/getting-started/04-linking-pages.md`
- [x] `ddocs/tutorials/getting-started/semantic-search.md`
- [x] `ddocs/tutorials/knowledge-management/flashcards-basics.md`
- [x] `ddocs/tutorials/task-management/todo-basics.md`
- [x] `ddocs/tutorials/task-management/project-planning.md`
- [x] `ddocs/tutorials/collaboration/real-time-sync.md`

### 1.3 How-To Guides (17 files)

- [x] `ddocs/how-to/tasks/repeating-tasks.md`
- [x] `ddocs/how-to/queries/advanced-queries.md`
- [x] `ddocs/how-to/content/quick-capture.md`
- [x] `ddocs/how-to/content/view-types.md`
- [x] `ddocs/how-to/content/favorites-recent.md`
- [x] `ddocs/how-to/content/create-flashcard-deck.md`
- [x] `ddocs/how-to/integrations/publishing.md`
- [x] `ddocs/how-to/integrations/cli-basic.md`
- [x] `ddocs/how-to/integrations/custom-css.md`
- [x] `ddocs/how-to/integrations/create-theme.md`
- [x] `ddocs/how-to/integrations/x-callback-url.md`
- [x] `ddocs/how-to/integrations/zotero.md`
- [x] `ddocs/how-to/integrations/custom-js.md`
- [x] `ddocs/how-to/multimedia/video-embed.md`
- [x] `ddocs/how-to/multimedia/asset-management.md`
- [x] `ddocs/how-to/backup/backup-restore.md`
- [x] `ddocs/README.md`

### 1.4 Reference (14 files)

- [x] `ddocs/reference/database/schema-overview.md`
- [x] `ddocs/reference/configuration/config-edn.md`
- [x] `ddocs/reference/configuration/config-edn-db.md`
- [x] `ddocs/reference/configuration/keyboard-shortcuts.md`
- [x] `ddocs/reference/configuration/css-variables.md`
- [x] `ddocs/reference/features/user-profiles.md`
- [x] `ddocs/reference/features/pdf-annotations.md`
- [x] `ddocs/reference/features/flashcards.md`
- [x] `ddocs/reference/api/http-api.md`
- [x] `ddocs/reference/api/plugin-api.md`
- [x] `ddocs/reference/api/mcp-server.md`
- [x] `ddocs/reference/api/semantic-search-api.md`
- [x] `ddocs/reference/syntax/code-blocks.md`
- [x] `ddocs/reference/syntax/dynamic-variables.md`

### 1.5 Explanation (7 files)

- [x] `ddocs/explanation/comparisons/rtc-vs-file-sync.md`
- [x] `ddocs/explanation/security/e2ee-encryption.md`
- [x] `ddocs/explanation/features/how-embeddings-work.md`
- [x] `ddocs/explanation/features/handbooks-system.md`
- [x] `ddocs/explanation/architecture/data-architecture.md`
- [x] `ddocs/explanation/architecture/system-overview.md`
- [x] `ddocs/explanation/concepts/file-vs-db.md`

---

## 2. Main docs/ - Engineering & Developer Documentation (99 files)

### 2.1 Architecture (7 files)

- [x] `02-architecture/README.md`
- [x] `02-architecture/application-overview.md`
- [x] `02-architecture/executive-summary.md`
- [x] `02-architecture/system-architecture.md`
- [x] `02-architecture/data-architecture.md`
- [x] `02-architecture/technical-stack.md`
- [x] `02-architecture/rtc-vs-sync.md`

### 2.2 Features (27 files)

- [x] `03-features/README.md`
- [x] `03-features/query-system/overview.md`
- [x] `03-features/query-system/developer-guide.md`
- [x] `03-features/query-system/query-performance.md`
- [x] `03-features/query-system/result-transform-user.md`
- [x] `03-features/query-system/result-transform-dev.md`
- [x] `03-features/query-system/query-inputs.md`
- [x] `03-features/query-system/rules.md`
- [x] `03-features/query-system/syntax-db-vs-file.md`
- [x] `03-features/dynamic-variables/developer-guide.md`
- [x] `03-features/dynamic-variables/user-guide.md`
- [x] `03-features/quick-capture/guide.md`
- [x] `03-features/flashcards/implementation.md`
- [x] `03-features/task-management/system.md`
- [x] `03-features/semantic-search/developer-guide.md`
- [x] `03-features/tagging-classification/system-guide.md`
- [x] `03-features/plugin-ecosystem/overview.md`
- [x] `03-features/graph-validation/developer-guide.md`
- [x] `03-features/git-backup/versioning-db-mode.md`
- [x] `03-features/garbage-collection/developer-guide.md`
- [x] `03-features/database-graphs/schema-guide.md`
- [x] `03-features/database-graphs/schema-reference.md`
- [x] `03-features/namespaces-libraries/developer-guide.md`
- [x] `03-features/namespaces-libraries/user-guide.md`
- [x] `03-features/namespaces-libraries/system-overview.md`

### 2.3 API Reference (11 files)

- [x] `04-api-reference/config-edn-reference.md`
- [x] `04-api-reference/tabler-icons-extension-catalog.md`
- [x] `04-api-reference/http-api/server-guide.md`
- [x] `04-api-reference/mcp-server/guide.md`
- [x] `04-api-reference/plugin-api/overview.md`
- [x] `04-api-reference/plugin-api/documentation-todo.md`
- [x] `04-api-reference/plugin-api/changes-2024-2025.md`

### 2.4 Developer Guides (21 files)

- [x] `05-developer-guides/cli-development/developer-guide.md`
- [x] `05-developer-guides/cli-development/edn-format-reference.md`
- [x] `05-developer-guides/cli-development/onboarding.md`
- [x] `05-developer-guides/theme-development/overview.md`
- [x] `05-developer-guides/theme-development/guide.md`
- [x] `05-developer-guides/theme-development/quick-reference.md`
- [x] `05-developer-guides/theme-development/example-ocean-breeze.md`
- [x] `05-developer-guides/theme-development/file-vs-db-graph-differences.md`
- [x] `05-developer-guides/plugin-development/README.md`
- [x] `05-developer-guides/plugin-development/index.md`
- [x] `05-developer-guides/plugin-development/typescript-guide.md`
- [x] `05-developer-guides/plugin-development/clojurescript-guide.md`
- [x] `05-developer-guides/plugin-development/clojurescript-detective.md`
- [x] `05-developer-guides/plugin-development/file-graphs.md`
- [x] `05-developer-guides/plugin-development/db-graphs.md`
- [x] `05-developer-guides/plugin-development/database-guide.md`
- [x] `05-developer-guides/plugin-development/database-guide-formatted.md`
- [x] `05-developer-guides/plugin-development/tutorial-duplicate-block.md`
- [x] `05-developer-guides/core-development/contents-page-guide.md`
- [x] `05-developer-guides/mcp-development/what-is-mcp.md`
- [x] `05-developer-guides/mcp-development/testing-guide.md`

### 2.5 Engineering Documentation (4 files)

- [x] `eng-documentation/LOGSEQ_COMMANDS.md`
- [x] `eng-documentation/RTC_DEVELOPER_ONBOARDING.md`
- [x] `eng-documentation/cli-app-interaction.md`
- [x] `eng-documentation/cli-onboarding.md`

### 2.6 Onboarding (1 file)

- [x] `01-onboarding/rtc-onboarding.md`

### 2.7 Processes (16 files)

- [x] `08-processes/rtc-sync-schema-changes.md`

---

## 3. User Guide Review Cycle

- [x] `06-user-guides/cli-user-guide.md` — Rewritten to match the current CLI spec and implementation; removed stale appendix content and added a revision date.
- [x] `06-user-guides/view-guide.md` — Archived and redirected to the canonical view-function and advanced-query docs.
- [x] `06-user-guides/database-backup-guide.md` — Rewritten with code-verified desktop/browser backup behavior, corrected storage paths, and removed unsupported restore/scheduling claims.
- [x] `06-user-guides/root-tag-guide.md` — Kept and amended: removed unsupported migration function claims and refreshed code references/line mappings to current root-tag behavior.
- [x] `06-user-guides/x-callback-url-user-guide.md` — Rewritten to desktop-only x-callback behavior; documented both supported actions (`quickCapture`, `invokeCommand`) and corrected quick-capture config defaults.
- [x] `06-user-guides/logseq-db-markdown-guide.md` — Archived and redirected to maintained schema/properties/slash-command docs to avoid duplicate drift-prone guidance.
- [x] `08-processes/EXECUTION-PLAN.md`
- [x] `08-processes/validation-best-practices.md`
- [x] `08-processes/import-export-best-practices.md`
- [x] `08-processes/COMMAND-EXTRACTION-SUMMARY.md`
- [x] `08-processes/schema-change-testing.md`
- [x] `08-processes/schema-change-pr-checklist.md`
- [x] `08-processes/schema-migration-best-practices.md`
- [x] `08-processes/documentation/generation-prompt.md`
- [x] `08-processes/documentation/update-process.md`
- [x] `08-processes/documentation/plan.md`
- [x] `08-processes/changelog/readme.md`
- [x] `08-processes/changelog/system-summary.md`
- [x] `08-processes/changelog/quick-reference.md`
- [x] `08-processes/changelog/workflow.md`
- [x] `08-processes/dependency-management/eol-report.md`

## 4. Investigation Review Cycle

- [x] `07-investigations/x-callback-url-comprehensive-investigation.md` — Archived and redirected after drift audit (mixed desktop/mobile scope and superseded by maintained x-callback + quick-capture docs).
- [x] `07-investigations/quick-capture-callback-url-investigation.md` — Kept and amended: refreshed drifted line references, clarified verification scope for iOS external files, and updated action coverage notes.
- [x] `07-investigations/pages-under-root-tag.md` — Kept and amended: updated drifted source lines, added 2026 verification status, and linked the still-pending `^:fix-me` regression test.
- [x] `07-investigations/database-backup-electron.md` — Archived and redirected; superseded by maintained DB backup docs after architecture refactor.
- [x] `07-investigations/cmd-s-save-db-persistence.md` — Archived and redirected after persistence-flow drift audit (stale export cadence and signature assumptions).
- [x] `07-investigations/cmd-r-reload-persistence.md` — Archived and redirected after audit found removed/stale persistence infrastructure references.
- [x] `07-investigations/transaction-failure-logseq-class-page-missing.md` — Kept and amended: refreshed status, fixed broken absolute links, updated key reference locations, and added related-investigation cross-links.
- [x] `07-investigations/graph-edn-import-validation-errors.md` — Kept and amended: updated stale reference ranges, clarified implemented validation paths, and marked historical schema-version context.
- [x] `07-investigations/graph-embedding-schema-mismatch-error.md` — Archived and redirected after confirming issue resolution via later schema migration cleanup.
- [x] `07-investigations/import-validation-error.md` — Archived and redirected to maintained schema/integrity architecture docs.
- [x] `07-investigations/pdf-feature-platform-compatibility-investigation.md` — Archived and redirected to maintained PDF reference and iOS compatibility appendix docs.
- [x] `07-investigations/published-url-property-validation-investigation.md` — Kept and amended: corrected mitigation claims, added current-status verification, and aligned recommendations with actual dispatch logic.
- [x] `07-investigations/022-logseq-cli-semantic-search.md` — Archived and redirected after audit found stale semantic-search claims that no longer match current CLI search behavior.
- [x] `07-investigations/LOGSEQ-DB-AST-GUIDE.md` — Kept and amended: refreshed verification metadata and added explicit guidance that line ranges are historical and symbol/file references are authoritative.
- [x] `07-investigations/LOGSEQ-THEME-CSS-REFERENCE.md` — Archived and redirected to maintained theme plugin guide and canonical CSS variable source files.
- [x] `07-investigations/PLUGIN-UI-INTEGRATION-GUIDE.md` — Kept and amended: converted machine-specific links to workspace-relative links and added canonical-doc guidance plus verification metadata.
- [x] `07-investigations/custom-css-customization-guide.md` — Archived and redirected to maintained custom CSS and theme guides.
- [x] `07-investigations/custom-js-customization-guide.md` — Archived and redirected to maintained custom JavaScript and plugin API docs.
- [x] `07-investigations/block-node-history-query-scenarios.md` — Kept and amended: refreshed verification metadata and corrected stale RTC worker code reference.
- [x] `07-investigations/calculator-feature-investigation.md` — Kept and amended: corrected stale "reference page missing" claim and refreshed verification/status notes.
- [x] `07-investigations/graph-view-setup-and-relations-investigation.md` — Archived and redirected after audit found major architecture drift versus current graph-view implementation and docs.
- [x] `07-investigations/scheduled-deadline-db-graphs.md` — Archived and redirected to maintained query-system and task-management feature docs.
- [x] `07-investigations/rtc-e2ee-security-investigation.md` — Archived and redirected to maintained E2EE/security explanation docs.
- [x] `07-investigations/schema-evolution-and-data-migration.md` — Archived and redirected to maintained schema-change process docs.
- [x] `07-investigations/schema-integrity-01-executive-summary.md` — Archived and redirected to maintained schema/integrity architecture and feature docs.
- [x] `07-investigations/schema-integrity-02-schema-system.md` — Archived and redirected to maintained schema/integrity architecture and feature docs.
- [x] `07-investigations/schema-integrity-03-migration-system.md` — Archived and redirected to maintained migration/process documentation.
- [x] `07-investigations/schema-integrity-04-import-export-validation.md` — Archived and redirected to maintained import/export and validation process documentation.
- [x] `07-investigations/schema-integrity-05-rtc-synchronization.md` — Archived and redirected to maintained RTC sync/process/protocol documentation.
- [x] `07-investigations/schema-integrity-06-failure-patterns-recommendations.md` — Archived and redirected to maintained schema testing/migration/validation process documentation.
- [x] `07-investigations/schema-integrity-07-pr-review-standards.md` — Archived and redirected to maintained schema PR checklist and review process documentation.
- [x] `07-investigations/schema-integrity-08-index.md` — Archived and redirected to maintained architecture/process documentation for schema integrity.
- [x] `07-investigations/block-node-history-query-scenarios.md` — Verified against current code; kept and amended metadata/code-reference links.
- [x] `07-investigations/ios-development-00-index.md` — Verified and kept; refreshed verification metadata, corrected local sync doc reference, and clarified known-gap wording.
- [x] `07-investigations/ios-development-01-setup.md` — Verified and kept; updated command paths to current pnpm/bb usage, refreshed metadata, and normalized references.
- [x] `07-investigations/ios-development-02-features.md` — Verified and kept; corrected iOS/Capacitor version baselines, refreshed metadata, and normalized references.
- [x] `07-investigations/ios-development-03-sync.md` — Archived and redirected after audit found major path drift from old `worker/rtc` references to current maintained db-sync docs and `worker/sync` sources.
- [x] `07-investigations/ios-development-04-device-compatibility-appendix.md` — Verified and kept as QA template; refreshed metadata and updated sync cross-references to maintained docs.
- [x] `07-investigations/* link-integrity sweep (2026-05-28)` — Fixed stale internal links across iOS index, quick-capture, AST guide, graph-EDN import report, and transaction-failure investigation after doc/code moves.
- [x] `07-investigations/PLUGIN-UI-INTEGRATION-GUIDE.md` — Link repair follow-up: replaced stale `libs/src/helpers.ts` references with current `libs/src/common.ts` locations.
- [x] `07-investigations/calculator-feature-investigation.md` — Link repair follow-up: removed stale legacy snapshot references to deleted `src/test/docs-0.10.12/pages/Calculator.md`.
- [x] `07-investigations/LOGSEQ-DB-AST-GUIDE.md` / `07-investigations/published-url-property-validation-investigation.md` — Metadata normalization: standardized verification field naming and added top-level investigation metadata where missing.
- [x] `07-investigations/graph-view-setup-and-relations-investigation.md` — Re-validated archive redirect targets and refreshed `lastVerified` metadata.
- [x] `07-investigations/pages-under-root-tag.md` / `quick-capture-callback-url-investigation.md` / `graph-edn-import-validation-errors.md` — Metadata label normalization: standardized `Date` to `Investigation Date` for active investigation headers.
- [x] `07-investigations/LOGSEQ-DB-AST-GUIDE.md` / `pages-under-root-tag.md` — Final header consistency: ensured top-level `Investigation Date` presence/order and normalized bold-label punctuation style.

### 2.8 Plugin Dev Starter (10 files)

- [x] `plugin-dev-starter/README.md`
- [x] `plugin-dev-starter/01-quick-start.md`
- [x] `plugin-dev-starter/02-plugin-basics.md`
- [x] `plugin-dev-starter/03-development-setup.md`
- [x] `plugin-dev-starter/04-api-overview.md`
- [x] `plugin-dev-starter/05-blocks-and-pages.md`
- [x] `plugin-dev-starter/INDEX.md`
- [x] `plugin-dev-starter/QUICK_REFERENCE.md`
- [x] `plugin-dev-starter/17-examples.md`
- [S] `plugin-dev-starter/CREATION_SUMMARY.md` — Meta/process doc

### 2.9 Product Reports (3 files)

- [S] `product-report/logseq_executive_report.md` — Point-in-time report
- [S] `product-report/label_analysis.md` — Point-in-time analysis
- [S] `product-report/initial-prompt.md` — Prompt template

### 2.10 Dev Notes (4 files)

- [x] `dev/PLUGIN_API_CHANGES_2024-2025.md`
- [x] `dev/PLUGIN_DOCUMENTATION_UPDATE_PROCESS.md`
- [x] `dev/import-validation-error-investigation.md`
- [x] `dev/PLUGIN_API_DOCUMENTATION_TODO.md`

### 2.11 Templates (2 files)

- [S] `09-templates/continuation-prompt.md` — Prompt template
- [S] `09-templates/ai-ontology-requirements.md` — Requirements doc

---

## Progress Summary

| Section            | Total   | Done    | Skipped | Remaining |
| ------------------ | ------- | ------- | ------- | --------- |
| ddocs Templates    | 5       | 0       | 5       | 0         |
| ddocs Tutorials    | 9       | 9       | 0       | 0         |
| ddocs How-To       | 17      | 17      | 0       | 0         |
| ddocs Reference    | 14      | 14      | 0       | 0         |
| ddocs Explanation  | 7       | 7       | 0       | 0         |
| Architecture       | 7       | 7       | 0       | 0         |
| Features           | 25      | 25      | 0       | 0         |
| API Reference      | 7       | 7       | 0       | 0         |
| Developer Guides   | 21      | 21      | 0       | 0         |
| Eng Documentation  | 4       | 4       | 0       | 0         |
| Onboarding         | 1       | 1       | 0       | 0         |
| Processes          | 16      | 16      | 0       | 0         |
| Plugin Dev Starter | 9       | 9       | 1       | 0         |
| Product Reports    | 3       | 0       | 3       | 0         |
| Dev Notes          | 4       | 4       | 0       | 0         |
| Templates          | 2       | 0       | 2       | 0         |
| **TOTAL**          | **146** | **135** | **11**  | **0**     |

**Status: COMPLETE** - All 135 actionable documents updated on 2026-03-10.
