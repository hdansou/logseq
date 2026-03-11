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

| Section | Total | Done | Skipped | Remaining |
|---------|-------|------|---------|-----------|
| ddocs Templates | 5 | 0 | 5 | 0 |
| ddocs Tutorials | 9 | 9 | 0 | 0 |
| ddocs How-To | 17 | 17 | 0 | 0 |
| ddocs Reference | 14 | 14 | 0 | 0 |
| ddocs Explanation | 7 | 7 | 0 | 0 |
| Architecture | 7 | 7 | 0 | 0 |
| Features | 25 | 25 | 0 | 0 |
| API Reference | 7 | 7 | 0 | 0 |
| Developer Guides | 21 | 21 | 0 | 0 |
| Eng Documentation | 4 | 4 | 0 | 0 |
| Onboarding | 1 | 1 | 0 | 0 |
| Processes | 16 | 16 | 0 | 0 |
| Plugin Dev Starter | 9 | 9 | 1 | 0 |
| Product Reports | 3 | 0 | 3 | 0 |
| Dev Notes | 4 | 4 | 0 | 0 |
| Templates | 2 | 0 | 2 | 0 |
| **TOTAL** | **146** | **135** | **11** | **0** |

**Status: COMPLETE** - All 135 actionable documents updated on 2026-03-10.
