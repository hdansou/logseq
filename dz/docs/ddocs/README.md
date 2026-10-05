# Logseq Documentation (Diataxis Framework)

> **Last revised:** 2026-03-10

**Complete documentation for Logseq knowledge management application**

This documentation follows the [Diataxis Framework](https://diataxis.fr/), organizing content into four quadrants based on user needs:

---

## Documentation Quadrants

> The file lists below are generated from the tree by
> `dz/support-engineering/script/doc-inventory/gen_index.py`. Edit the prose freely;
> the lists are rewritten on each run, and `--check` fails when they drift.

<!-- INDEX:START -->

### [Tutorials](tutorials/) — _Learning-Oriented_

#### [Advanced](tutorials/advanced/)

- 🟢 [Advanced Queries Tutorial](tutorials/advanced/advanced-queries-tutorial.md)
- 🟢 [Build a Custom Theme](tutorials/advanced/custom-theme-tutorial.md)
- 🟢 [Workflow Automation Tutorial](tutorials/advanced/workflow-automation-tutorial.md)

#### [Collaboration](tutorials/collaboration/)

- 🟡 [Getting Started with Real-Time Collaboration](tutorials/collaboration/real-time-sync.md)

#### [Getting Started](tutorials/getting-started/)

- 🟢 [Installation Guide](tutorials/getting-started/01-installation.md)
- 🟢 [Your First Graph](tutorials/getting-started/02-first-graph.md)
- 🟢 [Basic Note-Taking](tutorials/getting-started/03-basic-editing.md)
- 🟢 [Linking Pages](tutorials/getting-started/04-linking-pages.md)
- [Turn On Semantic Search](tutorials/getting-started/semantic-search.md)

#### [Integrations](tutorials/integrations/)

- [Build a UI on the db-worker API](tutorials/integrations/build-a-db-worker-ui.md)
- 🟡 [Model Context Protocol (MCP) Integration](tutorials/integrations/mcp-integration.md)

#### [Knowledge Management](tutorials/knowledge-management/)

- [Flashcards Basics](tutorials/knowledge-management/flashcards-basics.md)

#### [Plugins](tutorials/plugins/)

- 🟢 [Building Your First Logseq Plugin](tutorials/plugins/first-plugin.md)

#### [Queries](tutorials/queries/)

- [Build ASCII and SVG Charts in Logseq Queries with Hiccup](tutorials/queries/charts-with-hiccup.md)

#### [Task Management](tutorials/task-management/)

- [Project Planning](tutorials/task-management/project-planning.md)
- [Todo Basics](tutorials/task-management/todo-basics.md)

---

### [How-To Guides](how-to/) — _Task-Oriented_

#### [Content](how-to/content/)

- [Create a Flashcard Deck](how-to/content/create-flashcard-deck.md)
- 🟢 [Managing Favorites & Recent Pages](how-to/content/favorites-recent.md)
- [Hide Tags from a Node](how-to/content/hide-tags-from-node.md)
- [Quick Capture](how-to/content/quick-capture.md)
- [Search Your Graph: Which Tool for Which Job](how-to/content/search-your-graph.md)
- [View Functions](how-to/content/view-types.md)

#### [Integrations](how-to/integrations/)

- [CLI Basics](how-to/integrations/cli-basic.md)
- [Create a Theme](how-to/integrations/create-theme.md)
- [Custom CSS](how-to/integrations/custom-css.md)
- [Use a Custom Font](how-to/integrations/custom-font.md)
- [Custom JavaScript](how-to/integrations/custom-js.md)
- [Build an External Vector Index of Your Graph](how-to/integrations/external-vector-index.md)
- [Publish a Page](how-to/integrations/publishing.md)
- [X-Callback-URL](how-to/integrations/x-callback-url.md)
- [Integrating Zotero with Logseq](how-to/integrations/zotero.md)

#### [Multimedia](how-to/multimedia/)

- 🟢 [Asset Management in Logseq](how-to/multimedia/asset-management.md)
- [Set a Default Image Size with CSS](how-to/multimedia/default-image-size.md)
- [Set Image and Asset Width](how-to/multimedia/image-width.md)
- 🟢 [Video Embedding in Logseq](how-to/multimedia/video-embed.md)

#### [Optimization](how-to/optimization/)

- 🟢 [Performance Optimization Guide](how-to/optimization/performance-optimization.md)

#### [Organization](how-to/organization/)

- 🟢 [Backup and Recovery Strategies](how-to/organization/backup-strategies.md)
- 🟢 [Import and Export Guide](how-to/organization/import-export.md)

#### [Queries](how-to/queries/)

- [Advanced Queries](how-to/queries/advanced-queries.md)
- [Customize the Journal Queries](how-to/queries/customize-journal-queries.md)
- [Find Empty and Unused Nodes](how-to/queries/find-empty-and-unused-nodes.md)

#### [Tasks](how-to/tasks/)

- 🟢 [Stop Task Time Tracking in DB Graphs](how-to/tasks/disable-task-time-tracking.md)
- 🟢 [Setting Up Repeating Tasks](how-to/tasks/repeating-tasks.md)

#### [Troubleshooting](how-to/troubleshooting/)

- 🟢 [Troubleshooting Guide](how-to/troubleshooting/troubleshooting-guide.md)

#### [Workflows](how-to/workflows/)

- 🟢 [Daily Workflow Setup Guide](how-to/workflows/daily-workflows.md)
- 🟢 [Meeting Notes Workflow](how-to/workflows/meeting-notes-workflow.md)
- 🟢 [Research Workflow](how-to/workflows/research-workflow.md)
- 🟢 [Task Management Workflows](how-to/workflows/task-management-workflows.md)
- 🟢 [Zettelkasten Method in Logseq](how-to/workflows/zettelkasten-method.md)

---

### [Reference](reference/) — _Information-Oriented_

#### [Advanced](reference/advanced/)

- 🟢 [Query Patterns Reference](reference/advanced/query-patterns.md)
- 🟢 [Query System Reference](reference/advanced/query-system.md)

#### [Api](reference/api/)

- [db-worker HTTP API](reference/api/db-worker-http-api.md)
- [HTTP API Reference](reference/api/http-api.md)
- [Logseq Server API Reference (REST + MCP)](reference/api/logseq-server-api.md)
- [MCP Server Reference](reference/api/mcp-server.md)
- [Plugin API Reference](reference/api/plugin-api.md)
- [Semantic Search API](reference/api/semantic-search-api.md)
- [db-worker thread-api Reference (generated)](reference/api/thread-api.generated.md)

#### [Configuration](reference/configuration/)

- [Code Block Editor Options (:editor/extra-codemirror-options)](reference/configuration/codemirror-options.md)
- [config.edn Reference](reference/configuration/config-edn.md)
- [CSS Variables Reference](reference/configuration/css-variables.md)
- [Keyboard Shortcuts Reference](reference/configuration/keyboard-shortcuts.md)

#### [Data](reference/data/)

- 🟢 [Properties Reference](reference/data/properties-reference.md)

#### [Database](reference/database/)

- [Database Graph Schema Reference](reference/database/schema-overview.md)

#### [Features](reference/features/)

- [Assets: Link Syntax, File Types, and Sync](reference/features/asset-sync.md)
- [Calculator Reference](reference/features/calculator.md)
- [Flashcard System](reference/features/flashcards.md)
- 🟠 [PDF Annotations](reference/features/pdf-annotations.md)
- [Sync Limits, Throttles, and Batch Sizes](reference/features/sync-limits.md)
- [User Profiles](reference/features/user-profiles.md)

#### [Formatting](reference/formatting/)

- 🟢 [Markdown Syntax Reference](reference/formatting/markdown-syntax.md)

#### [Interface](reference/interface/)

- [Keyboard Shortcuts (moved)](reference/interface/keyboard-shortcuts.md)
- 🟢 [Slash Commands Reference](reference/interface/slash-commands.md)
- 🟢 [Templates System](reference/interface/templates-system.md)

#### [Platforms](reference/platforms/)

- [Default Paths by Platform](reference/platforms/default-paths.md)
- 🟢 [Mobile Platform Differences](reference/platforms/mobile-differences.md)
- [URI Schemes](reference/platforms/uri-schemes.md)

#### [Plugin Apis](reference/plugin-apis/)

- [logseq.Net: HTTP client for plugins](reference/plugin-apis/net-api.md)

##### [Plugin Apis / Pr 12395](reference/plugin-apis/pr-12395/)

- [Postmate MessageChannel transport](reference/plugin-apis/pr-12395/01-postmate-message-channel.md)
- [Experiments.registerHostedRenderer](reference/plugin-apis/pr-12395/02-hosted-renderer.md)
- [Experiments.registerSidebarRenderer](reference/plugin-apis/pr-12395/03-sidebar-renderer.md)
- [App.getCurrentRoute](reference/plugin-apis/pr-12395/04-app-get-current-route.md)
- [Editor.getBlockProperties class defaults](reference/plugin-apis/pr-12395/05-block-properties-class-defaults.md)
- [helpers.ts to common.ts rename](reference/plugin-apis/pr-12395/06-helpers-to-common-rename.md)
- [PR #12395 — Plugin libs refactor](reference/plugin-apis/pr-12395/README.md)

#### [Syntax](reference/syntax/)

- 🟢 [Code Blocks Reference](reference/syntax/code-blocks.md)
- [Dynamic Variables Reference](reference/syntax/dynamic-variables.md)
- [Markdown Mirror Syntax](reference/syntax/markdown-mirror-syntax.md)
- [Paste and Type Syntax](reference/syntax/paste-and-type-syntax.md)

---

### [Explanation](explanation/) — _Understanding-Oriented_

#### [Architecture](explanation/architecture/)

- 🟢 [Block Editor Architecture](explanation/architecture/block-editor.md)
- [Data Architecture](explanation/architecture/data-architecture.md)
- 🟢 [Database Integrity & Validation](explanation/architecture/database-integrity.md)
- 🟢 [Plugin Security Architecture](explanation/architecture/plugin-security.md)
- 🟢 [Database Schema System](explanation/architecture/schema-system.md)
- [Logseq System Overview](explanation/architecture/system-overview.md)

#### [Comparisons](explanation/comparisons/)

- [RTC vs File Sync](explanation/comparisons/rtc-vs-file-sync.md)

#### [Concepts](explanation/concepts/)

- [Bidirectional properties](explanation/concepts/bidirectional-properties.md)
- 🟢 [Block-Based Editing Philosophy](explanation/concepts/block-editing-philosophy.md)
- [:block/tags vs :logseq.property/page-tags](explanation/concepts/block-tags-vs-page-tags.md)
- 🟢 [File Organization and Namespaces](explanation/concepts/file-organization.md)
- [File vs Database Graphs](explanation/concepts/file-vs-db.md)
- 🟢 [Graph Visualization and Navigation](explanation/concepts/graph-visualization.md)
- 🟢 [Linked References and Backlinks](explanation/concepts/linked-references.md)
- 🟢 [Local-First Architecture](explanation/concepts/local-first-architecture.md)
- 🟡 [Plugin Architecture](explanation/concepts/plugin-architecture.md)
- [Tag properties vs page properties](explanation/concepts/tag-properties-vs-page-properties.md)

#### [Features](explanation/features/)

- 🟢 [Handbooks System: In-App Help Panel](explanation/features/handbooks-system.md)
- [How Semantic Search Works](explanation/features/how-embeddings-work.md)
- [How Publishing Works](explanation/features/how-publishing-works.md)

#### [Security](explanation/security/)

- 🟡 [End-to-End Encryption (E2EE) in Logseq](explanation/security/e2ee-encryption.md)

---

### [Templates](_templates/)

- [[Concept/Topic Name]](_templates/explanation-template.md)
- ['](_templates/frontmatter.md)
- [How to [Task Name]](_templates/how-to-template.md)
- [[Reference Subject] Reference](_templates/reference-template.md)
- [[Tutorial Title]](_templates/tutorial-template.md)

<!-- INDEX:END -->

## Status Legend

| Badge | Meaning                | Details                                            |
| ----- | ---------------------- | -------------------------------------------------- |
| 🟢    | **Stable**             | Production-ready, unlikely to change significantly |
| 🟡    | **Active Development** | Working but API/behavior may change                |
| 🟠    | **Desktop Only**       | Not available on mobile platforms                  |
| 🔴    | **Deprecated**         | Use alternative approach                           |

---

## Quick Navigation by Role

### New User

Start here to learn Logseq basics:

1. [Installation](tutorials/getting-started/01-installation.md)
2. [Your First Graph](tutorials/getting-started/02-first-graph.md)
3. [Basic Note-Taking](tutorials/getting-started/03-basic-editing.md)
4. [Linking Pages](tutorials/getting-started/04-linking-pages.md)

### Power User

Level up your productivity:

- [Advanced Queries](how-to/queries/advanced-queries.md)
- [Quick Capture](how-to/content/quick-capture.md)
- [Flashcards](reference/features/flashcards.md)

### Developer

Build extensions and integrate:

- [HTTP API Reference](reference/api/http-api.md)
- [Plugin API Reference](reference/api/plugin-api.md)
- [MCP Server Reference](reference/api/mcp-server.md)
- [Semantic Search API](reference/api/semantic-search-api.md)

### System Administrator

Understand the system:

- [System Overview](explanation/architecture/system-overview.md)
- [Data Architecture](explanation/architecture/data-architecture.md)
- [File-Based vs Database Graphs](explanation/concepts/file-vs-db.md)
- [Backup and Recovery Strategies](how-to/organization/backup-strategies.md)

---

## Feature Coverage by Version

| Feature           | Minimum Version | Status          | Documentation                                               |
| ----------------- | --------------- | --------------- | ----------------------------------------------------------- |
| Database Graphs   | v65.0           | 🟢 Stable       | [Schema Overview](reference/database/schema-overview.md)    |
| RTC Sync          | v65.0+          | 🟡 Active Dev   | [Real-Time Sync](tutorials/collaboration/real-time-sync.md) |
| E2EE              | v65.0+          | 🟡 Active Dev   | [E2EE Encryption](explanation/security/e2ee-encryption.md)  |
| PDF Annotation    | All versions    | 🟠 Desktop Only | [PDF Annotations](reference/features/pdf-annotations.md)    |
| Flashcards (FSRS) | v64.0+          | 🟢 Stable       | [Flashcards](reference/features/flashcards.md)              |
| MCP Server        | v65.10+         | 🟢 Stable       | [MCP Reference](reference/api/mcp-server.md)                |

---

## Contributing

This documentation is generated following the Diataxis Framework principles:

- **Tutorials**: Teach through hands-on lessons
- **How-To Guides**: Solve specific problems
- **Reference**: Provide technical specifications
- **Explanation**: Clarify concepts and design decisions

When contributing, ensure your content fits the appropriate quadrant.

---

## Documentation Standards

All documentation follows these standards:

- **Frontmatter**: Every doc includes status, version, platform, and related docs
- **Code Examples**: All code snippets are tested against current version
- **Version Annotations**: Features note their "Since v" version
- **Status Badges**: Clear indication of feature maturity
- **Cross-References**: Link to related docs in all quadrants
