# Logseq Documentation (Diataxis Framework)

> **Last revised:** 2026-03-10

**Complete documentation for Logseq knowledge management application**

This documentation follows the [Diataxis Framework](https://diataxis.fr/), organizing content into four quadrants based on user needs:

---

## Documentation Quadrants

### [Tutorials](tutorials/) — _Learning-Oriented_

**Step-by-step lessons to build knowledge and skills**

#### [Getting Started](tutorials/getting-started/)

- [Installation Guide](tutorials/getting-started/01-installation.md)
- [Your First Graph](tutorials/getting-started/02-first-graph.md)
- [Basic Note-Taking](tutorials/getting-started/03-basic-editing.md)
- [Linking Pages](tutorials/getting-started/04-linking-pages.md)
- [Semantic Search](tutorials/getting-started/semantic-search.md)

#### [Knowledge Management](tutorials/knowledge-management/)

- [Flashcards Basics](tutorials/knowledge-management/flashcards-basics.md)

#### [Task Management](tutorials/task-management/)

- [Todo System Basics](tutorials/task-management/todo-basics.md)
- [Project Planning with Tasks](tutorials/task-management/project-planning.md)

#### [Collaboration](tutorials/collaboration/)

- 🟡 [Real-Time Sync Setup](tutorials/collaboration/real-time-sync.md)

---

### [How-To Guides](how-to/) — _Task-Oriented_

**Step-by-step recipes for solving specific problems**

#### [Content Creation](how-to/content/)

- [Quick Capture](how-to/content/quick-capture.md)
- [View Types (Table/List/Gallery)](how-to/content/view-types.md)
- [Managing Favorites & Recent Pages](how-to/content/favorites-recent.md)
- [Create a Flashcard Deck](how-to/content/create-flashcard-deck.md)

#### [Multimedia](how-to/multimedia/)

- [Embedding Video](how-to/multimedia/video-embed.md)
- [Asset Management](how-to/multimedia/asset-management.md)

#### [Queries](how-to/queries/)

- [Advanced Queries](how-to/queries/advanced-queries.md)

#### [Integrations](how-to/integrations/)

- [Using the CLI](how-to/integrations/cli-basic.md)
- [Zotero Integration](how-to/integrations/zotero.md)
- [Creating Custom Themes](how-to/integrations/create-theme.md)
- [Custom CSS](how-to/integrations/custom-css.md)
- [Custom JS](how-to/integrations/custom-js.md)
- [Publishing Your Graph](how-to/integrations/publishing.md)
- [x-callback-url](how-to/integrations/x-callback-url.md)

#### [Task Management](how-to/tasks/)

- [Setting Up Repeating Tasks](how-to/tasks/repeating-tasks.md)

#### [Backup & Recovery](how-to/backup/)

- [Backup & Restore Guide](how-to/backup/backup-restore.md)

---

### [Reference](reference/) — _Information-Oriented_

**Technical descriptions and specifications**

#### [Syntax](reference/syntax/)

- [Code Blocks Reference](reference/syntax/code-blocks.md)
- [Dynamic Variables](reference/syntax/dynamic-variables.md)

#### [Database](reference/database/)

- [Schema Overview](reference/database/schema-overview.md) _(Since v65.12)_

#### [Configuration](reference/configuration/)

- [config.edn Reference](reference/configuration/config-edn.md)
- [config.edn (DB Graphs)](reference/configuration/config-edn-db.md)
- [Keyboard Shortcuts](reference/configuration/keyboard-shortcuts.md)
- [CSS Variables](reference/configuration/css-variables.md)

#### [API](reference/api/)

- [HTTP API Reference](reference/api/http-api.md)
- [MCP Server Reference](reference/api/mcp-server.md)
- [Plugin API Reference](reference/api/plugin-api.md)
- [Semantic Search API](reference/api/semantic-search-api.md)

#### [Features](reference/features/)

- 🟠 [PDF Annotations](reference/features/pdf-annotations.md) _(Desktop Only)_
- [Flashcards](reference/features/flashcards.md)
- [User Profiles](reference/features/user-profiles.md)

---

### [Explanation](explanation/) — _Understanding-Oriented_

**Background context and conceptual understanding**

#### [Concepts](explanation/concepts/)

- [File-Based vs Database Graphs](explanation/concepts/file-vs-db.md)

#### [Architecture](explanation/architecture/)

- [System Overview](explanation/architecture/system-overview.md)
- [Data Architecture](explanation/architecture/data-architecture.md)

#### [Features](explanation/features/)

- [Handbooks System](explanation/features/handbooks-system.md)
- [How Embeddings Work](explanation/features/how-embeddings-work.md)

#### [Security](explanation/security/)

- 🟡 [End-to-End Encryption (E2EE)](explanation/security/e2ee-encryption.md)

#### [Comparisons](explanation/comparisons/)

- [RTC vs File Sync](explanation/comparisons/rtc-vs-file-sync.md)

---

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
- [Backup & Restore](how-to/backup/backup-restore.md)

---

## Feature Coverage by Version

| Feature           | Minimum Version   | Status          | Documentation                                               |
| ----------------- | ----------------- | --------------- | ----------------------------------------------------------- |
| Database Graphs   | v65.0             | 🟢 Stable       | [Schema Overview](reference/database/schema-overview.md)    |
| RTC Sync          | v65.0+            | 🟡 Active Dev   | [Real-Time Sync](tutorials/collaboration/real-time-sync.md) |
| E2EE              | v65.0+            | 🟡 Active Dev   | [E2EE Encryption](explanation/security/e2ee-encryption.md)  |
| PDF Annotation    | All versions      | 🟠 Desktop Only | [PDF Annotations](reference/features/pdf-annotations.md)    |
| Flashcards (FSRS) | v64.0+            | 🟢 Stable       | [Flashcards](reference/features/flashcards.md)              |
| MCP Server        | v65.10+           | 🟢 Stable       | [MCP Reference](reference/api/mcp-server.md)                |

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
