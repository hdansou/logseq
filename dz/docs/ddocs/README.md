# Logseq Documentation (Diátaxis Framework)

**Complete documentation for Logseq knowledge management application**

This documentation follows the [Diátaxis Framework](https://diataxis.fr/), organizing content into four quadrants based on user needs:

---

## 📚 Documentation Quadrants

### 🎓 [Tutorials](tutorials/) — _Learning-Oriented_

**Step-by-step lessons to build knowledge and skills**

#### [Getting Started](tutorials/getting-started/)

- [Installation Guide](tutorials/getting-started/01-installation.md)
- [Your First Graph](tutorials/getting-started/02-first-graph.md)
- [Basic Note-Taking](tutorials/getting-started/03-basic-editing.md)
- [Linking Pages](tutorials/getting-started/04-linking-pages.md)

#### [Knowledge Management](tutorials/knowledge-management/)

- [Building a Knowledge Graph](tutorials/knowledge-management/building-knowledge-graph.md)
- [Daily Journaling Workflow](tutorials/knowledge-management/daily-journaling.md)

#### [Task Management](tutorials/task-management/)

- [Todo System Basics](tutorials/task-management/todo-basics.md)
- [Project Planning with Tasks](tutorials/task-management/project-planning.md)

#### [Plugins & Extensions](tutorials/plugins/)

- [Your First Plugin](tutorials/plugins/first-plugin.md)
- [MCP Integration](tutorials/plugins/mcp-integration.md)

#### [Collaboration](tutorials/collaboration/)

- 🟡 [Real-Time Sync Setup](tutorials/collaboration/real-time-sync.md)

#### [Mobile](tutorials/mobile/)

- [Getting Started on Mobile](tutorials/mobile/getting-started.md)

---

### 🛠️ [How-To Guides](how-to/) — _Task-Oriented_

**Step-by-step recipes for solving specific problems**

#### [Content Creation](how-to/content/)

- [Quick Capture](how-to/content/quick-capture.md)
- [Using Templates](how-to/content/templates.md)
- [View Types (Table/List/Gallery)](how-to/content/view-types.md)
- [Using Graph View](how-to/content/graph-view.md)
- [Embedding Video](how-to/content/video-embed.md)
- [Managing Favorites & Recent Pages](how-to/content/favorites-recent.md)

#### [Queries](how-to/queries/)

- [Advanced Queries](how-to/queries/advanced-queries.md)
- [Building Dashboards](how-to/queries/dashboards.md)

#### [Integrations](how-to/integrations/)

- [Using the CLI](how-to/integrations/cli-basic.md)
- [Zotero Integration](how-to/integrations/zotero.md)
- [Creating Custom Themes](how-to/integrations/create-theme.md)
- [Publishing Your Graph](how-to/integrations/publishing.md)

#### [Task Management](how-to/tasks/)

- [Setting Up Repeating Tasks](how-to/tasks/repeating-tasks.md)
- [Using Property History](how-to/tasks/property-history.md)

#### [Backup & Recovery](how-to/backup/)

- [Backup & Restore Guide](how-to/backup/backup-restore.md)

---

### 📖 [Reference](reference/) — _Information-Oriented_

**Technical descriptions and specifications**

#### [Syntax](reference/syntax/)

- [Markdown Reference](reference/syntax/markdown.md)
- [Query DSL Reference](reference/syntax/query-dsl.md)
- [Block Editing Reference](reference/syntax/block-editing.md)
- [Code Blocks Reference](reference/syntax/code-blocks.md)

#### [Database](reference/database/)

- [Schema Overview](reference/database/schema-overview.md) _(Since v65.12)_
- [Property Types](reference/database/property-types.md)
- [Built-in Classes](reference/database/built-in-classes.md)

#### [Configuration](reference/configuration/)

- [config.edn Reference](reference/configuration/config-edn.md)
- [Keyboard Shortcuts](reference/configuration/keyboard-shortcuts.md)
- [Feature Flags](reference/configuration/feature-flags.md)

#### [API](reference/api/)

- [HTTP API Reference](reference/api/http-api.md)
- [MCP Server Reference](reference/api/mcp-server.md)
- [CLI Commands Reference](reference/api/cli-commands.md)

#### [Features](reference/features/)

- 🟠 [PDF Annotation](reference/features/pdf.md) _(Desktop Only)_
- [Calculator](reference/features/calculator.md)
- [Math & LaTeX](reference/features/math-latex.md)
- [Asset Management](reference/features/asset-management.md)
- [Property History](reference/features/property-history.md)

#### [Mobile](reference/mobile/)

- [Platform Limitations](reference/mobile/platform-limitations.md)

---

### 💡 [Explanation](explanation/) — _Understanding-Oriented_

**Background context and conceptual understanding**

#### [Concepts](explanation/concepts/)

- [What is a Knowledge Graph?](explanation/concepts/knowledge-graph.md)
- [Block vs Page Model](explanation/concepts/block-model.md)
- [Bidirectional Linking](explanation/concepts/bidirectional-linking.md)
- [File-Based vs Database Graphs](explanation/concepts/file-vs-db.md)
- [View Types Explained](explanation/concepts/view-types.md)

#### [Architecture](explanation/architecture/)

- [System Overview](explanation/architecture/system-overview.md)
- [Schema Integrity](explanation/architecture/schema-integrity.md)
- [Handbooks System](explanation/architecture/handbooks-system.md)

#### [Security](explanation/security/)

- 🟡 [End-to-End Encryption (E2EE)](explanation/security/e2ee-encryption.md)

#### [Comparisons](explanation/comparisons/)

- [RTC vs File Sync](explanation/comparisons/rtc-vs-file-sync.md)
- [Query Types Compared](explanation/comparisons/query-types.md)

---

## 🏷️ Status Legend

| Badge | Meaning                | Details                                            |
| ----- | ---------------------- | -------------------------------------------------- |
| 🟢    | **Stable**             | Production-ready, unlikely to change significantly |
| 🟡    | **Active Development** | Working but API/behavior may change                |
| 🟠    | **Desktop Only**       | Not available on mobile platforms                  |
| 🔴    | **Deprecated**         | Use alternative approach                           |
| ⚠️    | **Experimental**       | May have bugs, not recommended for production      |

---

## 🎯 Quick Navigation by Role

### New User

Start here to learn Logseq basics:

1. [Installation](tutorials/getting-started/01-installation.md)
2. [Your First Graph](tutorials/getting-started/02-first-graph.md)
3. [Basic Note-Taking](tutorials/getting-started/03-basic-editing.md)
4. [Linking Pages](tutorials/getting-started/04-linking-pages.md)

### Power User

Level up your productivity:

- [Advanced Queries](how-to/queries/advanced-queries.md)
- [Building Dashboards](how-to/queries/dashboards.md)
- [Quick Capture](how-to/content/quick-capture.md)
- [Property History](how-to/tasks/property-history.md)

### Developer

Build extensions and integrate:

- [Your First Plugin](tutorials/plugins/first-plugin.md)
- [HTTP API Reference](reference/api/http-api.md)
- [MCP Integration](tutorials/plugins/mcp-integration.md)
- [CLI Commands](reference/api/cli-commands.md)

### System Administrator

Understand the system:

- [System Overview](explanation/architecture/system-overview.md)
- [File-Based vs Database Graphs](explanation/concepts/file-vs-db.md)
- [Schema Integrity](explanation/architecture/schema-integrity.md)
- [Backup & Restore](how-to/backup/backup-restore.md)

---

## 📊 Feature Coverage by Version

| Feature           | Minimum Version   | Status          | Documentation                                               |
| ----------------- | ----------------- | --------------- | ----------------------------------------------------------- |
| Database Graphs   | v65.0             | 🟢 Stable       | [Schema Overview](reference/database/schema-overview.md)    |
| Query System      | All versions      | 🟢 Stable       | [Query DSL](reference/syntax/query-dsl.md)                  |
| Semantic Search   | v65.0 (DB graphs) | 🟢 Stable       | _(Coming soon)_                                             |
| RTC Sync          | v65.0+            | 🟡 Active Dev   | [Real-Time Sync](tutorials/collaboration/real-time-sync.md) |
| E2EE              | v65.0+            | 🟡 Active Dev   | [E2EE Encryption](explanation/security/e2ee-encryption.md)  |
| Property History  | v65.12+           | 🟢 Stable       | [Property History](reference/features/property-history.md)  |
| PDF Annotation    | All versions      | 🟠 Desktop Only | [PDF Reference](reference/features/pdf.md)                  |
| Flashcards (FSRS) | v64.0+            | 🟢 Stable       | _(Coming soon)_                                             |
| Whiteboard        | v63.0+            | 🟢 Stable       | _(Coming soon)_                                             |
| MCP Server        | v65.10+           | 🟢 Stable       | [MCP Reference](reference/api/mcp-server.md)                |

---

## 🤝 Contributing

This documentation is generated following the Diátaxis Framework principles:

- **Tutorials**: Teach through hands-on lessons
- **How-To Guides**: Solve specific problems
- **Reference**: Provide technical specifications
- **Explanation**: Clarify concepts and design decisions

When contributing, ensure your content fits the appropriate quadrant.

---

## 📝 Documentation Standards

All documentation follows these standards:

- **Frontmatter**: Every doc includes status, version, platform, and related docs
- **Code Examples**: All code snippets are tested against current version
- **Version Annotations**: Features note their "Since v" version
- **Status Badges**: Clear indication of feature maturity
- **Cross-References**: Link to related docs in all quadrants

Last Updated: 2026-01-11
