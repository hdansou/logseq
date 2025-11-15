# Logseq Documentation

Welcome to the comprehensive documentation for Logseq! This guide will help you navigate our documentation based on your role and needs.

## 📚 Quick Navigation by Role

### 🆕 New to Logseq Development?

Start here:

- [RTC Developer Onboarding](01-onboarding/rtc-onboarding.md)
- [System Architecture Overview](02-architecture/system-architecture.md)
- [Application Overview](02-architecture/application-overview.md)

### 👨‍💻 Plugin Developers

- [Plugin Development Index](05-developer-guides/plugin-development/index.md) - Start here
- [TypeScript Plugin Guide](05-developer-guides/plugin-development/typescript-guide.md)
- [ClojureScript Plugin Guide](05-developer-guides/plugin-development/clojurescript-guide.md)
- [Plugin API Reference](04-api-reference/plugin-api/overview.md)

### 🏗️ Core Contributors

- [Core Development Setup](../develop-logseq.md) (parent folder)
- [Development Practices](../dev-practices.md)
- [Contents Page Guide](05-developer-guides/core-development/contents-page-guide.md)

### 🎨 Theme Developers

- [Theme Development Overview](05-developer-guides/theme-development/overview.md)
- [Theme Development Guide](05-developer-guides/theme-development/guide.md)
- [Quick Reference](05-developer-guides/theme-development/quick-reference.md)

### 📖 End Users & Power Users

- [CLI User Guide](06-user-guides/cli-user-guide.md)
- [Root Tag Guide](06-user-guides/root-tag-guide.md)
- [Query System Overview](03-features/query-system/overview.md)

---

## 📂 Documentation Structure

### 01-onboarding/

Getting started materials for new developers and contributors.

- RTC developer onboarding
- Setup guides and initial orientation

### 02-architecture/

High-level system architecture and design documentation.

- **[Executive Summary](02-architecture/executive-summary.md)** - Business overview
- **[Application Overview](02-architecture/application-overview.md)** - Product overview
- **[System Architecture](02-architecture/system-architecture.md)** - Technical architecture
- **[Technical Stack](02-architecture/technical-stack.md)** - Technologies used
- **[Data Architecture](02-architecture/data-architecture.md)** - Database and data design
- **[RTC vs Sync](02-architecture/rtc-vs-sync.md)** - Collaboration comparison

### 03-features/

Deep-dive documentation for specific Logseq features.

**Database & Schema:**

- [Database Graphs Schema Guide](03-features/database-graphs/schema-guide.mda)
- [Schema Reference](03-features/database-graphs/schema-reference.md)

**Query System:**

- [Query System Overview](03-features/query-system/overview.md)
- [Developer Guide](03-features/query-system/developer-guide.md)
- [Query Performance](03-features/query-system/query-performance.md)
- [Rules Guide](03-features/query-system/rules.md)

**Organization & Classification:**

- [Tagging & Classification](03-features/tagging-classification/system-guide.md)
- [Namespaces & Libraries](03-features/namespaces-libraries/system-overview.md)
- [Task Management](03-features/task-management/system.md)

**Other Features:**

- [Plugin Ecosystem](03-features/plugin-ecosystem/overview.md)
- [Flashcards](03-features/flashcards/implementation.md)
- [Quick Capture](03-features/quick-capture/guide.md)
- [Semantic Search](03-features/semantic-search/developer-guide.md)
- [Dynamic Variables](03-features/dynamic-variables/developer-guide.md)
- [Graph Validation](03-features/graph-validation/developer-guide.md)
- [Garbage Collection](03-features/garbage-collection/developer-guide.md)
- [Git Backup](03-features/git-backup/versioning-db-mode.md)

### 04-api-reference/

Technical API specifications and references.

**Plugin API:**

- [Overview](04-api-reference/plugin-api/overview.md)
- [API Changes 2024-2025](04-api-reference/plugin-api/changes-2024-2025.md)
- [Documentation TODO](04-api-reference/plugin-api/documentation-todo.md)

**Other APIs:**

- [HTTP Server API](04-api-reference/http-api/server-guide.md)
- [MCP Server](04-api-reference/mcp-server/guide.md)

### 05-developer-guides/

How to develop features and extensions for Logseq.

**Core Development:**

- [Contents Page Guide](05-developer-guides/core-development/contents-page-guide.md)

**Plugin Development:**

- [Plugin Development Index](05-developer-guides/plugin-development/index.md) ⭐ Start here
- [File-Based Graphs](05-developer-guides/plugin-development/file-graphs.md)
- [Database Graphs](05-developer-guides/plugin-development/db-graphs.md)
- [TypeScript Guide](05-developer-guides/plugin-development/typescript-guide.md)
- [ClojureScript Guide](05-developer-guides/plugin-development/clojurescript-guide.md)
- [Tutorial: Duplicate Block Plugin](05-developer-guides/plugin-development/tutorial-duplicate-block.md)

**CLI Development:**

- [Developer Guide](05-developer-guides/cli-development/developer-guide.md)
- [Onboarding](05-developer-guides/cli-development/onboarding.md)

**Theme Development:**

- [Overview](05-developer-guides/theme-development/overview.md)
- [Guide](05-developer-guides/theme-development/guide.md)
- [Quick Reference](05-developer-guides/theme-development/quick-reference.md)
- [Example: Ocean Breeze](05-developer-guides/theme-development/example-ocean-breeze.md)

**MCP Development:**

- [What is MCP?](05-developer-guides/mcp-development/what-is-mcp.md)
- [Testing Guide](05-developer-guides/mcp-development/testing-guide.md)

### 06-user-guides/

How to use Logseq features effectively.

- [CLI User Guide](06-user-guides/cli-user-guide.md)
- [Root Tag Guide](06-user-guides/root-tag-guide.md)
- [View Guide](06-user-guides/view-guide.md)

### 07-investigations/

Historical deep-dive investigations and bug reports.

- [CMD+R Reload Data Persistence](07-investigations/cmd-r-reload-persistence.md)
- [Pages Under Root Tag Bug](07-investigations/pages-under-root-tag.md)
- [Import Validation Error](07-investigations/import-validation-error.md)

### 08-processes/

Development workflows, processes, and standards.

**Changelog:**

- [Workflow](08-processes/changelog/workflow.md)
- [System Summary](08-processes/changelog/system-summary.md)
- [Quick Reference](08-processes/changelog/quick-reference.md)

**Documentation:**

- [Generation Prompt](08-processes/documentation/generation-prompt.md)
- [Documentation Plan](08-processes/documentation/plan.md)
- [Update Process](08-processes/documentation/update-process.md)

**Dependency Management:**

- [EOL Report](08-processes/dependency-management/eol-report.md)

### 09-templates/

Reusable templates for documentation and development.

- [Continuation Prompt](09-templates/continuation-prompt.md)
- [AI Ontology Requirements](09-templates/ai-ontology-requirements.md)

---

## 🔍 Finding What You Need

### By Topic

**Learning the Architecture:**

1. Start with [Executive Summary](02-architecture/executive-summary.md)
2. Read [Application Overview](02-architecture/application-overview.md)
3. Deep dive into [System Architecture](02-architecture/system-architecture.md)

**Building a Plugin:**

1. Start with [Plugin Development Index](05-developer-guides/plugin-development/index.md)
2. Choose your language: [TypeScript](05-developer-guides/plugin-development/typescript-guide.md) or [ClojureScript](05-developer-guides/plugin-development/clojurescript-guide.md)
3. Reference [Plugin API](04-api-reference/plugin-api/overview.md)
4. Follow [Tutorial: Duplicate Block](05-developer-guides/plugin-development/tutorial-duplicate-block.md)

**Understanding Database Graphs:**

1. Read [Schema Guide](03-features/database-graphs/schema-guide.md)
2. Reference [Schema Reference](03-features/database-graphs/schema-reference.md)
3. Check [Tagging System](03-features/tagging-classification/system-guide.md)

**Working with Queries:**

1. Start with [Query System Overview](03-features/query-system/overview.md)
2. Read [Developer Guide](03-features/query-system/developer-guide.md)
3. Learn [Rules](03-features/query-system/rules.md)
4. Optimize with [Performance Guide](03-features/query-system/query-performance.md)

**Developing Themes:**

1. Read [Theme Overview](05-developer-guides/theme-development/overview.md)
2. Follow [Theme Guide](05-developer-guides/theme-development/guide.md)
3. Use [Quick Reference](05-developer-guides/theme-development/quick-reference.md)
4. Study [Ocean Breeze Example](05-developer-guides/theme-development/example-ocean-breeze.md)

---

## 🤝 Contributing to Documentation

We welcome documentation contributions! Here's how:

1. **Report Issues**: Found outdated or incorrect docs? [Open an issue](https://github.com/logseq/logseq/issues)
2. **Suggest Improvements**: Have ideas? Create a discussion or PR
3. **Follow Standards**: See [Documentation Update Process](08-processes/documentation/update-process.md)
4. **Use Templates**: Check [Templates](09-templates/) for consistency

### Documentation Maintenance

Documentation is organized to make updates easy:

- **Code changes trigger doc updates**: When you change code, check the related docs folder
- **Each folder has clear ownership**: See individual README files for maintainers
- **Regular reviews**: Quarterly documentation review cycles

---

## 📊 Documentation Stats

- **Total Categories**: 9
- **Architecture Docs**: 6
- **Feature Guides**: 30+
- **Developer Guides**: 20+
- **API References**: 5+
- **User Guides**: 3+

---

## 🔗 External Resources

- [Logseq Website](https://logseq.com)
- [GitHub Repository](https://github.com/logseq/logseq)
- [Community Forum](https://discuss.logseq.com)
- [Discord Community](https://discord.gg/logseq)
- [Plugin Marketplace](https://github.com/logseq/marketplace)

---

## 📝 License

Logseq documentation is part of the Logseq project and follows the same [AGPL-3.0 License](../LICENSE.md).

---

**Last Updated**: November 15, 2025  
**Maintainer**: Logseq Core Team  
**Status**: Reorganized Structure (Phase 1 Complete)
