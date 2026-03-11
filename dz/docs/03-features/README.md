# Feature Documentation

> **Last revised:** 2026-03-10

Deep-dive documentation for specific Logseq features and subsystems.

## Overview

This folder contains detailed technical documentation for individual Logseq features. Each feature has its own subdirectory with comprehensive guides.

## Features by Category

### Database & Schema

**[database-graphs/](database-graphs/)**

- [Schema Guide](database-graphs/schema-guide.md) - Complete schema investigation (~1,800 lines)
- [Schema Reference](database-graphs/schema-reference.md) - All properties, classes, types (~1,400 lines)

**[tagging-classification/](tagging-classification/)**

- [System Guide](tagging-classification/system-guide.md) - Enhanced tagging and classification

### Query & Search

**[query-system/](query-system/)**

- [Overview](query-system/overview.md) - Query system introduction
- [Developer Guide](query-system/developer-guide.md) - Building queries
- [Syntax: DB vs File](query-system/syntax-db-vs-file.md) - Graph type differences
- [Query Inputs](query-system/query-inputs.md) - Input parameters
- [Performance](query-system/query-performance.md) - Optimization guide
- [Rules](query-system/rules.md) - Custom query rules
- [Result Transform](query-system/result-transform-dev.md) - Developer guide
- [Result Transform User](query-system/result-transform-user.md) - User guide

**[semantic-search/](semantic-search/)**

- [Developer Guide](semantic-search/developer-guide.md) - Semantic search implementation

### Organization & Workflow

**[namespaces-libraries/](namespaces-libraries/)**

- [System Overview](namespaces-libraries/system-overview.md) - Architecture
- [Developer Guide](namespaces-libraries/developer-guide.md) - Development
- [User Guide](namespaces-libraries/user-guide.md) - Using namespaces

**[task-management/](task-management/)**

- [System](task-management/system.md) - Task management features

**[quick-capture/](quick-capture/)**

- [Guide](quick-capture/guide.md) - Quick capture functionality

### Extension & Integration

**[plugin-ecosystem/](plugin-ecosystem/)**

- [Overview](plugin-ecosystem/overview.md) - Plugin architecture

**[flashcards/](flashcards/)**

- [Implementation](flashcards/implementation.md) - Flashcard system

### Advanced Features

**[dynamic-variables/](dynamic-variables/)**

- [Developer Guide](dynamic-variables/developer-guide.md) - Implementation
- [User Guide](dynamic-variables/user-guide.md) - Usage

**[graph-validation/](graph-validation/)**

- [Developer Guide](graph-validation/developer-guide.md) - Validation system

**[garbage-collection/](garbage-collection/)**

- [Developer Guide](garbage-collection/developer-guide.md) - GC implementation

**[git-backup/](git-backup/)**

- [Versioning DB Mode](git-backup/versioning-db-mode.md) - Git integration

## Quick Access by Use Case

### "I want to understand the database schema"

→ Start with [database-graphs/schema-guide.md](database-graphs/schema-guide.md)

### "I want to build complex queries"

→ Read [query-system/developer-guide.md](query-system/developer-guide.md)

### "I need to optimize query performance"

→ Check [query-system/query-performance.md](query-system/query-performance.md)

### "I'm working with tags and classes"

→ See [tagging-classification/system-guide.md](tagging-classification/system-guide.md)

### "I want to use namespaces in my graph"

→ Read [namespaces-libraries/user-guide.md](namespaces-libraries/user-guide.md)

### "I'm developing a plugin"

→ Review [plugin-ecosystem/overview.md](plugin-ecosystem/overview.md)

## Maintenance

### Update Triggers

Update feature documentation when:

- Feature is added or significantly changed
- API for the feature changes
- User-facing behavior changes
- Performance characteristics change
- New capabilities are added

### Ownership

Each feature folder has its own maintainer:

- **database-graphs**: DB team
- **query-system**: Query engine team
- **plugin-ecosystem**: Plugin team
- **tagging-classification**: Schema team
- Other features: Respective feature owners

### Last Updated

- Most docs: March 10, 2026
- Schema docs: March 2026 (schema version 65.22)

## Related Documentation

- **Architecture**: [../02-architecture/](../02-architecture/)
- **API Reference**: [../04-api-reference/](../04-api-reference/)
- **Developer Guides**: [../05-developer-guides/](../05-developer-guides/)
- **User Guides**: [../06-user-guides/](../06-user-guides/)

---

**Navigation**: [Back to Docs Home](../README.md)
