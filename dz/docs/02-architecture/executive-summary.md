# Executive Summary

> **Last revised:** 2026-05-28

## Overview

Logseq is a privacy-first, open-source knowledge management and collaboration platform that empowers users to organize, connect, and visualize their thoughts and information. Built on modern web technologies with a focus on user control and data ownership, Logseq combines the flexibility of a note-taking application with the power of a knowledge graph.

## Key Value Propositions

### 🔒 Privacy-First Design

- **Local Data Storage**: All user data remains on the user's device by default
- **No Vendor Lock-in**: File-based graphs use open text formats (Markdown, Org-mode) for data portability; the newer DB graph mode stores data in SQLite and relies on export for portability
- **Optional Cloud Sync**: Users control whether and how their data is synchronized

### 🧠 Knowledge Graph Approach

- **Bi-directional Linking**: Automatic relationship discovery between concepts
- **Block-level References**: Granular linking at the paragraph/block level
- **Visual Graph Representation**: Interactive visualization of knowledge connections

### 🎨 Flexible Content Creation

- **Multiple Formats**: File-based graphs support Markdown and Org-mode; DB graphs store structured data in SQLite independent of file format
- **Rich Media**: Integration of images, PDFs, videos, and external content
- **Whiteboard Feature**: Visual canvas for spatial thinking and mind mapping

  > **Removed (in progress):** Whiteboard removal is underway in the codebase (see `src/main/frontend/components/export.cljs` and `src/main/frontend/handler/assets.cljs`). The `logseq.class/Whiteboard` schema class and tldraw data properties still exist but are being phased out.

### 🔧 Extensible Platform

- **Plugin Ecosystem**: Rich API for custom functionality
- **Theme System**: Customizable appearance and user interface
- **Integration Capabilities**: Connect with external tools and services

## Target Users

### Knowledge Workers

- Researchers, academics, and students
- Writers, journalists, and content creators
- Consultants and analysts

### Teams and Organizations

- Research teams requiring collaborative knowledge management
- Organizations implementing knowledge-first workflows
- Educational institutions

### Developers and Power Users

- Plugin developers extending functionality
- Users requiring highly customizable note-taking solutions
- Privacy-conscious individuals and organizations

## Market Position

Logseq occupies a unique position in the knowledge management space by combining:

1. **Open Source Philosophy**: Transparent development and community-driven features
2. **Modern Architecture**: Built with contemporary web technologies for performance and reliability
3. **Data Sovereignty**: Users maintain complete control over their information
4. **Cross-Platform Availability**: Consistent experience across desktop, web, and mobile

## Business Model

- **Open Source Core**: Free and open-source software with community contributions
- **Premium Services**: Optional cloud sync and collaboration features
- **Plugin Marketplace**: Ecosystem for third-party extensions and themes

## Technical Highlights

### Architecture

- **Frontend**: ClojureScript with React and Rum
- **Database**: Two storage backends co-exist:
  - **DataScript** (in-memory Datalog DB) — used by legacy file-based graphs
  - **SQLite** (`@sqlite.org/sqlite-wasm`) — used by the new DB graph mode, which is the primary development target
- **Desktop**: Electron-based cross-platform application
- **Mobile**: Capacitor-based (ClojureScript web view)

### Performance

- **Local-First**: Instant response times for core operations
- **Incremental Sync**: Efficient data synchronization when enabled
- **Progressive Loading**: Optimized for large knowledge bases

## Competitive Advantages

1. **Data Ownership**: Unlike cloud-first competitors, users own their data completely
2. **Open Architecture**: Extensible through plugins and open APIs
3. **Graph Database**: Native support for complex knowledge relationships
4. **Multi-Modal**: Combines text, visual, and spatial thinking tools
5. **Cross-Platform**: Consistent experience across all devices

## Strategic Direction

### Short-Term Goals

- Enhanced mobile experience and feature parity
- Improved real-time collaboration capabilities
- Expanded plugin ecosystem

### Long-Term Vision

- Become the leading open-source knowledge management platform
- Pioneer new approaches to personal and organizational knowledge work
- Build sustainable ecosystem for knowledge workers

## Success Metrics

### User Adoption

- Active user growth and retention
- Community engagement and contributions
- Plugin ecosystem development

### Technical Excellence

- Performance benchmarks
- Data security and privacy compliance
- Cross-platform compatibility

### Ecosystem Health

- Third-party integrations
- Developer community growth
- Educational and enterprise adoption

## Findings: New Capabilities

The following capabilities are present in the codebase but not mentioned in this document:

- **DB Graph Mode (SQLite)**: A new primary graph type backed by SQLite (`@sqlite.org/sqlite-wasm`) with a structured schema (`logseq-schema` dependency). This mode replaces the open-file paradigm with a proper relational/entity store and is the active development focus.
- **FSRS Spaced-Repetition Flashcards**: The `io.github.open-spaced-repetition/cljc-fsrs` library is integrated, enabling flashcard review with the FSRS algorithm. Flashcards are exposed as a dedicated tab in the mobile app.
- **shadcn-based UI Component Library**: `packages/ui/` provides a shadcn-derived component system used throughout the frontend, representing a significant UI infrastructure layer.
- **logseq-schema External Dependency**: The DB graph schema (`logseq/logseq-schema` via Git) has been extracted into a dedicated repository, enabling schema versioning independent of the main app.

---

_This executive summary provides stakeholders with a comprehensive overview of Logseq's position, capabilities, and strategic direction in the knowledge management market._
