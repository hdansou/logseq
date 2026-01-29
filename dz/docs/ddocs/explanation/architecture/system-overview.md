---
title: 'Logseq System Overview'
description: "High-level overview of Logseq's architecture, value propositions, and design principles"
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
category: architecture
relatedDocs:
  reference:
    - '[Database Schema](../../reference/database/schema-overview.md)'
    - '[Plugin API](../../reference/api/plugin-api.md)'
  explanation:
    - '[System Architecture](system-architecture.md)'
    - '[Data Architecture](data-architecture.md)'
---

# Logseq System Overview

> Understanding what Logseq is, why it exists, and how it empowers users to manage knowledge effectively.

## What is Logseq?

Logseq is a **privacy-first, open-source knowledge management platform** that combines the flexibility of note-taking with the power of a knowledge graph. It enables users to organize, connect, and visualize their thoughts and information in ways that traditional note-taking apps cannot.

---

## Core Design Principles

### 🔒 Privacy-First

- **Local-First**: All data stays on your device by default
- **No Vendor Lock-in**: Open file formats (Markdown, Org-mode) ensure portability
- **Your Data, Your Control**: Cloud sync is optional, never required

### 🧠 Knowledge Graph Approach

- **Bi-directional Linking**: Automatically discover relationships between concepts
- **Block-level References**: Link at paragraph granularity, not just page level
- **Visual Exploration**: Interactive graph visualization of connections

### 🔧 Extensibility

- **Plugin Ecosystem**: Extend with custom functionality
- **Theme System**: Customize appearance and behavior
- **Open APIs**: Integrate with external tools and workflows

---

## Two Graph Types

Logseq supports two storage architectures:

| Aspect            | File-Based Graphs         | Database Graphs      |
| ----------------- | ------------------------- | -------------------- |
| **Storage**       | Markdown/Org files        | SQLite database      |
| **Compatibility** | External editors          | Logseq only          |
| **Properties**    | Text-based                | First-class entities |
| **Scalability**   | File system limits        | Unlimited            |
| **Collaboration** | File sync                 | Real-time (RTC)      |
| **Best For**      | Git workflows, simplicity | Power users, teams   |

Both types maintain the same user experience—the difference is in how data is stored and synchronized.

---

## Architecture Layers

```
┌─────────────────────────────────────────────┐
│             User Interface                  │
│   (React/Rum components, Editor, Sidebar)   │
└─────────────────────┬───────────────────────┘
                      │
┌─────────────────────▼───────────────────────┐
│            State Management                 │
│   (Atoms, Event Handlers, Subscriptions)    │
└─────────────────────┬───────────────────────┘
                      │
┌─────────────────────▼───────────────────────┐
│           DataScript Database               │
│   (In-memory graph DB, Datalog queries)     │
└─────────────────────┬───────────────────────┘
                      │
┌─────────────────────▼───────────────────────┐
│           Storage Abstraction               │
│   ┌─────────────┐   ┌─────────────────┐     │
│   │ File System │   │ SQLite + OPFS   │     │
│   │ (Markdown)  │   │ (Database)      │     │
│   └─────────────┘   └─────────────────┘     │
└─────────────────────────────────────────────┘
```

---

## Key Subsystems

### 1. Database Layer

- **DataScript**: In-memory Datalog database for queries
- **Schema System**: Validates entity structure and relationships
- **Transaction Log**: Tracks changes for undo/redo

### 2. Editor System

- **Block-based Editing**: Every paragraph is a referenceable block
- **Slash Commands**: Quick access to features via `/`
- **Markdown/Org Support**: Choose your preferred format

### 3. Plugin System

- **SDK**: JavaScript/TypeScript API for extensions
- **Sandbox Modes**: Iframe (isolated) or Shadow (fast)
- **UI Slots**: Inject custom components

### 4. Sync & Collaboration

- **File Sync**: Traditional cloud sync for markdown files
- **RTC**: Real-time collaboration with CRDT-like operations
- **E2EE**: End-to-end encryption for private data

---

## Technology Stack

| Layer           | Technology         | Purpose                   |
| --------------- | ------------------ | ------------------------- |
| **Language**    | ClojureScript      | Main application code     |
| **UI**          | React + Rum        | Component rendering       |
| **Database**    | DataScript         | In-memory graph queries   |
| **Persistence** | SQLite (WASM/OPFS) | Database graph storage    |
| **Desktop**     | Electron           | Cross-platform native app |
| **Mobile**      | Capacitor          | iOS/Android apps          |
| **Styling**     | Tailwind CSS       | UI styling                |

---

## Who Uses Logseq?

### Knowledge Workers

- Researchers, academics, students
- Writers, journalists, content creators
- Consultants and analysts

### Development Teams

- Documentation management
- Technical decision logs
- Research collaboration

### Personal Users

- Daily journaling
- Project planning
- Learning and spaced repetition

---

## What Makes Logseq Different?

| Compared To     | Logseq Advantage                         |
| --------------- | ---------------------------------------- |
| **Notion**      | Local-first, no vendor lock-in           |
| **Obsidian**    | Block-level linking, open-source         |
| **Roam**        | Free, self-hostable, database graphs     |
| **Apple Notes** | Knowledge graph, plugins, power features |

---

## Getting Started

1. **Download**: Get Logseq from [logseq.com](https://logseq.com)
2. **Create Graph**: Choose file-based or database type
3. **Start Writing**: Use daily journals or create pages
4. **Link Ideas**: Use `[[double brackets]]` to connect
5. **Explore**: Try queries, properties, and plugins

---

## Further Reading

- **System Architecture**: [system-architecture.md](system-architecture.md) - Technical deep dive
- **Data Architecture**: [data-architecture.md](data-architecture.md) - How data is stored
- **File vs DB**: [file-vs-db.md](../concepts/file-vs-db.md) - Choosing your graph type

---

_Last updated: 2026-01-29_
