# System Architecture

> **Last revised:** 2026-05-28

## Architecture Overview

Logseq employs a **local-first, multi-platform architecture** that prioritizes user data ownership while providing seamless cross-platform experiences. The system features a sophisticated dual-mode architecture supporting both traditional file-based storage and modern SQLite database-backed graphs, designed around reactive, event-driven patterns with graph databases at its core.

**Current Architecture Status:**

- **Dual Storage Models**: File-based graphs and SQLite database-based graphs
- **Seamless Migration**: Transparent switching between storage modes
- **Enhanced Performance**: SQLite implementation provides unlimited scalability and ACID transactions
- **Backward Compatibility**: Full support for existing file-based workflows

## High-Level Architecture

```mermaid
C4Context
    title System Context Diagram for Logseq

    Person(user, "User", "Knowledge worker, researcher, or student")
    System(logseq, "Logseq Application", "Knowledge management and collaboration platform")

    System_Ext(fs, "File System", "Local file storage")
    System_Ext(cloud, "Cloud Storage", "Optional sync services")
    System_Ext(plugins, "Plugin Ecosystem", "Third-party extensions")
    System_Ext(external, "External Tools", "Zotero, Git, etc.")

    Rel(user, logseq, "Creates and manages knowledge")
    Rel(logseq, fs, "Reads/writes files")
    Rel(logseq, cloud, "Syncs data")
    Rel(logseq, plugins, "Extends functionality")
    Rel(logseq, external, "Integrates with")
```

## Container Architecture

```mermaid
C4Container
    title Container Diagram for Logseq Application (Dual-Mode Architecture)

    Container(webapp, "Web Application", "ClojureScript/React", "Main application interface")
    Container(desktop, "Desktop App", "Electron", "Native desktop wrapper")
    Container(mobile, "Mobile App", "Capacitor", "Mobile application")

    Container(ds_db, "DataScript Database", "In-memory graph DB", "Query engine and reactive layer")
    Container(sqlite_db, "SQLite Database", "SQLite + WASM/OPFS", "Persistent storage for DB graphs")
    Container(fs, "File System Layer", "ClojureScript", "File I/O for file-based graphs")
    Container(storage_abstraction, "Storage Abstraction", "ClojureScript", "Unified interface for dual storage modes")

    Container(db_worker, "Database Worker", "Web Worker + SQLite", "Background database operations")
    Container(sync, "Sync Engine", "ClojureScript", "Data synchronization")
    Container(plugins, "Plugin Runtime", "JavaScript/ClojureScript", "Plugin execution environment")

    Rel(webapp, ds_db, "Queries/updates")
    Rel(ds_db, storage_abstraction, "Data persistence")
    Rel(storage_abstraction, sqlite_db, "DB graphs")
    Rel(storage_abstraction, fs, "File graphs")
    Rel(webapp, db_worker, "SQLite operations")
    Rel(db_worker, sqlite_db, "Direct DB access")
    Rel(desktop, webapp, "Embeds")
    Rel(mobile, ds_db, "Queries/updates")
    Rel(webapp, sync, "Sync operations")
    Rel(webapp, plugins, "Plugin API calls")
```

## Component Architecture

```mermaid
C4Component
    title Component Diagram for Logseq Frontend

    Component(ui, "UI Components", "React/Rum", "User interface components")
    Component(state, "State Management", "Rum/Atom", "Application state")
    Component(handlers, "Event Handlers", "ClojureScript", "Business logic")
    Component(db_layer, "Database Layer", "DataScript", "Data access")
    Component(router, "Router", "Reitit", "Navigation")
    Component(editor, "Editor", "Custom contenteditable", "Block text editing (CodeMirror used for code blocks only)")

    Rel(ui, state, "Subscribes to")
    Rel(ui, handlers, "Triggers events")
    Rel(handlers, db_layer, "Queries/transactions")
    Rel(handlers, state, "Updates")
    Rel(router, ui, "Route changes")
    Rel(editor, handlers, "Edit events")
```

## Architectural Patterns

### Local-First Architecture

The application follows a **local-first** approach where:

- All data operations happen locally first
- Cloud sync is an optional enhancement
- Users maintain full control over their data
- Offline functionality is guaranteed

### Event-Driven Architecture

Logseq implements an event-driven system using:

- **Event Handlers**: Process user actions and system events
- **State Subscriptions**: Reactive updates to UI components
- **Database Transactions**: Atomic data changes
- **Plugin Hooks**: Extension points for custom functionality

### Reactive Data Flow

```mermaid
graph TB
    A[User Action] --> B[Event Handler]
    B --> C[Database Transaction]
    C --> D[State Update]
    D --> E[UI Re-render]

    F[File System Change] --> G[File Watcher]
    G --> C

    H[Plugin Event] --> I[Plugin Handler]
    I --> C

    style A fill:#e3f2fd
    style E fill:#e8f5e8
    style C fill:#fff3e0
```

## Core Subsystems

### 1. Database Subsystem (Dual-Mode Architecture)

**Purpose**: Unified graph-based knowledge storage with dual persistence models

#### DataScript Layer (Query Engine)

**Key Components**:

- **DataScript Core**: In-memory Datalog database for queries and reactive updates
- **Schema Management**: Dynamic schema selection based on graph type
- **Query Engine**: Unified Datalog query processing for both storage modes
- **Transaction Log**: Change tracking and undo/redo with dual persistence

#### Storage Backends

**A. File-Based Graphs (Legacy Mode)**:

- **Purpose**: Traditional file-system storage with markdown/org files
- **Storage**: Direct file I/O with format parsers
- **Limitations**: File system constraints, limited scalability
- **Benefits**: Full file system access, external tool compatibility

**B. Database-Based Graphs (SQLite Mode)**:

- **Purpose**: Modern persistent storage with unlimited scalability
- **Storage**: SQLite database with ACID transactions
- **Technology Stack**: SQLite WASM + OPFS (browser) / Native SQLite (desktop)
- **Benefits**: Unlimited storage, better performance, full-text search integration

#### Unified Data Model

```clojure
;; Block entity structure (file-based graphs)
{:db/id 123
 :block/uuid #uuid "550e8400-e29b-41d4-a716-446655440000"
 :block/content "This is a block of text"
 :block/page {:db/id 456}
 :block/parent {:db/id 122}
 :block/refs [{:db/id 789} {:db/id 890}]}

;; Block entity structure (database-based graphs)
;; Note: :block/content is NOT in the DB graph schema.
;; Block and page text is stored in :block/title.
{:db/id 123
 :block/uuid #uuid "550e8400-e29b-41d4-a716-446655440000"
 :block/title "This is a block of text"
 :block/page {:db/id 456}
 :block/parent {:db/id 122}
 :block/refs [{:db/id 789} {:db/id 890}]
 ;; DB-specific attributes
 :block/tags [{:db/id 999}]          ; refs to logseq.class/Card entity, etc.
 :block/closed-value-property [{:db/id 888}]} ; cardinality/many ref type
```

#### Storage Abstraction Layer

```clojure
;; Storage interface (from datascript.storage)
(defprotocol IStorage
  (-store [this addr+data-seq delete-addrs])
  (-restore [this addr]))

;; Graph type detection (logseq.db.sqlite.util / frontend.config)
(defn db-based-graph? [graph-name]
  (sqlite-util/db-based-graph? graph-name))
```

### 2. Frontend Application Layer

**Purpose**: User interface and interaction management

**Architecture**:

```mermaid
graph TD
    A[Components Layer] --> B[State Management]
    B --> C[Event Handlers]
    C --> D[Database Layer]

    E[Router] --> A
    F[Plugins] --> C
    G[Workers] --> C

    subgraph "Components"
        H[Pages]
        I[Blocks]
        J[Editor]
        K[Graph View]
        L[Whiteboard]
    end

    A --> H
    A --> I
    A --> J
    A --> K
    A --> L
```

### 3. Storage Integration Layer

**Purpose**: Unified storage abstraction supporting dual persistence modes

#### File System Integration (File-Based Graphs)

**Layers**:

- **File Protocols**: Abstraction over different storage backends
- **Format Parsers**: Markdown/Org-mode processing
- **File Watchers**: Real-time file change detection
- **Conflict Resolution**: Handle concurrent file modifications

#### SQLite Integration (Database-Based Graphs)

**Components**:

- **SQLite WASM**: Browser-based SQLite with OPFS persistence
- **Connection Pooling**: Efficient database connection management
- **Transaction Management**: ACID transactions with WAL mode
- **Storage Serialization**: Transit-based data serialization
- **Full-Text Search**: Integrated search database with automatic indexing

**SQLite Architecture**:

```mermaid
graph TD
    A[Database Worker] --> B[SQLite WASM]
    B --> C[OPFS Storage]
    B --> D[Main Database]
    B --> E[Search Database]
    B --> F[Client Ops Database]

    G[DataScript] --> H[Storage Abstraction]
    H --> I[Transit Serialization]
    I --> A

    J[File System] --> H
    K[Background Sync] --> A

    style C fill:#e3f2fd
    style D fill:#fff3e0
    style E fill:#e8f5e8
```

**Key Implementation Details**:

```clojure
;; SQLite storage implementation
(defn new-sqlite-storage [^Object db]
  (reify IStorage
    (-store [_ addr+data-seq _delete-addrs]
      (let [data (map
                  (fn [[addr data]]
                    (let [data' (if (map? data) (dissoc data :addresses) data)
                          addresses (when (map? data)
                                      (when-let [addresses (:addresses data)]
                                        (js/JSON.stringify (bean/->js addresses))))]
                      #js {:$addr addr
                           :$content (sqlite-util/write-transit-str data')
                           :$addresses addresses}))
                  addr+data-seq)]
        (upsert-addr-content! db data)))
    (-restore [_ addr]
      (restore-data-from-addr db addr))))

;; Connection setup with optimizations
(defn- enable-sqlite-wal-mode! [^Object db]
  (.exec db "PRAGMA locking_mode=exclusive")
  (.exec db "PRAGMA journal_mode=WAL")  ; Write-Ahead Logging for better concurrency
  (.exec db "PRAGMA synchronous=NORMAL"))  ; no per-commit fsync (fff75ea17d)
```

### 4. Plugin System

**Purpose**: Extensibility and third-party integrations

**Architecture**:

```mermaid
graph LR
    A[Plugin API] --> B[Plugin Runtime]
    B --> C[Iframe Sandbox]
    B --> D[Web Worker]

    E[Core System] --> A
    F[Plugin Manager] --> B

    subgraph "Plugin Types"
        G[UI Plugins]
        H[Data Plugins]
        I[Integration Plugins]
        J[Theme Plugins]
    end

    C --> G
    D --> H
    B --> I
    A --> J
```

## Platform-Specific Implementations

### Desktop (Electron)

- **Main Process**: System integration and window management
- **Renderer Process**: Web application runtime
- **Native APIs**: File system, notifications, system tray
- **Auto-updater**: Seamless application updates

### Web Platform

- **Progressive Web App**: Offline-first web application
- **Service Workers**: Background sync and caching
- **IndexedDB**: Browser-based data persistence
- **File System Access API**: Modern browser file integration

### Mobile (Capacitor)

- **Web View**: ClojureScript application running in a Capacitor web view
- **Native Plugins**: Platform-specific functionality via Capacitor plugins
- **Background Sync**: Data synchronization while offline
- **Performance Optimization**: Mobile-specific optimizations

## Data Flow Architecture

### Write Operations (Dual-Mode)

```mermaid
sequenceDiagram
    participant UI as User Interface
    participant Handler as Event Handler
    participant DS as DataScript
    participant SA as Storage Abstraction
    participant SQLite as SQLite DB
    participant FS as File System
    participant Sync as Sync Engine

    UI->>Handler: User action
    Handler->>DS: Transaction
    DS->>SA: Persist data

    alt Database Graph
        SA->>SQLite: Store EAV data
        SQLite->>SA: Confirm storage
    else File Graph
        SA->>FS: Write file (async)
        FS->>SA: Confirm write
    end

    SA->>Handler: Storage complete
    Handler->>Sync: Queue sync (if enabled)
    Handler->>UI: State update
```

### Graph Initialization

```mermaid
sequenceDiagram
    participant App as Application
    participant Config as Graph Config
    participant Worker as DB Worker
    participant SQLite as SQLite Engine
    participant DS as DataScript

    App->>Config: Determine graph type

    alt Database Graph
        Config->>Worker: Initialize DB worker
        Worker->>SQLite: Create/open SQLite DB
        SQLite->>Worker: DB connection
        Worker->>DS: Setup with SQLite storage
    else File Graph
        Config->>DS: Setup with file storage
    end

    DS->>App: Ready for operations
```

### Read Operations (Dual-Mode)

```mermaid
sequenceDiagram
    participant UI as User Interface
    participant State as State Management
    participant DS as DataScript
    participant SA as Storage Abstraction
    participant SQLite as SQLite DB
    participant FS as File System
    participant Cache as Query Cache

    UI->>State: Subscribe to data
    State->>DS: Datalog query
    DS->>Cache: Check cache

    alt Cache miss
        DS->>SA: Request data
        alt Database Graph
            SA->>SQLite: Query storage
            SQLite->>SA: Return data
        else File Graph
            SA->>FS: Read files
            FS->>SA: Return data
        end
        SA->>DS: Restore data
    end

    DS->>State: Query result
    State->>UI: Data update
```

## Scalability and Performance

### Graph Type Comparison

| Feature               | File-Based Graphs  | Database-Based Graphs      |
| --------------------- | ------------------ | -------------------------- |
| **Storage Limit**     | File system limit  | Unlimited (SQLite)         |
| **Performance**       | Memory limited     | Disk-based, scalable       |
| **ACID Transactions** | No                 | Yes (SQLite)               |
| **Full-Text Search**  | Limited            | Integrated (FTS5)          |
| **Concurrent Access** | File locking       | SQLite WAL mode            |
| **Backup/Recovery**   | File-based         | Database snapshots         |
| **External Tools**    | Full compatibility | Limited                    |
| **Migration**         | Not needed         | Automatic schema migration |

### Performance Optimizations

#### Common Optimizations

- **Lazy Loading**: Components and data loaded on demand
- **Virtual Scrolling**: Efficient rendering of large lists
- **Query Optimization**: Indexed queries and result caching
- **Memory Management**: Garbage collection and object pooling

#### Database-Specific Optimizations

- **OPFS SAH Pool**: OPFS file handle pool (initialCapacity: 20 handles)
- **WAL Mode**: Write-Ahead Logging for better concurrency
- **Vacuum Operations**: Automatic database maintenance and garbage collection
- **Search Index Management**: Automatic FTS index updates with triggers
- **Transit Serialization**: Optimized data serialization for storage

```clojure
;; Database garbage collection (every 1 month)
;; src/main/frontend/worker/db_core.cljs
(defn- gc-sqlite-dbs! [sqlite-db datascript-conn {:keys [full-gc?]}]
  (let [last-gc-at (:kv/value (d/entity @datascript-conn :logseq.kv/graph-last-gc-at))]
    (when (or full-gc?
              (nil? last-gc-at)
              (not (number? last-gc-at))
              (> (- (common-util/time-ms) last-gc-at) (* 30 24 3600 1000))) ; 1 month ago
      (log/info :gc-sqlite-dbs "gc current graph")
      (sqlite-gc/gc-kvs-table! sqlite-db {:full-gc? full-gc?})
      (.exec sqlite-db "VACUUM")
      (ldb/transact! datascript-conn [{:db/ident :logseq.kv/graph-last-gc-at
                                       :kv/value (common-util/time-ms)}]
                     {:skip-validate-db? true
                      :persist-op? false}))))
```

### Concurrent Operations

- **Transaction Serialization**: Atomic database operations with ACID guarantees
- **Optimistic Updates**: Immediate UI feedback with rollback capability
- **Conflict Resolution**: Advanced merge strategies for concurrent edits
- **Background Processing**: Non-blocking operations in dedicated workers
- **Real-time Collaboration**: Enhanced support for concurrent multi-user editing

## Security Architecture

### Data Protection

#### File-Based Graphs

- **File System Security**: OS-level file permissions
- **Local Encryption**: Optional at-rest encryption
- **Direct File Access**: Full user control over files

#### Database-Based Graphs

- **OPFS Isolation**: Origin Private File System sandboxing
- **SQLite Security**: Built-in transaction integrity
- **Data Integrity**: ACID transactions prevent corruption
- **Automatic Backups**: Database snapshots for recovery

#### Universal Security Features

- **Secure Transmission**: HTTPS/TLS for sync operations
- **Plugin Sandboxing**: Isolated execution environments
- **Content Security Policy**: XSS protection
- **No Network Dependencies**: Core functionality works offline

### Privacy Features

- **No Tracking**: No analytics by default
- **Local Storage**: Data remains on user devices (both modes)
- **Optional Telemetry**: User-controlled usage statistics
- **Open Source Transparency**: Auditable codebase
- **Origin Isolation**: Browser-based graphs isolated per origin

## Deployment Architecture

### Distribution Channels

- **Desktop**: Native installers and package managers
- **Web**: CDN-hosted web application
- **Mobile**: App stores and direct distribution

### Update Mechanisms

- **Auto-updates**: Background application updates
- **Progressive Enhancement**: Incremental feature rollouts
- **Rollback Capability**: Safe update reversals
- **Plugin Updates**: Independent plugin versioning

## Migration and Transition

### Graph Type Selection

**When to Choose File-Based Graphs:**

- External tool integration required (Obsidian, other markdown editors)
- File system access needed for automation
- Small to medium graph sizes (< 10,000 blocks)
- Simple backup requirements (file copying)

**When to Choose Database-Based Graphs:**

- Large graph sizes (> 10,000 blocks)
- Performance is critical
- Advanced search capabilities needed
- Real-time collaboration planned
- Mobile-first usage patterns

### Migration Process

**File-to-Database Migration:**

1. **Export**: Generate graph-agnostic EDN export from file-based graph
2. **Create**: Initialize new database-based graph
3. **Import**: Load EDN data into SQLite-backed graph
4. **Verify**: Confirm data integrity and relationships
5. **Switch**: Update graph references and sync settings

**Migration Implementation:**

```clojure
;; Export from file graph
(defn export-graph-to-edn [repo]
  "Export graph data in universal EDN format")

;; Import to database graph
(defn import-edn-to-db-graph [repo edn-data]
  "Import EDN data into SQLite-backed graph")

;; Schema migration for database graphs
(defn migrate-db-schema [repo from-version to-version]
  "Handle database schema migrations")
```

### Technology Stack Summary

#### Frontend Architecture

- **UI Framework**: React with Rum (ClojureScript)
- **State Management**: DataScript with reactive subscriptions
- **Query Language**: Datalog (unified across storage modes)
- **Routing**: Reitit for navigation

#### Storage Technologies

- **File Graphs**: Direct file I/O with format parsers
- **Database Graphs**: SQLite WASM + OPFS (browser), Native SQLite (desktop)
- **Serialization**: Transit for efficient data serialization
- **Search**: Integrated FTS5 full-text search

#### Platform Integration

- **Desktop**: Electron with native SQLite bindings
- **Web**: SQLite WASM with Origin Private File System
- **Mobile**: Capacitor with SQLite integration

#### Development Tools

- **Language**: ClojureScript for consistency
- **Build System**: shadow-cljs for compilation
- **Testing**: Comprehensive test suites for both storage modes

---

_This comprehensive architecture documentation covers Logseq's evolution from a file-based system to a sophisticated dual-mode platform supporting both traditional file storage and modern SQLite-backed persistence, providing users with the flexibility to choose the best approach for their specific use cases while maintaining full data portability between modes._
