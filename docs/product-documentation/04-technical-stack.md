# Technical Stack

## Technology Overview

Logseq is built on a modern, carefully selected technology stack that prioritizes performance, maintainability, and extensibility. The choice of functional programming languages and reactive patterns reflects the application's focus on data relationships and user interaction.

## Frontend Technologies

### Core Language: ClojureScript

**Why ClojureScript?**
- **Functional Programming**: Immutable data structures and pure functions
- **Lisp Syntax**: Powerful macro system for DSL creation
- **JavaScript Interop**: Seamless integration with JavaScript ecosystem
- **REPL-Driven Development**: Interactive development and debugging

**Benefits**:
- Reduced complexity in state management
- Powerful data transformation capabilities
- Strong support for graph data structures
- Hot reloading and live coding

### UI Framework: React + Rum

**React (18.3.1)**
- Industry-standard UI library
- Virtual DOM for efficient updates
- Component-based architecture
- Extensive ecosystem and tooling

**Rum Framework**
- ClojureScript wrapper for React
- Functional component definitions
- Built-in state management
- Reactive subscriptions

```clojure
(rum/defc block-component < rum/reactive
  [block]
  (let [content (rum/react (:block/content block))]
    [:div.block
     [:div.block-content content]]))
```

### State Management

**Architecture**:
```mermaid
graph TD
    A[Rum Components] --> B[Reactive Atoms]
    B --> C[DataScript DB]
    C --> D[Event Handlers]
    D --> B
    
    E[User Actions] --> D
    F[File Changes] --> D
    G[Plugin Events] --> D
    
    style B fill:#e3f2fd
    style C fill:#fff3e0
```

**Key Technologies**:
- **Atoms**: Clojure's reference types for state
- **Reactive Subscriptions**: Automatic UI updates
- **Event System**: Decoupled state mutations

### Database: DataScript

**What is DataScript?**
- In-memory database implementing Datalog
- Immutable database values
- Time-travel debugging
- Powerful query language

**Query Example**:
```clojure
;; Find all blocks that reference a specific page
[:find ?block
 :where
 [?block :block/refs ?page]
 [?page :page/name "Artificial Intelligence"]]
```

**Benefits**:
- Natural representation of graph relationships
- Efficient querying of complex data patterns
- Automatic indexing and optimization
- Transaction-based updates

## Build Tools and Development

### Shadow-CLJS (2.28.23)

**Primary Build Tool**:
- ClojureScript compilation to JavaScript
- Hot reloading during development
- Code splitting and optimization
- Multiple build targets (web, desktop, mobile)

**Configuration**:
```clojure
{:source-paths ["src/main" "src/dev"]
 :dependencies [[reagent "1.1.1"]
                [datascript "1.3.15"]]
 :builds {:app {:target :browser
                :output-dir "public/js"
                :modules {:main {:init-fn frontend.core/init}}}}}
```

### Gulp (4.0.2)

**Asset Processing**:
- CSS compilation and optimization
- Static asset management
- Development server setup
- Build task orchestration

### Webpack (5.98.0)

**Module Bundling**:
- JavaScript dependency management
- Asset optimization
- Development and production builds
- Plugin and worker compilation

## Styling and UI Components

### CSS Framework: Tailwind CSS (3.3.5)

**Utility-First Approach**:
- Rapid UI development
- Consistent design system
- Responsive design utilities
- Custom configuration for Logseq themes

### Component Libraries

**Radix UI**:
- Accessible UI primitives
- Unstyled, composable components
- Keyboard navigation support
- ARIA compliance

**Custom UI System (SHUI)**:
- Logseq-specific component library
- Based on shadcn/ui design patterns
- ClojureScript wrappers for React components
- Consistent styling and behavior

```clojure
(defn button [props & children]
  (shui/button 
   (merge {:class "logseq-button"} props)
   children))
```

## Desktop Platform: Electron

### Electron Integration

**Main Process**:
- Application lifecycle management
- Native OS integration
- File system access
- Auto-updater functionality

**Renderer Process**:
- Web application runtime
- IPC communication with main process
- Plugin sandbox environment

**Key Features**:
- Native file dialogs
- System tray integration
- Protocol handlers
- Deep OS integration

### Desktop-Specific Technologies

```javascript
// IPC communication example
const { ipcRenderer } = require('electron')

// From renderer to main
ipcRenderer.invoke('read-file', filePath)
  .then(content => updateEditor(content))

// From main to renderer
ipcRenderer.on('file-changed', (event, filePath) => {
  reloadFile(filePath)
})
```

## Mobile Platform: React Native

### Mobile Architecture

**React Native Components**:
- Native UI components
- Platform-specific styling
- Performance optimizations
- Native module integration

**Capacitor (7.2.0)**:
- Cross-platform native runtime
- Plugin ecosystem
- Web view integration
- Native API access

### Mobile-Specific Features

**Plugins Used**:
- `@capacitor/filesystem` - File system access
- `@capacitor/camera` - Camera integration
- `@capacitor/clipboard` - Clipboard operations
- `@capacitor/haptics` - Tactile feedback

## Testing and Quality Assurance

### Testing Framework: Playwright (1.51.0)

**End-to-End Testing**:
```javascript
// Example test
test('should create new block', async ({ page }) => {
  await page.goto('/');
  await page.click('.editor');
  await page.type('.editor', 'New block content');
  await page.press('Enter');
  
  expect(await page.textContent('.block')).toContain('New block content');
});
```

### Code Quality Tools

**Karma (6.4.4)**:
- Unit test runner
- Browser testing environment
- Coverage reporting

**Clj-kondo**:
- ClojureScript linting
- Static analysis
- Code quality enforcement

**Stylelint (13.8.0)**:
- CSS linting and formatting
- Style guide enforcement

## Development Dependencies

### Key Development Tools

```json
{
  "devDependencies": {
    "@playwright/test": "1.51.0",
    "shadow-cljs": "2.28.23",
    "gulp": "4.0.2",
    "webpack": "5.98.0",
    "tailwindcss": "3.3.5",
    "typescript": "4.4.3"
  }
}
```

### Build Scripts

**Primary Commands**:
```bash
# Development mode
yarn watch

# Production build
yarn release

# Mobile development
yarn mobile-watch

# Testing
yarn test
```

## Runtime Dependencies

### Core Libraries

**Data Management**:
- `datascript` - Graph database
- `mldoc` - Document parsing
- `jszip` - Archive handling

**UI and Interaction**:
- `react` (18.3.1) - UI framework
- `react-dom` (18.3.1) - DOM rendering
- `@dnd-kit/core` - Drag and drop
- `react-virtuoso` - Virtual scrolling

**File and Media Handling**:
- `pdfjs-dist` (4.2.67) - PDF processing
- `html2canvas` - Screenshot generation
- `photoswipe` - Image galleries

### Platform-Specific Dependencies

**Desktop (Electron)**:
- Native file system access
- OS integration APIs
- Auto-updater services

**Web Platform**:
- Service worker APIs
- IndexedDB storage
- File System Access API

**Mobile (Capacitor)**:
- Native camera access
- File system integration
- Platform-specific UI components

## Performance Optimizations

### Bundle Optimization

**Code Splitting**:
- Separate bundles for core, plugins, and workers
- Lazy loading of non-critical components
- Dynamic imports for large dependencies

**Tree Shaking**:
- Elimination of unused code
- Optimized bundle sizes
- Dead code elimination

### Runtime Performance

**Virtual DOM Optimization**:
- Efficient reconciliation algorithms
- Memoization of expensive computations
- Batched state updates

**Memory Management**:
- Garbage collection optimization
- Object pooling for frequently created objects
- Efficient data structure usage

## Security Considerations

### Content Security Policy

```html
<meta http-equiv="Content-Security-Policy" 
      content="default-src 'self'; 
               script-src 'self' 'unsafe-eval'; 
               style-src 'self' 'unsafe-inline';">
```

### Plugin Sandboxing

**Iframe Isolation**:
- Separate execution contexts for plugins
- Limited API surface for security
- Message-based communication

**Web Worker Security**:
- Isolated execution environment
- Restricted access to sensitive APIs
- Controlled data flow

## Deployment and Distribution

### Build Targets

**Web Application**:
- Static site generation
- CDN-optimized bundles
- Progressive web app features

**Desktop Applications**:
- Platform-specific installers
- Auto-update mechanisms
- Code signing for security

**Mobile Applications**:
- App store distribution
- Over-the-air updates
- Platform-specific optimizations

---

*This technical stack documentation provides comprehensive insight into the technologies and architectural decisions that power Logseq across all platforms.*
