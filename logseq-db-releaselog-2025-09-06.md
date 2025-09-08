# 🚀 Logseq DB Release Log - September 6, 2025

*Two weeks of incredible progress for your Personal Knowledge Management experience*

---

## ✨ What's New

### 🛠️ Powerful CLI & API Enhancements
Your workflow just got supercharged! We've added powerful new CLI commands that let you export and append content directly from the command line. Perfect for automation, scripting, and integrating Logseq into your broader toolchain.

- **Export commands** - Save your markdown content with a single command
- **Append functionality** - Add content programmatically to your pages
- **New API endpoint** - `get_page_properties` for developers building on Logseq

### 🔍 Smarter Query System
We've enhanced our query engine with a new `:self-ref` rule that makes page reference queries more intuitive and reliable. Your complex queries will now return exactly the nodes you expect!

---

## 📱 Mobile Experience Transformed

This release brings **major improvements** to the mobile experience:

### 🎯 Navigation Reimagined
- **Sheet-based sidebar** - More intuitive, tap-outside-to-close functionality
- **No more edge swiping** - Navigate naturally without precise gestures
- **Smart safe areas** - Perfect layout on notched and punch-hole devices

### 🔍 Enhanced Search & Discovery
- **Auto-focus search** - Jump right into searching
- **Improved scrolling** - Smoother results browsing
- **Larger fonts** - Better readability for graph navigation

### ✋ Touch Interactions
- **Gesture improvements** - Toggle sidebar without lifting your finger
- **Smart context awareness** - Sidebar won't interrupt your editing flow

---

## ⚡ Performance & Memory Optimizations

We've been working hard under the hood to make Logseq faster and more memory-efficient:

### 🧠 Memory Management
- **Weak references** - Significant memory usage reduction
- **Smarter caching** - Less memory pressure during large graph operations
- **Database optimizations** - Faster block reference rebuilding

### 🏃‍♂️ Speed Improvements
- **RTC optimizations** - Smoother real-time collaboration downloads
- **Build performance** - Faster development and deployment cycles
- **Query efficiency** - Reduced computational overhead

---

## 🐛 Bug Fixes That Matter

### 📝 Block & Page Management
- Fixed property handling when deleting nodes
- Resolved math rendering issues across platforms
- Improved visual feedback for collapsible blocks
- Better namespace handling for page creation

### 🔍 Search & Queries
- Query results now show proper titles instead of UUIDs
- Hidden properties stay hidden in results
- Reference calculations are now accurate
- Improved page sorting by backlinks

### 🎨 UI Polish
- Sticky headers work properly with multiple tables
- Property popups behave consistently
- Fixed datetime display issues
- Better mobile status bar colors
- Resolved popup scrolling problems

### 📱 Mobile-Specific Fixes
- App won't go white when switching back from background
- Database worker crashes are handled gracefully  
- iOS sharing works reliably
- Content sharing between mobile apps improved

---

## 🔧 Developer Experience

### 📚 Better Documentation
- Enhanced CLI usage descriptions
- Improved error messages and handling
- Updated development prompts and guidelines

### 🏗️ Build System
- Upgraded to Java 21 for Android builds
- Webpack optimizations for faster builds
- Updated dependencies including DataScript and Electron

---

## 🏗️ Code Quality Improvements

We've refactored significant parts of the codebase to improve maintainability:

- Simplified block reference handling
- Removed redundant macro usage
- Better database transaction management
- Cleaner code organization across modules

---

## 🙏 Thank You, Contributors!

This release was made possible by our amazing community and core team:

- **@Charlie** - Mobile experience enhancements and i18n improvements
- **@Gabriel Horner** - CLI tools and publishing infrastructure  
- **@Tienson Qin** - Core functionality and mobile optimizations
- **@rcmerci** - Performance optimizations and database improvements
- **@dependabot[bot]** - Keeping our dependencies secure and up-to-date

---

## 🔗 Links & Resources

- **GitHub Repository**: https://github.com/logseq/logseq
- **Documentation**: https://docs.logseq.com
- **Community Discord**: https://discord.gg/logseq
- **Report Issues**: https://github.com/logseq/logseq/issues

---

*Happy knowledge building! 🧠✨*

**The Logseq Team**