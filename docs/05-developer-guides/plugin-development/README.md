# Plugin Development Guides

Comprehensive guides for developing Logseq plugins in TypeScript, JavaScript, or ClojureScript.

## 🚀 Quick Start

**New to plugin development?** Start here:

1. **[Plugin Development Index](index.md)** ⭐ - Overview and decision guide
2. Choose your language:
   - **[TypeScript Guide](typescript-guide.md)** - Most common choice
   - **[ClojureScript Guide](clojurescript-guide.md)** - For Clojure developers
3. Understand graph types:
   - **[File-Based Graphs Guide](file-graphs.md)**
   - **[Database Graphs Guide](db-graphs.md)**
4. Follow a tutorial:
   - **[Tutorial: Duplicate Block Plugin](tutorial-duplicate-block.md)**

## 📚 All Guides

| Guide                                                      | Description                                     | Language      | Difficulty            |
| ---------------------------------------------------------- | ----------------------------------------------- | ------------- | --------------------- |
| [index.md](index.md)                                       | Master index and decision guide                 | N/A           | Beginner              |
| [typescript-guide.md](typescript-guide.md)                 | Complete TypeScript plugin development guide    | TypeScript    | Beginner-Intermediate |
| [clojurescript-guide.md](clojurescript-guide.md)           | ClojureScript SDK developer guide               | ClojureScript | Intermediate          |
| [clojurescript-detective.md](clojurescript-detective.md)   | Detective-style ClojureScript SDK investigation | ClojureScript | Advanced              |
| [file-graphs.md](file-graphs.md)                           | Developing for file-based (Markdown) graphs     | Any           | Intermediate          |
| [db-graphs.md](db-graphs.md)                               | Developing for database graphs                  | Any           | Intermediate          |
| [tutorial-duplicate-block.md](tutorial-duplicate-block.md) | Step-by-step plugin tutorial                    | TypeScript    | Beginner              |

## 🎯 Choose Your Path

### Path 1: TypeScript Plugin (Most Common)

```
1. Read index.md (overview)
2. Follow typescript-guide.md
3. Review plugin-api overview (../04-api-reference/plugin-api/)
4. Try tutorial-duplicate-block.md
5. Deploy your plugin!
```

### Path 2: ClojureScript Plugin

```
1. Read index.md (overview)
2. Follow clojurescript-guide.md
3. Study clojurescript-detective.md (deep dive)
4. Review plugin-api overview
5. Build and test
```

### Path 3: Universal Plugin (Both Graph Types)

```
1. Read index.md
2. Study file-graphs.md
3. Study db-graphs.md
4. Implement with feature detection
5. Test on both graph types
```

## 🔑 Key Concepts

### Graph Types

Logseq supports two graph modes:

- **File-Based Graphs**: Markdown/Org files on disk
- **Database Graphs**: SQLite-based with structured data

**Most plugins should support both!** See [index.md](index.md) for guidance.

### Plugin API

The Plugin API provides:

- Block operations (create, update, delete)
- Page operations
- Database queries
- UI components
- Event system
- Settings management

Full API reference: [../../04-api-reference/plugin-api/](../../04-api-reference/plugin-api/)

### Development Workflow

1. **Setup**: Install dependencies, configure build
2. **Develop**: Write plugin code, test locally
3. **Build**: Bundle plugin for distribution
4. **Test**: Verify on file & DB graphs
5. **Publish**: Submit to marketplace

## 📖 Related Documentation

### API References

- [Plugin API Overview](../../04-api-reference/plugin-api/overview.md)
- [API Changes 2024-2025](../../04-api-reference/plugin-api/changes-2024-2025.md)
- [HTTP API Guide](../../04-api-reference/http-api/server-guide.md)

### Feature Documentation

- [Database Schema Guide](../../03-features/database-graphs/schema-guide.md)
- [Query System](../../03-features/query-system/overview.md)
- [Tagging System](../../03-features/tagging-classification/system-guide.md)

### Architecture

- [Plugin Ecosystem Overview](../../03-features/plugin-ecosystem/overview.md)
- [System Architecture](../../02-architecture/system-architecture.md)

## 🛠️ Development Tools

### Required Tools

- Node.js (v16+)
- npm or yarn
- TypeScript (for TS plugins)
- Clojure tooling (for CLJS plugins)

### Recommended

- VS Code or IntelliJ
- Logseq desktop app (for testing)
- Git for version control

### Testing

- Manual testing in Logseq
- Unit tests (recommended)
- E2E tests for critical functionality

## 💡 Best Practices

1. **Support both graph types** when possible
2. **Handle errors gracefully**
3. **Provide clear user feedback**
4. **Document your plugin well**
5. **Test thoroughly before publishing**
6. **Keep dependencies minimal**
7. **Follow semantic versioning**

## 🐛 Common Issues

### Plugin Not Loading

- Check manifest.json format
- Verify entry point exists
- Check console for errors

### API Methods Not Working

- Ensure correct Logseq version
- Check API compatibility (file vs DB graphs)
- Review [API Changes](../../04-api-reference/plugin-api/changes-2024-2025.md)

### Build Failures

- Update dependencies
- Clear cache and rebuild
- Check for TypeScript errors

## 🤝 Getting Help

- [Logseq Discord](https://discord.gg/logseq) - #plugin-dev channel
- [Forum](https://discuss.logseq.com) - Plugin development category
- [GitHub Issues](https://github.com/logseq/logseq/issues) - Bug reports
- [Plugin Samples](https://github.com/logseq/logseq-plugin-samples)

## 📝 Example Plugins

Study these for inspiration:

- [logseq-plugin-samples](https://github.com/logseq/logseq-plugin-samples) - Official samples
- Community plugins in the marketplace
- Tutorial in [tutorial-duplicate-block.md](tutorial-duplicate-block.md)

## ✅ Plugin Development Checklist

Before publishing:

- [ ] Works on file-based graphs
- [ ] Works on database graphs
- [ ] Error handling implemented
- [ ] User-facing documentation
- [ ] Screenshots/demo video
- [ ] Version number set
- [ ] manifest.json complete
- [ ] Tested on multiple platforms
- [ ] Code reviewed
- [ ] Ready for marketplace submission

## 🔄 Maintenance

### Update Triggers

- Logseq API changes
- New plugin capabilities
- Framework updates
- Community feedback

### Ownership

- **Maintainer**: Plugin Developer Community
- **Review**: As needed based on API changes
- **Last Updated**: November 15, 2025

---

**Navigation**: [Back to Developer Guides](../README.md) | [Back to Docs Home](../../README.md)
