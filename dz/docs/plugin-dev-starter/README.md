# Logseq Plugin Development - Starter Documentation

Comprehensive documentation to help you build Logseq plugins quickly.

## 📚 Documentation Structure

### Getting Started

- **[Quick Start Guide](01-quick-start.md)** - Get your first plugin running in minutes
- **[Plugin Basics](02-plugin-basics.md)** - Understand core concepts and architecture
- **[Development Setup](03-development-setup.md)** - Set up your development environment

### Core Concepts

- **[API Overview](04-api-overview.md)** - Complete overview of the Logseq API
- **[Working with Blocks](05-blocks-and-pages.md)** - Create, read, update, and delete blocks
- **[UI Components](06-ui-components.md)** - Build custom user interfaces
- **[Commands & Shortcuts](07-commands-shortcuts.md)** - Register slash commands and keyboard shortcuts

### Advanced Topics

- **[State Management](08-state-management.md)** - Manage plugin state and persistence
- **[Event System](09-events.md)** - Listen and respond to Logseq events
- **[Database Operations](10-database.md)** - Query and manipulate the graph database
- **[Settings & Configuration](11-settings.md)** - Create user-configurable settings

### Best Practices

- **[Common Patterns](12-common-patterns.md)** - Proven patterns for common tasks
- **[Error Handling](13-error-handling.md)** - Robust error handling strategies
- **[Performance](14-performance.md)** - Optimize your plugin for speed
- **[Testing](15-testing.md)** - Test your plugin effectively

### Reference

- **[API Reference](16-api-reference.md)** - Complete API documentation
- **[Examples](17-examples.md)** - Real-world plugin examples
- **[Troubleshooting](18-troubleshooting.md)** - Common issues and solutions
- **[Migration Guide](19-migration-guide.md)** - Upgrading from older versions

## 🚀 Quick Links

### Essential APIs

- `logseq.Editor` - Block and page manipulation
- `logseq.App` - Application-level operations
- `logseq.UI` - User interface components
- `logseq.DB` - Database queries
- `logseq.Assets` - Asset management

### Common Tasks

- [Insert text at cursor](05-blocks-and-pages.md#inserting-at-cursor)
- [Create a new block](05-blocks-and-pages.md#creating-blocks)
- [Register a slash command](07-commands-shortcuts.md#slash-commands)
- [Show a notification](06-ui-components.md#notifications)
- [Query the database](10-database.md#basic-queries)
- [Save plugin settings](11-settings.md#saving-settings)

## 💡 Learning Path

### Beginner (Start Here!)

1. Read [Quick Start Guide](01-quick-start.md)
2. Follow [Plugin Basics](02-plugin-basics.md)
3. Try [Examples](17-examples.md)

### Intermediate

1. Master [Blocks and Pages](05-blocks-and-pages.md)
2. Learn [UI Components](06-ui-components.md)
3. Understand [State Management](08-state-management.md)

### Advanced

1. Deep dive into [Database Operations](10-database.md)
2. Study [Event System](09-events.md)
3. Apply [Performance](14-performance.md) techniques

## 📖 Additional Resources

- [Official Plugin Marketplace](https://github.com/logseq/marketplace)
- [Plugin Examples Repository](https://github.com/logseq/logseq-plugin-samples)
- [Logseq Discord Community](https://discord.gg/logseq)
- [TypeDoc API Documentation](../index.html)

## 🤝 Contributing

Found an issue or want to improve the documentation? Contributions are welcome!

---

**Last Updated**: December 31, 2025
**SDK Version**: @logseq/libs v0.2.9
