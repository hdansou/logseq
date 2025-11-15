# Architecture Documentation

High-level system architecture and design documentation for Logseq.

## Overview

This folder contains comprehensive architecture documentation covering Logseq's system design, technical stack, and data architecture.

## Quick Links

### For Stakeholders

- **[Executive Summary](executive-summary.md)** - Business overview and value proposition

### For Product Managers

- **[Application Overview](application-overview.md)** - Product features and capabilities

### For Architects & Senior Developers

- **[System Architecture](system-architecture.md)** - Technical architecture and design patterns
- **[Technical Stack](technical-stack.md)** - Technologies, frameworks, and tools
- **[Data Architecture](data-architecture.md)** - Database design and data flow

### For RTC Developers

- **[RTC vs Sync Comparison](rtc-vs-sync.md)** - Understanding collaboration approaches

## Full Index

| Document                                           | Description                                                       | Target Audience                    |
| -------------------------------------------------- | ----------------------------------------------------------------- | ---------------------------------- |
| [executive-summary.md](executive-summary.md)       | High-level business overview, value propositions, market position | Stakeholders, leadership           |
| [application-overview.md](application-overview.md) | What Logseq is, use cases, differentiators                        | Product managers, new team members |
| [system-architecture.md](system-architecture.md)   | System design, component architecture, C4 diagrams                | Architects, senior developers      |
| [technical-stack.md](technical-stack.md)           | ClojureScript, React, Electron, build tools                       | All developers                     |
| [data-architecture.md](data-architecture.md)       | DataScript, SQLite, data models                                   | Backend developers, DB admins      |
| [rtc-vs-sync.md](rtc-vs-sync.md)                   | Real-time collaboration vs file sync comparison                   | RTC developers                     |

## Architecture Diagrams

Diagrams are embedded within the documents using Mermaid syntax. Key diagrams include:

- **System Context Diagram** (C4 Level 1) - in system-architecture.md
- **Container Diagram** (C4 Level 2) - in system-architecture.md
- **Component Diagram** (C4 Level 3) - in system-architecture.md
- **Data Flow Diagrams** - in data-architecture.md
- **Technology Stack Visualization** - in technical-stack.md

## Maintenance

### Update Triggers

Update these documents when:

- **Major architectural changes** occur (new platforms, major refactoring)
- **Technology stack updates** (framework versions, new dependencies)
- **Data model changes** (schema updates, new storage mechanisms)
- **Collaboration features** change (RTC/Sync updates)

### Ownership

- **Maintainer**: Tech Lead, Architecture Team
- **Review Frequency**: Quarterly
- **Last Updated**: November 15, 2025

### Related Documentation

- **Feature Documentation**: [../03-features/](../03-features/)
- **Developer Guides**: [../05-developer-guides/](../05-developer-guides/)
- **API Reference**: [../04-api-reference/](../04-api-reference/)

## Contributing

To update architecture documentation:

1. Ensure changes reflect actual system state
2. Update relevant diagrams
3. Cross-reference related documents
4. Follow [documentation standards](../08-processes/documentation/update-process.md)
5. Get review from architecture team

---

**Navigation**: [Back to Docs Home](../README.md)
