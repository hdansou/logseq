# AI Prompt Template for Detective-Style Code Documentation

## The Master Prompt

Use this prompt template to generate comprehensive, investigative-style documentation for any codebase component:

---

## PROMPT TEMPLATE

````
ACT as a detective and investigative developer. I need you to create a comprehensive developer guide for [COMPONENT_NAME] in this codebase.

## Investigation Scope

The component/feature I want documented is: [COMPONENT_NAME]
Main purpose: Provide a clear and concise reference for developers using the [COMPONENT_NAME]
Entry points (if known): [FILE_PATHS_OR_FUNCTION_NAMES]

## Documentation Requirements

### 1. Investigation Approach
- Start from user-facing entry points (UI, API, CLI)
- Trace execution flows through the codebase
- Identify all key files, functions, and data structures
- Discover architectural patterns and design decisions
- Uncover hidden behaviors and edge cases
- Find performance characteristics and bottlenecks

### 2. Structure (Use this exact format)

```markdown
# [Component Name]: A Detective's Investigation Report

## 🔍 Case Summary
- Brief overview of what this component does
- Why it exists (the problem it solves)
- Key discovery highlights

## 📋 Table of Contents
[Auto-generate based on sections]

## Architecture Overview
[High-level diagram + brief explanation]

## The Crime Scene Map
[System architecture diagram showing all major pieces]

## Evidence #1: [First Major Aspect]
### The Witness Files
[Key files involved with their roles]

### [Detailed Investigation]
[Deep dive with code references]

### [Mermaid Diagram]
[Visual representation of this aspect]

## Evidence #2: [Second Major Aspect]
[Repeat structure]

## Evidence #3: [Third Major Aspect]
[Repeat structure]

[Continue for all major aspects...]

## Key Witnesses (Main Files)
[Table of all important files with roles]

## Critical Findings
[Numbered list of important discoveries with evidence]

## Known Limitations
[Issues, edge cases, technical debt]

## [API/Interface] Reference
[Complete reference for public interfaces]

## Message Flow Reference
[Sequence diagrams for key operations]

## Debugging Guide
[Practical debugging tips and tools]

## Performance Benchmarks
[Performance characteristics if applicable]

## Future Improvements
[Recommended enhancements]

## Case Closed
[Summary verdict]

## Appendix: File Tree
[Visual representation of file structure]
````

### 3. Diagram Requirements

Create Mermaid diagrams for:

- **Architecture diagrams**: Show component relationships
- **Sequence diagrams**: Show execution flows with timing
- **Flowcharts**: Show decision trees and processes
- **Graph diagrams**: Show data structures (trees, graphs, etc.)
- **State diagrams**: Show state transitions if applicable

Use these Mermaid styles:

- Color-code different layers/components
- Add descriptive notes for complex steps
- Keep diagrams focused (max 10-15 nodes per diagram)
- Split complex flows into multiple diagrams

### 4. Writing Style

Detective Metaphors:

- "Evidence" for findings from code analysis
- "Witnesses" for source files
- "Investigation Trail" for execution paths
- "Crime Scene" for system architecture
- "Case Closed" for conclusions
- "Critical Findings" for important discoveries

Tone:

- Professional but engaging
- Use analogies and metaphors
- Explain "why" not just "what"
- Highlight surprising discoveries with 🔍
- Mark warnings with ⚠️
- Mark critical issues with ❌
- Mark good practices with ✅

### 5. Code References

For every claim, provide:

- File path (e.g., `src/main/frontend/example.cljs:123`)
- Line numbers where relevant
- Actual code snippets in code blocks
- Explanatory comments

Format:
**Location**: `path/to/file.ext:line-number`

```language
// Actual code snippet
```

### 6. Practical Sections Required

Must include:

- **API Reference**: Complete interface documentation
- **Debugging Guide**: How to debug this component
- **Performance Notes**: Speed, memory, scaling characteristics
- **Limitations**: Known issues and constraints
- **Future Improvements**: What could be better

### 7. Investigation Process

Follow this methodology:

1. **Find Entry Points**: UI components, API endpoints, exported functions
2. **Trace Forward**: Follow execution from entry to completion
3. **Trace Backward**: Find what triggers this code
4. **Map Dependencies**: Identify all files involved
5. **Discover State**: Track state changes and data flow
6. **Identify Patterns**: Find architectural patterns
7. **Uncover Edge Cases**: Look for error handling, validations
8. **Benchmark**: Understand performance characteristics

### 8. Special Investigations

Look for and document:

- 🔍 **Hidden Behaviors**: Undocumented features or side effects
- ⚠️ **Gotchas**: Surprising behaviors or footguns
- 🎯 **Best Practices**: How the code should be used
- 🐛 **Common Bugs**: Known issues or error-prone areas
- ⚡ **Performance Tips**: Optimization opportunities
- 🔒 **Security Concerns**: Potential vulnerabilities
- 📊 **Metrics**: What's measured and logged

## Output Format

Save the documentation as: `docs/[component-name]-developer-guide.md`

Make it:

- **Comprehensive**: Cover all aspects
- **Accurate**: Every claim backed by code evidence
- **Practical**: Include debugging, examples, references
- **Visual**: Heavy use of diagrams
- **Engaging**: Detective narrative style
- **Navigable**: Clear table of contents and sections

## Example Quality Standards

For each major flow, provide:
✅ At least one sequence diagram
✅ File locations with line numbers
✅ Code snippets with explanations
✅ Performance characteristics
✅ Error handling behavior
✅ Common usage examples

## Investigation Deliverables

1. Complete markdown document (docs/[name]-developer-guide.md)
2. Minimum 8 mermaid diagrams
3. All major flows documented with sequences
4. Complete API/interface reference
5. Debugging guide with practical examples
6. Performance benchmarks (if applicable)
7. Known limitations with severity ratings

```

---

## Usage Examples

### Example 1: Documenting a Feature

```

ACT as a detective and investigative developer. I need you to create a comprehensive developer guide for the Block Editing System in this codebase.

## Investigation Scope

The component/feature I want documented is: Block Editing System
Main purpose: Allow users to create, edit, and manipulate blocks in the outliner
Entry points (if known):

- frontend.handler.editor namespace
- frontend.components.editor component
- Block editing keyboard shortcuts

[Include all requirements from template above]

```

### Example 2: Documenting an API

```

ACT as a detective and investigative developer. I need you to create a comprehensive developer guide for the Plugin API System in this codebase.

## Investigation Scope

The component/feature I want documented is: Plugin API System
Main purpose: Allow third-party plugins to extend Logseq functionality
Entry points (if known):

- src/main/frontend/extensions/api.cljs
- Plugin SDK interface
- IPC communication layer

[Include all requirements from template above]

```

### Example 3: Documenting Data Flow

```

ACT as a detective and investigative developer. I need you to create a comprehensive developer guide for the Database Transaction Pipeline in this codebase.

## Investigation Scope

The component/feature I want documented is: Database Transaction Pipeline
Main purpose: Handle all database writes with transactions, validation, and syncing
Entry points (if known):

- DataScript transaction processing
- SQLite transaction handling
- RTC sync integration

[Include all requirements from template above]

```

---

## Customization Options

### For Different Project Types

**Frontend Component**:
Add sections:
- Component Props Reference
- State Management
- Render Performance
- Event Handling
- Style Guide

**Backend Service**:
Add sections:
- Request/Response Flows
- Error Handling Strategy
- Retry Logic
- Rate Limiting
- Database Queries

**Data Processing Pipeline**:
Add sections:
- Input/Output Formats
- Transformation Steps
- Error Recovery
- Batch Processing
- Memory Usage

**Algorithm Implementation**:
Add sections:
- Time Complexity
- Space Complexity
- Edge Cases
- Alternative Approaches
- Optimization Opportunities

### Diagram Type Selection

Choose diagram types based on component:

| Component Type | Recommended Diagrams |
|---------------|---------------------|
| UI Component | Component tree, State machine, Event flow |
| API Endpoint | Sequence diagram, Error flow, Data flow |
| Data Pipeline | Flowchart, Data transformation, State diagram |
| Worker/Thread | Thread communication, Message flow, State machine |
| Database | ER diagram, Query flow, Transaction flow |
| Algorithm | Flowchart, Complexity graph, Data structure |

---

## Quality Checklist

Before considering documentation complete, verify:

- [ ] All major flows have sequence diagrams
- [ ] Every claim has file location + line number
- [ ] Code snippets are accurate (not pseudocode)
- [ ] Mermaid diagrams render correctly
- [ ] Table of contents is complete
- [ ] API reference is comprehensive
- [ ] Debugging guide is practical
- [ ] Performance notes are included
- [ ] Limitations are clearly marked
- [ ] Future improvements are realistic
- [ ] File tree is accurate
- [ ] Examples are runnable/testable
- [ ] Links to files are correct
- [ ] Technical terms are explained
- [ ] Analogies are helpful not confusing

---

## Advanced Techniques

### Multi-File Tracing

When a feature spans many files:
1. Create a "File Journey" section
2. Show the path with numbered steps
3. Use sequence diagrams to connect them
4. Provide a file dependency graph

### State Machine Documentation

For stateful components:
1. Draw state transition diagram
2. List all states with descriptions
3. List all transitions with triggers
4. Show edge cases (error states)
5. Provide state inspection methods

### Performance Investigation

Include:
1. Benchmark results (if measurable)
2. Bottleneck identification
3. Memory usage patterns
4. Optimization opportunities
5. Profiling guide

### Error Path Documentation

For each major flow:
1. Happy path (success case)
2. Error paths (failure cases)
3. Recovery mechanisms
4. Error messages catalog
5. Debugging tips per error

---

## Prompt Variations

### For Quick Investigation (30 min)

```

ACT as a detective and investigative developer. I need a QUICK investigation guide for [COMPONENT].

Focus on:

- System architecture diagram
- Main execution flow (one sequence diagram)
- Key files table
- Critical findings (top 5)
- Common usage examples

Skip:

- Detailed API reference
- Performance benchmarks
- Future improvements

Time limit: Create this in one focused investigation session.

```

### For Deep Dive (2+ hours)

```

ACT as a detective and investigative developer. I need an EXHAUSTIVE investigation guide for [COMPONENT].

Include EVERYTHING:

- Multiple architecture views
- Every execution path with diagrams
- Complete API reference with examples
- Performance benchmarks with data
- Security analysis
- Code coverage analysis
- Comparison with alternatives
- Migration guide from old approach
- Testing strategy
- Deployment considerations

Leave no stone unturned. This will be the definitive reference.

```

### For Maintenance Documentation

```

ACT as a detective and investigative developer. I need a MAINTENANCE guide for [COMPONENT].

Focus on:

- How to modify safely
- Testing checklist
- Common changes and patterns
- Gotchas and footguns
- Rollback procedures
- Dependencies to watch
- Breaking change guidelines

Audience: Developers who need to change this code.

```

---

## Output Optimization Tips

### For Better Diagrams

Prompt addition:
```

For all Mermaid diagrams:

- Use consistent color schemes (provide hex codes)
- Add descriptive notes at complex points
- Keep node labels concise (< 40 chars)
- Use subgraphs for logical grouping
- Add legends for symbols/colors
- Ensure left-to-right or top-to-bottom flow

```

### For Better Code Examples

Prompt addition:
```

For all code snippets:

- Include full context (imports, setup)
- Add inline comments for clarity
- Show actual values, not placeholders
- Include error handling
- Mark important lines with arrows (→)
- Provide "why" comments, not "what"

```

### For Better Navigation

Prompt addition:
```

For navigation:

- Create anchor links in table of contents
- Add "back to top" links in long sections
- Cross-reference related sections
- Create a visual section map diagram
- Add breadcrumbs for subsections

```

---

## Common Pitfalls to Avoid

Tell the AI to avoid:

❌ Generic descriptions without code references
❌ Pseudocode instead of actual code
❌ Diagrams without labels or legends
❌ Claims without evidence (file + line)
❌ Skipping error/edge cases
❌ Missing performance characteristics
❌ Incomplete API references
❌ No practical debugging examples

✅ Do instead:
- Every claim = code reference
- Real code from the actual codebase
- Clear, labeled diagrams
- File:line citations
- Comprehensive edge case coverage
- Performance notes with data
- Complete API coverage
- Practical debugging workflows

---

## Template Variables

Replace these in the prompt:

| Variable | Example | Purpose |
|----------|---------|---------|
| `[COMPONENT_NAME]` | "Semantic Search" | What you're documenting |
| `[BRIEF_DESCRIPTION]` | "Vector-based search using embeddings" | One-line summary |
| `[FILE_PATHS_OR_FUNCTION_NAMES]` | "frontend.handler.search" | Starting points |
| `[OUTPUT_FILENAME]` | "semantic-search-developer-guide.md" | Where to save |

---

## Success Metrics

Good documentation should:

✅ Allow a new developer to understand the component in 30 minutes
✅ Serve as debugging reference when things go wrong
✅ Answer "why" questions, not just "what"
✅ Include visual aids for complex flows
✅ Be accurate (no outdated information)
✅ Be maintainable (easy to update)
✅ Be discoverable (good TOC and search keywords)

---

## Post-Generation Checklist

After AI generates documentation:

1. **Verify Accuracy**: Check that file paths and line numbers are correct
2. **Test Diagrams**: Ensure all Mermaid diagrams render properly
3. **Run Examples**: Verify code examples actually work
4. **Check Links**: Ensure all internal references are valid
5. **Review Flow**: Read through as a newcomer - is it clear?
6. **Update TOC**: Ensure table of contents matches sections
7. **Add Context**: Add any domain knowledge AI might miss
8. **Format Check**: Ensure consistent markdown formatting

---

**Last Updated**: 2025-10-14
**Template Version**: 1.0
**Tested With**: Claude Code, Claude 3.5 Sonnet

---

## Meta-Prompt (For AI to Self-Improve)

After generating documentation, ask AI:

```

Review the documentation you just created. Identify:

1. What sections were most effective and why?
2. What diagrams provided the most clarity?
3. What could be improved for next time?
4. What patterns emerged that should be templates?
5. What questions would a reader still have?

Then create an improved version addressing these points.

```

This creates a feedback loop for continuously better documentation.
```
