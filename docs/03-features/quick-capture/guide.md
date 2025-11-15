# Logseq Quick Capture: Complete Guide

## Table of Contents

- [Overview](#overview)
- [How Quick Capture Works](#how-quick-capture-works)
- [Configuration Options](#configuration-options)
- [Template System](#template-system)
- [Page Selection Logic](#page-selection-logic)
- [Mobile Integration](#mobile-integration)
- [Practical Examples](#practical-examples)
- [Advanced Usage](#advanced-usage)
- [Troubleshooting](#troubleshooting)
- [Technical Details](#technical-details)

## Overview

Quick capture is Logseq's feature for rapidly collecting information from external sources or when you're in a hurry. It allows you to quickly save text, URLs, and media content with minimal interruption to your workflow. The feature is particularly powerful on mobile devices where it integrates with the system's share functionality.

### Key Features

- **Smart content formatting** for different media types
- **Flexible template system** with variable substitution
- **Intelligent page targeting** with fallback logic
- **Mobile OS integration** via share intents
- **Multi-format support** for text, URLs, images, and documents

## How Quick Capture Works

### Core Functionality

Quick capture operates through the `frontend.quick-capture/quick-capture` function, which processes incoming content and determines where to place it based on your configuration and current context.

### Input Processing

The system accepts several types of input:

- **URL**: Web links that get formatted appropriately
- **Title**: Display text for the content
- **Content**: The main text content
- **Page**: Target page specification
- **Append**: Flag to control insertion behavior

### Smart Content Formatting

Quick capture intelligently formats different content types:

1. **Video URLs**: Automatically wrapped in `{{video URL}}` format
2. **Twitter/X Links**: Converted to `{{twitter URL}}` format
3. **Regular URLs**: Formatted as proper links with titles
4. **Plain Text**: Inserted as-is with timestamp

## Configuration Options

### Quick Capture Options

Configure behavior in your `config.edn`:

```clojure
:quick-capture-options
{:insert-today? false           ;; Insert into today's journal (default: false)
 :redirect-page? false          ;; Navigate to capture page after saving (default: false)
 :default-page "quick capture"} ;; Default target page (default: "quick capture")
```

**Configuration Details:**

- **`insert-today?`**: When `true`, captures go to today's journal page regardless of other settings
- **`redirect-page?`**: When `true`, navigates you to the target page after capturing
- **`default-page`**: Fallback page when no specific target is provided

### Example Configurations

#### Research-Focused Setup

```clojure
:quick-capture-options
{:insert-today? false
 :redirect-page? true
 :default-page "Research Inbox"}
```

#### Daily Journal Integration

```clojure
:quick-capture-options
{:insert-today? true
 :redirect-page? false
 :default-page ""}
```

#### Content Curation System

```clojure
:quick-capture-options
{:insert-today? false
 :redirect-page? false
 :default-page "Content Queue"}
```

## Template System

### Basic Templates

Customize the capture format using templates:

```clojure
:quick-capture-templates
{:text "**{time}** [[quick capture]]: {text} from {url}"
 :media "**{time}** [[quick capture]]: {url}"}
```

### Template Variables

- **`{time}`**: Current time (format: HH:mm)
- **`{date}`**: Current date in your journal format, use `[[{date}]]` for page reference
- **`{text}`**: Selected or provided text content
- **`{url}`**: URL or formatted link

### Default Template

```
"**{time}** [[quick capture]]: {text} {url}"
```

### Template Examples

#### Academic Research Template

```clojure
:quick-capture-templates
{:text "- **{time}** [[research]] {text}\n  - Source: {url}\n  - Captured: [[{date}]]\n  - #to-review"
 :media "- **{time}** [[research]] Media capture\n  - {url}\n  - Captured: [[{date}]]\n  - #to-review"}
```

#### GTD (Getting Things Done) Template

```clojure
:quick-capture-templates
{:text "- {text}\n  - Source: {url}\n  - Captured: [[{date}]] {time}\n  - Context: \n  - Next Action: \n  - #gtd-inbox"}
```

#### Zettelkasten Template

```clojure
:quick-capture-templates
{:text "# {time}-capture\n\n{text}\n\n**Source:** {url}\n**Date:** [[{date}]]\n\n**Connections:**\n- \n\n**Tags:** #zettel #unprocessed"}
```

#### Structured Content Template

```clojure
:quick-capture-templates
{:text "## {text}\n- **Source:** {url}\n- **Captured:** [[{date}]] at {time}\n- **Status:** #unprocessed\n- **Notes:**\n  - \n\n---"}
```

## Page Selection Logic

Quick capture uses intelligent page selection with this priority order:

1. **Explicit page parameter** (if provided)
2. **Today's journal** (if `insert-today?` is true or page="TODAY")
3. **Configured default page** (from `:default-page` setting)
4. **Current page** (where you are when capturing)
5. **Fallback**: Today's journal (if journals enabled) or "quick capture" page

## Mobile Integration

### Share Intent Support

On mobile devices, quick capture integrates with the system share functionality:

- **Text Sharing**: Captures selected text from other apps
- **URL Sharing**: Captures links with automatic title extraction
- **Media Sharing**: Handles images, videos, and documents
- **Complex Payloads**: Supports mixed content (text + media)

### Mobile-Specific Features

- Automatic file copying and asset management
- URI component decoding for iOS compatibility
- Support for various file formats and MIME types
- Rich content composition from multiple sources

### Mobile Template Examples

#### Photo with Context

```clojure
:quick-capture-templates
{:media "## 📷 Photo Capture\n{url}\n\n**Context:** {text}\n**Location:** \n**Date:** [[{date}]] at {time}\n**Tags:** #photo #visual-note\n\n**Description:**\n- "}
```

#### Voice Memo Template

```clojure
:quick-capture-templates
{:text "## 🎤 Voice Note\n**Transcription:**\n> {text}\n\n**Captured:** {time} on [[{date}]]\n**Audio File:** {url}\n**Status:** #voice-memo #needs-processing\n\n**Key Points:**\n- "}
```

## Practical Examples

### Scenario 1: Mobile Web Browsing

**Situation:** Reading an interesting article on your phone and want to save key quotes.

**Setup:**

```clojure
:quick-capture-templates
{:text "### {url}\n> {text}\n\n**My thoughts:** \n- \n\n*Captured {time} on [[{date}]]*"}
```

**Workflow:**

1. Select important text in mobile browser
2. Tap share → Logseq
3. Text appears formatted as a quote with space for your thoughts

**Result:**

```markdown
### https://example.com/article

> Machine learning is transforming how we approach data analysis

## **My thoughts:**

_Captured 14:30 on [[Dec 15th, 2024]]_
```

### Scenario 2: Meeting Notes Quick Capture

**Situation:** You're in a meeting and need to quickly capture action items.

**Setup:**

```clojure
:quick-capture-options
{:insert-today? true
 :redirect-page? false}

:quick-capture-templates
{:text "- [ ] **{time}** {text} #action-item #meeting"}
```

**Result:**

```markdown
- [ ] **14:30** Follow up with Sarah about budget approval #action-item #meeting
```

### Scenario 3: Social Media Content Curation

**Setup:**

```clojure
:quick-capture-templates
{:text "## Social Media Capture\n{url}\n\n**Key Points:**\n- {text}\n\n**Tags:** #social-media #[[{date}]]\n**Review Date:** \n\n---"}
```

**Result:**

```markdown
## Social Media Capture

{{twitter https://twitter.com/user/status/123}}

**Key Points:**

- Thread about productivity methods and time management

**Tags:** #social-media #[[Dec 15th, 2024]]
**Review Date:**

---
```

### Scenario 4: Reading List Management

**Setup:**

```clojure
:quick-capture-options
{:default-page "Reading List"}

:quick-capture-templates
{:text "- [ ] [[{text}]]\n  - URL: {url}\n  - Added: [[{date}]] at {time}\n  - Priority: \n  - Notes: "}
```

**Result:**

```markdown
- [ ] [[The Future of Remote Work]]
  - URL: https://example.com/remote-work-future
  - Added: [[Dec 15th, 2024]] at 14:30
  - Priority:
  - Notes:
```

## Advanced Usage

### Multi-Purpose Template

```clojure
:quick-capture-templates
{:text "**{time}** [[quick capture]]\n{text}\n\n**Source:** {url}\n**Type:** #[[{date}]]\n**Status:** #inbox\n\n**Follow-up:**\n- [ ] Review\n- [ ] Tag appropriately\n- [ ] Move to proper location\n\n---"}
```

### Workflow Integration

#### Daily Review System

```clojure
:quick-capture-options
{:insert-today? true}

:quick-capture-templates
{:text "### Quick Capture {time}\n{text}\n**Source:** {url}\n\n**Evening Review Questions:**\n- What's the main insight?\n- How does this connect to existing knowledge?\n- What action should I take?\n\n**Status:** #needs-review"}
```

### Content Type Detection

For different content types, you can create specialized workflows:

```clojure
;; Example of content-aware template
:quick-capture-templates
{:text "{% if url contains 'youtube.com' %}\n## 🎥 Video: {text}\n{{video {url}}}\n{% elsif url contains 'twitter.com' %}\n## 🐦 Tweet\n{{twitter {url}}}\n{% else %}\n## 📄 {text}\n{url}\n{% endif %}\n\n**Captured:** {time} on [[{date}]]"}
```

## Troubleshooting

### Common Issues and Solutions

#### Problem: Captures scattered across different pages

**Solution:** Use consistent default page

```clojure
:quick-capture-options
{:insert-today? false
 :default-page "Capture Inbox"}
```

#### Problem: Hard to find captured content

**Solution:** Add consistent tagging

```clojure
:quick-capture-templates
{:text "{text} {url}\n\n**Captured:** [[{date}]] {time} #captured #needs-review"}
```

#### Problem: Templates too complex for mobile

**Solution:** Create minimal mobile template

```clojure
:quick-capture-templates
{:text "{time}: {text} {url} #quick"}
```

#### Problem: Quick capture not working on mobile

**Potential causes:**

- App permissions not granted
- Share intent not properly configured
- Template syntax errors

**Solutions:**

- Check mobile app permissions
- Verify config.edn syntax
- Test with simple template first

### Best Practices

#### Template Design

- Include timestamps for chronological organization
- Use consistent tagging (like `[[quick capture]]`) for easy retrieval
- Consider your workflow when choosing between journal vs. dedicated pages
- Keep mobile templates simple and fast

#### Page Organization

- Use `insert-today?` for time-sensitive captures
- Create dedicated capture pages for different content types
- Set up a review workflow for processing captured content
- Consider using a staged approach: capture → review → organize

#### Mobile Workflow

- Configure templates that work well with mobile sharing
- Test capture behavior with different apps and content types
- Consider using shorter templates for mobile efficiency
- Set up consistent review periods for mobile captures

## Technical Details

### Implementation Location

Quick capture is implemented in:

- `src/main/frontend/quick_capture.cljs` - Core functionality
- `src/main/frontend/mobile/intent.cljs` - Mobile integration
- `src/main/frontend/schema/handler/common_config.cljc` - Configuration schema

### Smart Insertion Logic

The system handles different insertion contexts:

- **Active Editing**: Inserts into current block or creates new line
- **Page Navigation**: Creates new block on target page
- **Empty Content**: Replaces empty blocks when appropriate

### Error Handling

- Graceful fallbacks when pages don't exist
- Automatic page creation when needed
- Notification system for capture failures

### Integration Points

Quick capture integrates with:

- **Journal system**: For time-based organization
- **Page system**: For topic-based organization
- **Editor**: For seamless content insertion
- **Mobile OS**: For system-level sharing
- **File system**: For asset management

### Configuration Schema

The configuration follows this schema:

```clojure
:quick-capture-options (optional-keys
  [:map
   [:insert-today? :boolean]
   [:redirect-page? :boolean]
   [:default-page :string]])

:quick-capture-templates (optional-keys
  [:map
   [:text :string]
   [:media :string]])
```

## Getting Started

### Basic Setup

1. **Choose your approach**: Decide whether you want captures in journals or dedicated pages
2. **Configure options**: Set up `:quick-capture-options` in your `config.edn`
3. **Create templates**: Start with simple templates and evolve them based on usage
4. **Test the workflow**: Try capturing different content types to verify setup

### Recommended First Configuration

```clojure
;; Start with this simple setup
:quick-capture-options
{:insert-today? true
 :redirect-page? false
 :default-page "Quick Capture"}

:quick-capture-templates
{:text "- **{time}** {text} {url} #captured"
 :media "- **{time}** {url} #captured"}
```

### Evolution Path

1. **Week 1**: Use basic setup and observe patterns
2. **Week 2**: Add tags and improve templates based on usage
3. **Week 3**: Implement review workflow and organize captured content
4. **Week 4**: Optimize for your specific use cases and integrate with existing workflows

This comprehensive system makes quick capture a powerful tool for maintaining your knowledge base without interrupting your flow, whether you're on desktop or mobile.
