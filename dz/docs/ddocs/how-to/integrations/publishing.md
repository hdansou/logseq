---
title: 'Publishing Your Graph'
status: 🟢
since-version: 'N/A'
quadrant: 'How-To'
last-verified: '2026-03-10'
platform: 'All'
related-docs:
  - 'reference/configuration/config-edn.md'
  - 'how-to/integrations/cli-basic.md'
---

# Publishing Your Graph

> **Last revised:** 2026-03-10

Learn how to publish your Logseq graph to the web, making selected pages publicly accessible.

## Overview

Logseq's publishing feature allows you to:

- Convert your private notes into a public website
- Control which pages are published
- Maintain links between published pages
- Host on any static site hosting service
- Keep the Logseq reading experience for visitors

The published site is a single-page application (SPA) that embeds the graph database into a single `index.html` file, providing the same navigation experience as the desktop app.

## Prerequisites

- Logseq database graph (recommended) or file-based graph
- Pages you want to publish
- Basic understanding of static site hosting (optional)

## Step 1: Mark Pages for Publishing

### Using the Property

In a DB graph, set the built-in `publishing-public?` property to `true` on pages you want to publish. You can do this from the page's property editor in the UI.

In a file-based graph, add the property in markdown:

```markdown
- # My Public Page

  public:: true

  - This content will be published
  - Links to other [[Public Pages]] work
  - Links to private pages are removed
```

### All Pages Public

Instead of marking individual pages, you can make all pages public at once:

1. Open **Settings** (`Ctrl/Cmd + ,`)
2. Navigate to the **Editor** section
3. Toggle **"All pages public"** to enable

Or set it directly in `config.edn`:

```clojure
:publishing/all-pages-public? true
```

When this is enabled, all pages are published except those explicitly marked with `publishing-public?` set to `false`.

### Publishing Behavior

When `:publishing/all-pages-public?` is `false` (the default), only pages explicitly marked with `publishing-public?` set to `true` are included. When it is `true`, all pages are included except those explicitly marked `false`.

## Step 2: Export for Publishing

### Using the Desktop App (Recommended)

1. Go to the **Export** page (from the three-dot menu or `...` > **Export**)
2. Click **"Export public pages"**
3. Choose an output folder (Electron desktop app only)
4. The export creates a static site in the chosen folder

> **Note:** The "Export public pages" option is only available in the Electron desktop app, not in the browser version.

### Using the Publishing Script

The `deps/publishing` module provides a script for publishing from the command line. This requires building from source:

```bash
# From the logseq repository root
cd deps/publishing

# Run the publishing script (requires nbb)
# Usage: STATIC-DIR GRAPH-DIR OUT-DIR
npx nbb-logseq script/publishing.cljs <static-dir> <graph-dir> <output-dir>
```

The `static-dir` is the directory containing the built Logseq static assets (CSS, JS, fonts, etc.). The `graph-dir` is the path to your graph's database directory.

### Using the CLI

The Logseq CLI (`deps/cli`) supports exporting a graph as Markdown (not as a publishing site). To export as Markdown:

```bash
logseq export -g my-graph -f output.zip
```

This produces a zip of Markdown files, which is different from the HTML publishing export.

### What Gets Exported

The publishing export creates a single-page application:

```
output/
├── index.html              # SPA with embedded database
├── 404.html                # Redirect page for SPA routing
├── assets/
│   └── (graph assets)
└── static/
    ├── css/
    │   ├── style.css
    │   ├── custom.css
    │   └── export.css
    ├── fonts/
    ├── icons/
    ├── img/
    └── js/
        ├── main.js
        ├── code-editor.js
        ├── react.production.min.js
        ├── react-dom.production.min.js
        └── (other JS dependencies)
```

The entire graph database is serialized as transit data and embedded in `index.html`. The published site behaves as a single-page app with client-side routing.

## Step 3: Configure Published Content

### Homepage

Set which page loads on startup using `:default-home` in `config.edn`:

```clojure
;; config.edn
:default-home {:page "Welcome"}
```

You can also configure a sidebar page:

```clojure
:default-home {:page "Welcome", :sidebar "Contents"}
```

### Custom Styling

The publishing export automatically picks up custom CSS and JS from your graph's `logseq/` directory:

- `logseq/custom.css` - Custom CSS styles
- `logseq/custom.js` - Custom JavaScript
- `logseq/export.css` - Export-specific CSS overrides

These files are copied into the published output automatically. No config.edn keys are needed.

Example `logseq/custom.css`:

```css
/* custom.css */
:root {
  --primary-color: #3b82f6;
  --background-color: #ffffff;
}

.page-content {
  max-width: 800px;
  margin: 0 auto;
}
```

### Theme and Accent Color

When using the publishing script, you can specify theme and accent color programmatically:

```clojure
;; Options passed to the publishing/export function
{:ui/theme "dark"          ;; or "light"
 :ui/radix-color :cyan}    ;; accent color
```

### Page Metadata

Add metadata properties to pages for organization:

```markdown
- # Blog Post Title
  publishing-public?:: true
  description:: A great article about knowledge management
  tags:: #blog, #productivity
```

## Step 4: Deploy Your Site

### Option 1: GitHub Pages

1. **Create repository:**

```bash
gh repo create my-logseq-site --public
```

2. **Push exported site:**

```bash
cd public
git init
git add .
git commit -m "Initial publish"
git branch -M main
git remote add origin https://github.com/username/my-logseq-site.git
git push -u origin main
```

3. **Enable GitHub Pages:**

   - Go to repository **Settings > Pages**
   - Source: Deploy from branch `main`
   - Folder: `/ (root)`
   - Click **Save**

4. **Access your site:**
   - `https://username.github.io/my-logseq-site`

> **Note:** The published site includes a SPA redirect script compatible with GitHub Pages. The `404.html` file handles client-side routing.

### Option 2: Netlify

1. **Install Netlify CLI:**

```bash
npm install -g netlify-cli
```

2. **Deploy:**

```bash
cd public
netlify deploy --prod
```

3. **Follow prompts:**
   - Authorize Netlify
   - Choose site name
   - Set publish directory: `.`

### Option 3: Vercel

1. **Install Vercel CLI:**

```bash
npm install -g vercel
```

2. **Deploy:**

```bash
cd public
vercel --prod
```

### Option 4: Custom Server

Upload the output folder to any web server:

```bash
# Via SCP
scp -r public/* user@server:/var/www/html/

# Via FTP
# Use FileZilla or similar tool
```

## Publishing Workflow

### Regular Updates

Automate with a script (using the desktop app export):

```bash
#!/bin/bash
# publish.sh

# After exporting from the desktop app to ./public:

# Deploy to GitHub Pages
cd public
git add .
git commit -m "Update: $(date +%Y-%m-%d)"
git push

cd ..
echo "Published successfully!"
```

Make executable:

```bash
chmod +x publish.sh
./publish.sh
```

### Custom Domain

**GitHub Pages:**

1. Add `CNAME` file to published site:

```
www.yourdomain.com
```

2. Configure DNS:

```
Type: CNAME
Name: www
Value: username.github.io
```

**Netlify/Vercel:**

- Follow their custom domain setup guides
- Usually automatic DNS configuration

## Troubleshooting

### Links Not Working

**Problem:** Internal links broken after publishing

**Solution:**

- Ensure linked pages have `publishing-public?` set to `true`
- Private page links are automatically removed during export
- Check for typos in page names

### Images Not Showing

**Problem:** Images don't load on published site

**Solution:**

- Assets referenced by published pages are automatically included in the export
- Verify the assets exist in your graph's assets directory
- Check that the asset file names match what the pages reference

### Page Not Updated

**Problem:** Changes don't appear on published site

**Solution:**

1. Re-export the graph from the desktop app
2. Clear browser cache
3. Check deployment completed successfully

### Large Graph Export Fails

**Problem:** Export times out or crashes

**Solution:**

- Publish fewer pages initially (set specific pages to public instead of all)
- Increase system memory
- Use the desktop app's Electron version for better performance

## Best Practices

### 1. Organize Public Content

Create a clear structure:

```markdown
- # Public Home

  publishing-public?:: true

  - ## Published Sections
    - [[Blog]] - Articles and tutorials
    - [[Projects]] - Portfolio items
    - [[Resources]] - Shared knowledge
```

### 2. Review Before Publishing

Always preview locally:

```bash
# After exporting to a local folder
cd preview
python -m http.server 8000

# Open browser: http://localhost:8000
```

### 3. Separate Public/Private

Use namespaces:

```markdown
- [[Public/Blog Post 1]]
- [[Public/About Me]]
- [[Private/Personal Notes]]
```

### 4. Use Templates

Create publishing template:

```markdown
- # Publishing Template
  template:: public-page
  publishing-public?:: true
  description::
  tags::
```

Apply to new pages:

1. Type `/template`
2. Select `public-page`

## Security Considerations

### What Not to Publish

Never publish:

- Personal information (passwords, API keys)
- Private meeting notes
- Financial information
- Unpublished work (respect copyright)
- Others' private information

### Content Review

Before publishing:

1. Search for sensitive keywords
2. Review all marked pages
3. Check linked pages are intended to be public
4. Verify no private block references

### Access Control

Published sites are **publicly accessible**:

- No built-in authentication
- Anyone with URL can access
- Search engines will index
- Use private hosting for sensitive content

## Examples

### Personal Blog

```markdown
- # Blog Home

  publishing-public?:: true

  - Recent Posts:
    - [[Post: Getting Started with Logseq]]
    - [[Post: Building a Second Brain]]
    - [[Post: PKM Best Practices]]
```

### Documentation Site

```markdown
- # API Documentation

  publishing-public?:: true

  - ## Getting Started

    - [[Installation]]
    - [[Quick Start]]
    - [[Authentication]]

  - ## API Reference
    - [[Users API]]
    - [[Posts API]]
    - [[Comments API]]
```

### Portfolio

```markdown
- # Portfolio

  publishing-public?:: true
  description:: My work and projects

  - ## Projects
    - [[Project: E-commerce Platform]]
      tech:: React, Node.js, PostgreSQL
      url:: https://example.com
    - [[Project: Mobile App]]
      tech:: React Native
```

## See Also

- [CLI Basic Usage](cli-basic.md) - Using Logseq from command line
- [Configuration Reference](../../reference/configuration/config-edn.md) - All config options
- [Export Guide](../../how-to/backup/backup-restore.md) - Other export formats
