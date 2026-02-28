---
title: 'Publishing Your Graph'
status: 🟢
since-version: 'N/A'
quadrant: 'How-To'
last-verified: '2026-01-11'
platform: 'All'
related-docs:
  - 'reference/configuration/config-edn.md'
  - 'how-to/integrations/cli-basic.md'
---

# Publishing Your Graph

Learn how to publish your Logseq graph to the web, making selected pages publicly accessible.

## Overview

Logseq's publishing feature allows you to:

- Convert your private notes into a public website
- Control which pages are published
- Maintain links between published pages
- Host on any static site hosting service
- Keep the Logseq reading experience for visitors

## Prerequisites

- Logseq database graph (recommended) or file-based graph
- Pages you want to publish
- Basic understanding of static site hosting (optional)

## Step 1: Mark Pages for Publishing

### Using the Property

Add the `publishing-public?` property to pages you want to publish:

```markdown
- # My Public Page

  publishing-public?:: true

  - This content will be published
  - Links to other [[Public Pages]] work
  - Links to private pages are removed
```

### Bulk Selection

Mark multiple pages at once:

1. Open **Settings** (`Ctrl/Cmd + ,`)
2. Navigate to **Publishing** section
3. Click **"Select pages to publish"**
4. Check the pages you want public
5. Click **"Save"**

### Publishing Settings

Configure publishing behavior in `config.edn`:

```clojure
:publishing/all-pages-public? false

:publishing/pages-whitelist []

:exclude-from-graph-view [
  "Private Page 1"
  "Private Page 2"
]
```

## Step 2: Export for Publishing

### Using the CLI (Recommended)

The Logseq CLI provides the best export experience:

```bash
# Export entire graph for publishing
logseq export my-graph --format publish --output ./public
```

**Options:**

```bash
# Specify custom output directory
logseq export my-graph --format publish --output /path/to/site

# Include assets (images, PDFs)
logseq export my-graph --format publish --with-assets

# Preview mode (doesn't write files)
logseq export my-graph --format publish --dry-run
```

### Using Desktop App Export

1. Go to **Settings → Export**
2. Select **"Publish to Web"**
3. Choose output folder
4. Click **"Export"**

### What Gets Exported

The export creates a static site with:

```
output/
├── index.html              # Homepage
├── pages/
│   ├── page1.html
│   ├── page2.html
│   └── ...
├── assets/
│   ├── images/
│   ├── css/
│   └── js/
└── static/
    └── (static resources)
```

## Step 3: Configure Published Content

### Homepage

Set which page becomes your homepage:

```clojure
;; config.edn
:publishing/home-page "Welcome"
```

### Navigation

Create a navigation page:

```markdown
- # Navigation

  publishing-public?:: true

  - [[Home]]
  - [[About]]
  - [[Blog]]
  - [[Contact]]
```

Reference it in config:

```clojure
:publishing/nav-pages ["Navigation"]
```

### Page Metadata

Add SEO-friendly metadata:

```markdown
- # Blog Post Title
  publishing-public?:: true
  description:: A great article about knowledge management
  tags:: #blog, #productivity
  date:: [[2026-01-11]]
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

   - Go to repository **Settings → Pages**
   - Source: Deploy from branch `main`
   - Folder: `/ (root)`
   - Click **Save**

4. **Access your site:**
   - `https://username.github.io/my-logseq-site`

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

Upload the `public/` folder to any web server:

```bash
# Via SCP
scp -r public/* user@server:/var/www/html/

# Via FTP
# Use FileZilla or similar tool
```

## Publishing Workflow

### Regular Updates

Automate with a script:

```bash
#!/bin/bash
# publish.sh

# Export latest changes
logseq export my-graph --format publish --output ./public

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

### Scheduled Publishing

Use GitHub Actions for automatic updates:

```yaml
# .github/workflows/publish.yml
name: Publish Logseq

on:
  schedule:
    - cron: '0 0 * * *' # Daily at midnight
  workflow_dispatch: # Manual trigger

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18'

      - name: Install Logseq CLI
        run: npm install -g @logseq/cli

      - name: Export graph
        run: logseq export my-graph --format publish --output ./public

      - name: Deploy to GitHub Pages
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./public
```

## Advanced Configuration

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

### Custom Theme

Override default styles:

```clojure
;; config.edn
:publishing/custom-css "custom.css"
:publishing/custom-js "custom.js"
```

Create `custom.css`:

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

### Search Functionality

Published sites include built-in search:

```clojure
;; config.edn
:publishing/enable-search? true
```

Visitors can search published content with `Ctrl/Cmd + K`.

### Analytics

Add Google Analytics:

```clojure
;; config.edn
:publishing/google-analytics "G-XXXXXXXXXX"
```

Or Plausible:

```clojure
:publishing/plausible-domain "yourdomain.com"
```

## Troubleshooting

### Links Not Working

**Problem:** Internal links broken after publishing

**Solution:**

- Ensure linked pages have `publishing-public?:: true`
- Private page links are automatically removed
- Check for typos in page names

### Images Not Showing

**Problem:** Images don't load on published site

**Solution:**

```bash
# Include assets in export
logseq export my-graph --format publish --with-assets
```

### Page Not Updated

**Problem:** Changes don't appear on published site

**Solution:**

1. Re-export the graph
2. Clear browser cache
3. Check deployment completed successfully

### Large Graph Export Fails

**Problem:** Export times out or crashes

**Solution:**

- Publish fewer pages initially
- Exclude large assets
- Use CLI instead of desktop app
- Increase system memory

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

Always preview:

```bash
# Export to local folder
logseq export my-graph --format publish --output ./preview

# Start local server
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
  date:: [[<%today%>]]
```

Apply to new pages:

1. Type `/template`
2. Select `public-page`

## Security Considerations

### What Not to Publish

❌ Never publish:

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
