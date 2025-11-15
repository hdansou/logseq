# Logseq Theme Development Quick Reference

**Companion to:** [Theme Development Guide](./theme-development-guide.md)

## Essential CSS Variables

### Backgrounds

```css
--ls-primary-background-color    /* Main canvas */
--ls-secondary-background-color  /* Sidebars */
--ls-tertiary-background-color   /* Hover */
--ls-quaternary-background-color /* Active */
```

### Text Colors

```css
--ls-primary-text-color          /* Main text */
--ls-secondary-text-color        /* Muted text */
--ls-title-text-color            /* Headings */
--ls-link-text-color             /* Links */
--ls-link-text-hover-color       /* Link hover */
```

### Borders

```css
--ls-border-color                /* Default borders */
--ls-secondary-border-color      /* Subtle borders */
```

### Accent Colors

```css
--lx-accent-01 through --lx-accent-12  /* Full scale */
--lx-accent-09                         /* Primary accent */
```

### Gray Scale

```css
--lx-gray-01 through --lx-gray-12     /* Light to dark */
```

## Common Selectors

### Blocks

```css
.ls-block                   /* Block container */
/* Block container */
.ls-block .bullet          /* Bullet point */
.block-content             /* Block content */
.block-children            /* Nested blocks */
.block-ref; /* Block reference */
```

### UI Components

```css
.ui__button                /* shadcn button */
/* shadcn button */
.ui__card                  /* shadcn card */
.ui__dialog-content        /* shadcn dialog */
.ui__dropdown-menu-content; /* shadcn dropdown */
```

### Layout

```css
.cp__header                /* Top header */
/* Top header */
.cp__sidebar-left-layout   /* Left sidebar */
.cp__right-sidebar         /* Right sidebar */
.editor-wrapper; /* Editor container */
```

### Content

```css
h1, h2, h3                 /* Headings */
a                          /* Links */
.tag                       /* Tags */
code                       /* Inline code */
.extensions__code          /* Code blocks */
blockquote                 /* Quotes */
table                      /* Tables */
```

## Theme Modes

```css
/* Light mode */
html[data-theme='light'] {
  /* Variables */
}

/* Dark mode */
html[data-theme='dark'] {
  /* Variables */
}

/* Accent colors */
html[data-color='logseq'] {
}
html[data-color='blue'] {
}
html[data-color='red'] {
}
/* ...etc */
```

## Graph Mode Detection

```css
/* File-based mode */
html.is-file-graph {
}

/* Database mode */
html:not(.is-file-graph) {
}
```

## Quick Start Template

```css
/* Minimal theme template */
html[data-theme='light'] {
  --lx-gray-01: #ffffff;
  --lx-gray-12: #000000;
  --lx-accent-09: #0066cc;
}

html[data-theme='dark'] {
  --lx-gray-01: #1a1a1a;
  --lx-gray-12: #eeeeee;
  --lx-accent-09: #3b82f6;
}
```

## Available shadcn/ui Components

```text
alert-dialog    dropdown-menu   select
alert           form            separator
avatar          input           skeleton
badge           label           slider
button          link            switch
button-group    multi-select    tabs
calendar        popover         textarea
card            radio-group     toast
checkbox        scroll-area     toggle
command         search-input    toggle-group
context-menu    segmented       tooltip
dialog          segmented-control
```

## Debugging Tips

```css
/* Visual debugging (temporary) */
* {
  outline: 1px solid red !important;
}

.ls-block {
  outline: 1px solid blue !important;
}

/* Check computed values */
/* Use browser DevTools > Elements > Computed */
```

## File Locations

- **Custom CSS**: `<graph-root>/logseq/custom.css`
- **Plugin themes**: `<graph-root>/.logseq/plugins/<theme-name>/`
- **System themes**: Built into Logseq

## Testing Checklist

- [ ] Light mode
- [ ] Dark mode
- [ ] All accent colors
- [ ] Text readability
- [ ] Links visible
- [ ] Borders visible
- [ ] Focus states
- [ ] Hover states
- [ ] File-based mode
- [ ] Database mode
- [ ] Desktop app
- [ ] Mobile app

## Common Patterns

### Custom Font

```css
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');

:root {
  --ls-font-family: 'Inter', sans-serif;
}

.ls-block {
  font-family: var(--ls-font-family);
}
```

### Custom Focus Ring

```css
*:focus-visible {
  outline: 2px solid var(--lx-accent-09);
  outline-offset: 2px;
  border-radius: 4px;
}
```

### Smooth Transitions

```css
* {
  transition: background-color 0.2s, color 0.2s, border-color 0.2s;
}

@media (prefers-reduced-motion: reduce) {
  * {
    transition: none !important;
  }
}
```

### Custom Scrollbar

```css
.visible-scrollbar::-webkit-scrollbar {
  width: 8px;
}

.visible-scrollbar::-webkit-scrollbar-track {
  background: var(--ls-scrollbar-background-color);
}

.visible-scrollbar::-webkit-scrollbar-thumb {
  background: var(--ls-scrollbar-foreground-color);
  border-radius: 4px;
}
```

## Resources

- **Full Guide**: [theme-development-guide.md](./theme-development-guide.md)
- **Discord**: [#themes channel](https://discord.gg/logseq)
- **Forum**: [discuss.logseq.com](https://discuss.logseq.com/)
- **Radix Colors**: [radix-ui.com/colors](https://www.radix-ui.com/colors)
- **shadcn/ui**: [ui.shadcn.com](https://ui.shadcn.com/)

## Quick Commands

```bash
# Create theme directory
mkdir my-logseq-theme && cd my-logseq-theme

# Initialize package.json
npm init -y

# Open in editor
code .
```

## Common Issues

| Issue              | Solution                              |
| ------------------ | ------------------------------------- |
| Dark mode too dark | Increase `--lx-gray-01` value         |
| Links invisible    | Check `--ls-link-text-color` contrast |
| Bullets missing    | Set `--ls-block-bullet-color`         |
| Focus ring missing | Add `:focus-visible` styles           |
| Theme not loading  | Check file location & syntax errors   |

## Example Customizations

### Colorful Bullets

```css
.ls-block .bullet {
  background: linear-gradient(45deg, #ff6b6b, #4ecdc4);
}
```

### Rounded Cards

```css
.ui__card {
  border-radius: 16px;
}
```

### Custom Block Reference Style

```css
.block-ref {
  border-bottom: 2px dashed var(--lx-accent-09);
  background: var(--lx-accent-02);
  padding: 2px 4px;
  border-radius: 4px;
}
```

### Elegant Headings

```css
h1,
h2,
h3 {
  font-weight: 700;
  letter-spacing: -0.02em;
  background: linear-gradient(90deg, var(--lx-accent-11), var(--lx-accent-09));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
```

## Color Scale Reference

Radix provides 12-step scales. Typical usage:

- **Steps 1-2**: Backgrounds
- **Steps 3-5**: Component backgrounds
- **Steps 6-8**: Borders & separators
- **Steps 9-11**: Interactive elements & text
- **Step 12**: High contrast text

## Package.json Template

```json
{
  "name": "logseq-theme-mytheme",
  "version": "1.0.0",
  "description": "My awesome theme",
  "author": "Your Name",
  "license": "MIT",
  "logseq": {
    "id": "mytheme",
    "title": "My Theme",
    "icon": "./icon.png",
    "themes": [
      {
        "name": "My Theme",
        "url": "./custom.css",
        "description": "Beautiful theme for Logseq",
        "mode": "light"
      }
    ]
  }
}
```

---

**For complete documentation, see [theme-development-guide.md](./theme-development-guide.md)**
