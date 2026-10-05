# Logseq Theme Development Documentation

> **Last revised:** 2026-09-29 (activation selectors only)

> **Correction (2026-09-29):** colour variables set on `:root` or on `html[data-theme='…']` alone lose to Logseq's own `html[data-theme][data-color=logseq]` defaults and to the per-accent declarations on `body`, so the snippets on this page now use `html[data-theme='…'][data-color], html[data-theme='…'][data-color] body`. Core also reads `--lx-gray-*`/`--lx-accent-*` before `--ls-*`, and shui tokens (`--primary`, `--background`, …) must be bare HSL channels. Verified live on Logseq 2.0.1; see the [DB theme walkthrough](./db-theme-walkthrough.md).

Welcome to the Logseq Theme Development documentation! This collection of guides will help you create beautiful, custom themes for Logseq.

## 📚 Documentation Overview

### 0. [Create a Logseq DB theme, end to end](./db-theme-walkthrough.md) - **START HERE (verified 2026-09-29)**

The tested process: tokens → variable layers → activation selector → source-verified selectors → live validation → human testing graph → release.

### 1. [Theme Development Guide](./guide.md)

**Complete comprehensive guide covering:**

- Understanding Logseq's two modes (File-based & Database)
- Design system overview (Shui components, Radix color tokens, Tailwind utility layers)
- Theme architecture and file structure
- CSS variables reference
- Component styling guide for core UI surfaces and components
- Best practices for accessibility and performance
- Testing checklist and debugging tips
- Publishing and distribution workflows

**Who should read this:** Anyone creating their first Logseq theme or wanting deep understanding of the theming system.

**Estimated reading time:** 45-60 minutes

---

### 2. [Quick Reference Guide](./quick-reference.md) - **FOR QUICK LOOKUPS**

**One-page reference with:**

- Essential CSS variables organized by category
- Common selectors for blocks, UI components, layout
- Theme mode detection patterns
- Quick start template
- Available theme-relevant component hooks and selectors
- Debugging tips and common patterns
- Common issues and solutions table

**Who should read this:** Developers who've read the main guide and need quick lookups while coding.

**Estimated reading time:** 10 minutes browse, 2 minutes lookup

---

### 3. [Example Theme: Ocean Breeze](./example-ocean-breeze.md) - **LEARN BY EXAMPLE**

**Complete working theme including:**

- Full CSS implementation
- Ocean-inspired color palette
- Light and dark mode variants
- Custom typography with web fonts
- Animated components
- Mobile optimizations
- package.json for plugin distribution
- Customization tips

**Who should read this:** Developers who learn best by examining real code and want a production-ready starting point.

**Estimated reading time:** 20 minutes

---

## 🚀 Getting Started

### For Complete Beginners

1. **Read:** [Theme Development Guide](./guide.md) (Section 1-5)
2. **Study:** [Example Theme: Ocean Breeze](./example-ocean-breeze.md)
3. **Experiment:** Copy the example and modify colors
4. **Reference:** Use [Quick Reference](./quick-reference.md) while coding

### For Experienced CSS Developers

1. **Skim:** [Theme Development Guide](./guide.md) (Focus on "Design System Overview" and "CSS Variables Reference")
2. **Review:** [Quick Reference](./quick-reference.md)
3. **Start coding:** Use the basic template from Quick Start
4. **Reference:** Check example theme for specific implementations

### For Theme Distribution

1. **Complete your theme** using the guides above
2. **Test thoroughly** using checklist in main guide
3. **Follow publishing guidelines** in [Theme Development Guide - Publishing](./guide.md#publishing-your-theme)
4. **Submit to Marketplace** or share with community

---

## 🎯 Key Concepts

### Two Graph Modes

Logseq operates in two modes:

- **File-based mode**: Traditional markdown files
- **Database mode**: SQLite database storage

**Good news:** Theme authoring is effectively the same in both modes.

> **Removed:** The older wording implied identical storage/runtime internals across modes. Current source keeps the authoring model aligned, but storage and retrieval paths differ between File-based and DB graphs.

### Design System Stack

```text
Your Theme CSS
      ↓
Shui Components (Logseq UI layer)
      ↓
Token and utility layers
      ↓
Radix Colors
      ↓
TailwindCSS
```

> **Removed:** The previous version described a strict `shadcn/ui -> Radix Primitives -> TailwindCSS` runtime stack. Current source uses `logseq.shui` wrappers plus tokenized variables and utility layers, so this should be treated as a conceptual chain.

### CSS Variable Hierarchy

```text
--lx-* (Logseq Extended) ← Customize these
   ↓
--ls-* (Legacy) ← For backward compatibility
   ↓
--rx-* (Radix Base) ← Don't modify directly
```

## Findings: New Capabilities

- Published graph theming supports both custom `logseq/publish.css` and custom `logseq/publish.js` assets in the publishing pipeline.
- Published pages include built-in theme initialization and toggling via `data-theme`, persisted in local storage (`publish-theme`) with system-preference fallback.

---

## 📦 What You'll Build

By following these guides, you'll be able to create:

✅ **Custom color schemes** for light and dark modes  
✅ **Typography customization** with web fonts  
✅ **Component styling** (buttons, cards, dialogs, etc.)  
✅ **Animations and transitions**  
✅ **Mobile-responsive themes**  
✅ **Plugin-distributable themes**  
✅ **Accessible themes** meeting WCAG standards

---

## 🛠️ Technical Requirements

### Knowledge Prerequisites

- **Required:** CSS fundamentals (selectors, properties, values)
- **Recommended:** CSS variables, pseudo-classes
- **Optional:** CSS animations, media queries

### Tools

- **Text editor** (VS Code, Sublime, etc.)
- **Logseq** (latest version recommended)
- **Browser DevTools** (for debugging)
- **Git** (for version control and distribution)

### No Experience Needed With

- ClojureScript
- React
- Shadow-CLJS
- Build tools

**Themes are CSS-only!** No programming required.

---

## 📖 Documentation Structure

### Main Guide Sections

1. **Introduction** - Overview and setup
2. **Understanding Logseq Modes** - File-based vs Database
3. **Design System Overview** - Shui, token layers, Radix, Tailwind
4. **Theme Architecture** - File structure and organization
5. **Quick Start Guide** - Get coding in 5 minutes
6. **CSS Variables Reference** - Complete variable documentation
7. **Component Styling** - core UI component styling guide
8. **Best Practices** - Accessibility, performance, patterns
9. **Testing Your Theme** - Checklist and debugging
10. **Publishing Your Theme** - Distribution workflows
11. **Advanced Topics** - Animations, fonts, effects
12. **Resources** - Links and community
13. **FAQ** - Common questions answered

---

## 🎨 Theme Customization Levels

### Level 1: Color Tweaking (Easiest)

Modify CSS variables for colors:

```css
html[data-theme='light'][data-color],
html[data-theme='light'][data-color] body {
  --lx-accent-09: #your-color;
}
```

**Time investment:** 15 minutes  
**Result:** Personalized color scheme

### Level 2: Component Styling (Intermediate)

Custom styles for UI components:

```css
.ui__button {
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}
```

**Time investment:** 2-4 hours  
**Result:** Unique component appearance

### Level 3: Complete Theme (Advanced)

Full theme with typography, animations, custom layouts:

```css
/* 500+ lines of custom CSS */
```

**Time investment:** 1-2 days  
**Result:** Production-ready distributable theme

---

## 🤝 Community & Support

### Get Help

- **Discord:** [#themes channel](https://discord.gg/logseq)
- **Forum:** [discuss.logseq.com](https://discuss.logseq.com/)
- **GitHub:** [Discussions](https://github.com/logseq/logseq/discussions)

### Share Your Theme

- **Marketplace:** Submit via PR
- **Discord:** Share in #themes
- **Reddit:** r/logseq
- **Twitter:** #Logseq hashtag

### Contribute

Found issues with these docs? Suggestions for improvement?

- [Open an issue](https://github.com/logseq/logseq/issues)
- Submit a PR with improvements
- Share feedback in Discord

---

## 🔄 Version History

### v1.1.0 (May 2026)

- Updated terminology to match current implementation (`logseq.shui` and token-layer model)
- Added validated note on File-based vs DB mode internals
- Added published-theme capability findings for custom `publish.css` and `publish.js`

### v1.0.0 (October 2025)

- Initial comprehensive theme development documentation
- Covers File-based and Database modes
- Documents shadcn/ui integration
- Complete CSS variable reference
- Example theme included
- Quick reference guide

---

## 📋 Quick Links

**Documentation:**

- [Full Development Guide](./guide.md)
- [Quick Reference](./quick-reference.md)
- [Example Theme](./example-ocean-breeze.md)

**External Resources:**

- [Logseq Official Docs](https://docs.logseq.com/)
- [shadcn/ui Documentation](https://ui.shadcn.com/)
- [Radix Colors](https://www.radix-ui.com/colors)
- [TailwindCSS Docs](https://tailwindcss.com/)

**Tools:**

- [Radix Colors Palette](https://www.radix-ui.com/colors)
- [Contrast Checker](https://webaim.org/resources/contrastchecker/)
- [Coolors Color Generator](https://coolors.co/)

**Community:**

- [Logseq Discord](https://discord.gg/logseq)
- [Logseq Forum](https://discuss.logseq.com/)
- [Awesome Logseq](https://github.com/logseq/awesome-logseq)

---

## 🎯 Next Steps

1. **Choose your starting point** from the guides above
2. **Set up your development environment** (text editor + Logseq)
3. **Create your first theme** using the Quick Start template
4. **Test and iterate** using the testing checklist
5. **Share with the community** when ready!

---

## Happy theming! 🎨

We're excited to see what beautiful themes you'll create for Logseq!
