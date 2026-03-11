# Logseq Theme Development Documentation

> **Last revised:** 2026-03-10

Welcome to the Logseq Theme Development documentation! This collection of guides will help you create beautiful, custom themes for Logseq.

## 📚 Documentation Overview

### 1. [Theme Development Guide](./guide.md) - **START HERE**

**Complete comprehensive guide covering:**

- Understanding Logseq's two modes (File-based & Database)
- Design system overview (shadcn/ui, Radix Colors, TailwindCSS)
- Theme architecture and file structure
- CSS variables reference (150+ variables documented)
- Component styling guide for all shadcn/ui components
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
- Available shadcn/ui components list
- Debugging tips and common patterns
- Common issues and solutions table

**Who should read this:** Developers who've read the main guide and need quick lookups while coding.

**Estimated reading time:** 10 minutes browse, 2 minutes lookup

---

### 3. [Example Theme: Ocean Breeze](./example-ocean-breeze.md) - **LEARN BY EXAMPLE**

**Complete working theme including:**

- Full CSS implementation (500+ lines)
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

**Good news:** Themes work identically in both modes! No special handling needed.

### Design System Stack

```text
Your Theme CSS
      ↓
shadcn/ui Components
      ↓
Radix UI Primitives
      ↓
Radix Colors
      ↓
TailwindCSS
```

### CSS Variable Hierarchy

```text
--lx-* (Logseq Extended) ← Customize these
   ↓
--ls-* (Legacy) ← For backward compatibility
   ↓
--rx-* (Radix Base) ← Don't modify directly
```

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
3. **Design System Overview** - shadcn/ui, Radix, Tailwind
4. **Theme Architecture** - File structure and organization
5. **Quick Start Guide** - Get coding in 5 minutes
6. **CSS Variables Reference** - Complete variable documentation
7. **Component Styling** - shadcn/ui components guide
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
html[data-theme='light'] {
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
