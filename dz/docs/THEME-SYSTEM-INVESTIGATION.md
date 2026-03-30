# Investigation: Logseq DB Theme System

**Date:** 2025-03-29  
**Branch:** `test/db`  
**Scope:** Theme modes, accent colors, fonts, custom CSS, and plugin theming

---

## 1. Executive Summary

- **Three-layer theming:** Logseq DB's appearance is controlled by (1) theme mode (light/dark/system), (2) accent color (16 Radix-based palettes + "none"), and (3) font family (Default/Serif/Mono). All are persisted to `localStorage` and applied via `data-*` HTML attributes that drive CSS variable selectors.
- **CSS variable architecture:** Uses three namespaced variable families: `--rx-*` (Radix raw color scales), `--lx-*` (semantic accent/gray mappings), and `--ls-*` (legacy Logseq-specific UI variables). Each accent color remaps all three layers.
- **"Cancel accent color" (beta):** Sets `data-color="none"` on the `<html>` element. Since no CSS selector matches `[data-color=none]`, all accent variable overrides are dropped — bare base CSS applies. This exists primarily for compatibility with custom themes that define their own color systems.
- **Plugin theme API:** Plugins can inject custom themes via `logseq.provideTheme()` (loads external CSS files and switches light/dark mode) and custom CSS via `logseq.provideStyle()`. The `LSPluginThemeManager` handles registration, selection, and lifecycle.
- **Custom CSS:** Users can override any CSS variable in `logseq/custom.css` (stored as a file entity in the DB graph, or as a file on disk in file-based graphs). This is the primary user customization mechanism going forward.

---

## 2. Technical Deep Dive

### 2.1 Theme Mode (light / dark / system)

**Purpose:** Controls the base light/dark color scheme of the entire application.

**Settings UI:**  
[src/main/frontend/components/settings.cljs](src/main/frontend/components/settings.cljs#L311) — `theme-modes-row` component

**State keys:**
| Key | Type | Storage | Purpose |
|-----|------|---------|---------|
| `:ui/theme` | `"light"` \| `"dark"` | `localStorage` | Active resolved theme |
| `:ui/system-theme?` | `boolean` | `localStorage` | Whether to follow OS preference |

**Core functions** in [src/main/frontend/state.cljs](src/main/frontend/state.cljs):

| Function             | Lines     | Purpose                                                               |
| -------------------- | --------- | --------------------------------------------------------------------- |
| `use-theme-mode!`    | 1267–1280 | Entry point: accepts `"light"`, `"dark"`, or `"system"`               |
| `set-theme-mode!`    | 1247–1257 | Sets mode, persists, handles mobile native theming                    |
| `sync-system-theme!` | 1259–1265 | Reads `window.matchMedia("(prefers-color-scheme: dark)")` and applies |
| `toggle-theme!`      | 1285      | Cycles between light ↔ dark                                           |

**DOM effect** in [src/main/frontend/components/theme.cljs](src/main/frontend/components/theme.cljs#L44-L55):

```clojure
(.setAttribute doc "data-theme" theme)         ;; "light" or "dark"
(if (= theme "dark")
  (do (.add cls "dark")
      (doto cls-body (.remove "white-theme" "light-theme") (.add "dark-theme")))
  (do (.remove cls "dark")
      (doto cls-body (.remove "dark-theme") (.add "white-theme" "light-theme"))))
```

Multiple mechanisms apply simultaneously:

1. `data-theme="light|dark"` on `<html>` → drives CSS variable selectors
2. `.dark` class on `<html>` → drives Tailwind dark mode
3. `.dark-theme` / `.light-theme` / `.white-theme` on `<body>` → backward compat

**System theme listener** in [src/main/frontend/ui.cljs](src/main/frontend/ui.cljs#L418-L425):

```clojure
;; setup-system-theme-effect! listens for OS theme changes
(.addListener (js/window.matchMedia "(prefers-color-scheme: dark)")
              "change" (fn [] (state/sync-system-theme!)))
```

**Plugin hook:** After theme change, plugins are notified:

```clojure
(plugin-handler/hook-plugin-app :theme-mode-changed {:mode theme})
```

---

### 2.2 Accent Color System (Radix Color Engine)

**Purpose:** Provides 16 accent color palettes that remap the entire UI color scheme — not just accent highlights, but also gray scales, link colors, borders, and backgrounds.

**Settings UI:**  
[src/main/frontend/components/settings.cljs](src/main/frontend/components/settings.cljs#L330-L373) — `accent-color-row` component

**Available colors** (defined in [src/main/frontend/colors.cljs](src/main/frontend/colors.cljs#L6)):

```
:tomato :red :crimson :pink :plum :purple :violet :indigo :blue :cyan :teal :green :grass :orange
```

Plus two special values: `:logseq` (default) and `:none` (cancel/reset).

**State & persistence:**

```clojure
;; state.cljs:1989-1991
(defn set-color-accent! [color]
  (swap! state assoc :ui/radix-color color)
  (storage/set :ui/radix-color color))
```

**DOM attribute flow:**  
[src/main/frontend/components/container.cljs](src/main/frontend/components/container.cljs#L373):

```clojure
accent-color (some-> (state/sub :ui/radix-color) (name))
;; :blue → "blue", :none → "none", nil → nil
```

[src/main/frontend/components/theme.cljs](src/main/frontend/components/theme.cljs#L57-L61):

```clojure
(.setAttribute doc "data-color" (or accent-color "logseq"))
;; Sets: "logseq", "blue", "none", etc.
```

#### CSS Variable Architecture

The accent system uses three cascading CSS variable layers:

```
┌─────────────────────────────────────────────────────┐
│ Layer 1: Raw Radix Scales (radix-hsl.css)           │
│  :root { --rx-blue-01-hsl: 206 100% 99.2%; ... }   │
│  30+ colors × 12 shades each                        │
├─────────────────────────────────────────────────────┤
│ Layer 2: Semantic Accent Mapping (colors.css)       │
│  html[data-color=blue] {                            │
│    --lx-accent-01: var(--rx-blue-01);               │
│    --lx-gray-01: var(--rx-slate-01);    ← gray remap│
│    --primary: 206 100% 50%;                         │
│  }                                                  │
├─────────────────────────────────────────────────────┤
│ Layer 3: UI Variables (vars-classic.css)            │
│  html[data-theme=dark][data-color=logseq] {         │
│    --ls-primary-background-color: #002b36;          │
│    --ls-link-text-color: rgb(138, 187, 187);        │
│  }                                                  │
└─────────────────────────────────────────────────────┘
```

**Key files:**

| File                                                                 | Content                                                                                      |
| -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| [packages/ui/src/radix-hsl.css](packages/ui/src/radix-hsl.css)       | Raw Radix color scales (30+ palettes, 12 shades each, HSL format)                            |
| [packages/ui/src/colors.css](packages/ui/src/colors.css)             | Per-accent-color semantic mapping (`--lx-accent-*`, `--lx-gray-*`, `--primary`, link colors) |
| [packages/ui/src/vars-classic.css](packages/ui/src/vars-classic.css) | Logseq-specific UI variables per `data-theme` × `data-color` combination                     |
| [packages/ui/src/index.css](packages/ui/src/index.css)               | Tailwind + shadcn/ui base variables (`--background`, `--foreground`, `--card`, etc.)         |

**What each accent color remaps** (example from [packages/ui/src/colors.css](packages/ui/src/colors.css#L57)):

For `html[data-color=tomato]`:

- `--lx-accent-01` through `--lx-accent-12` → `var(--rx-tomato-01..12)` (the accent palette)
- `--lx-gray-01` through `--lx-gray-12` → `var(--rx-mauve-01..12)` (harmonized gray)
- `--primary`, `--accent`, `--ring` → hue-shifted HSL values
- `--ls-link-text-color` → `var(--rx-tomato-11)`

Each accent color pairs with a complementary gray scale:

| Accent                     | Gray Scale      |
| -------------------------- | --------------- |
| logseq                     | gray            |
| tomato, red, crimson, pink | mauve           |
| blue                       | slate           |
| plum, purple, brown        | sand            |
| violet, indigo             | sage            |
| cyan, teal                 | olive           |
| green, grass               | mint (rx-sage)  |
| orange                     | brown (rx-sand) |

---

### 2.3 "Cancel Accent Color" (`:none`) — Beta Feature

**UI:** The first button in the accent color picker — a red striped circle.

**Tooltip text:** _"Cancel accent color. This is currently in beta stage and mainly used for compatibility with custom themes."_

**Location:** [src/main/frontend/components/settings.cljs](src/main/frontend/components/settings.cljs#L355-L361)

**Mechanism:**

```txt
User clicks "Cancel" → state/set-color-accent!(:none)
  → localStorage: :ui/radix-color = :none
  → (some-> :none (name)) = "none"
  → <html data-color="none">
  → No CSS selector matches html[data-color=none]
  → All --lx-accent-*, --lx-gray-*, --primary overrides disappear
  → Base/fallback CSS applies
```

**Why it exists:** Custom themes (both plugin themes and `custom.css`) often define their own complete color system. The Radix accent color overrides would conflict with these custom definitions. By "canceling" the accent color, the custom theme's CSS takes full control.

**Risk:** 🟡 **Medium** — When `:none` is active and no custom theme provides colors, the UI falls back to bare Tailwind/shadcn defaults which may look incomplete or inconsistent. This is why it's marked as "beta."

---

### 2.4 Font Family System

**Purpose:** Controls the typeface used in the editor and optionally the entire UI.

**Settings UI:**  
[src/main/frontend/components/settings.cljs](src/main/frontend/components/settings.cljs#L242-L265) — `editor-font-family-row`

**Options:** `default` | `serif` | `mono`  
**Global toggle:** "Set as global font family" checkbox

**State:**

```clojure
;; Stored as: {:type "serif" :global true}
(defn set-editor-font! [config]
  (let [config' (:ui/editor-font @state)
        config (if (map? config') (merge config' config) {})]
    (swap! state assoc :ui/editor-font config)
    (storage/set :ui/editor-font config)))
```

**DOM effect** in [src/main/frontend/components/theme.cljs](src/main/frontend/components/theme.cljs#L63-L68):

```clojure
(doto js/document.documentElement
  (.setAttribute "data-font" (or type "default"))
  (.setAttribute "data-font-global" (boolean global)))
```

**CSS selectors** match `html[data-font=serif]`, `html[data-font=mono]`, etc., and `html[data-font-global=true]` extends the font family from the editor to the full page.

---

### 2.5 Custom CSS (`logseq/custom.css`)

**Purpose:** The primary mechanism for user-driven UI customization.

**How it works in DB mode:**

1. CSS is stored as a file entity with path `"logseq/custom.css"` inside the graph database
2. On graph load, [src/main/frontend/handler/ui.cljs](src/main/frontend/handler/ui.cljs#L111-L125) retrieves it:

```clojure
(defn add-style-if-exists! []
  (when-let [style (or (state/get-custom-css-link)    ;; URL override from config
                       (db-model/get-custom-css))]    ;; custom.css file content
    (p/let [style (assets-handler/<expand-assets-links-for-db-graph style)]
      (util/add-style! style))))
```

3. CSS is injected into a `<style id="logseq-custom-theme-id">` element in the DOM
4. On graph switch, `reset-custom-css!` removes the old style element and re-injects

**File-based graphs:** reads `{repo}/.logseq/custom.css` from disk

**DB graphs:** retrieves from DataScript via `(get-file repo "logseq/custom.css")`

**Path resolution** in [src/main/frontend/config.cljs](src/main/frontend/config.cljs#L274-L282):

```clojure
(defn get-custom-css-path [repo]
  (if (db-based-graph? repo)
    (path/path-join app-name custom-css-file)           ;; "logseq/custom.css"
    (when-let [repo-dir (get-repo-dir repo)]
      (path/path-join repo-dir app-name custom-css-file)))) ;; "{repo}/logseq/custom.css"
```

---

### 2.6 Plugin Theme API

**Purpose:** Allows plugins to provide complete themes (CSS + mode).

#### Providing a Theme

```typescript
// Plugin API (libs/src/LSPlugin.ts:3786)
logseq.provideTheme({
  name: 'My Theme',
  url: 'https://example.com/theme.css', // CSS file URL
  description: 'A custom dark theme',
  mode: 'dark', // "light" | "dark"
  pid: 'my-plugin-id',
})
```

**Flow:**

```txt
logseq.provideTheme(theme)
  → LSPlugin.user.ts: caller.call('provider:theme', theme)
  → LSPluginCore: pluginLocal.themeMgr.registerTheme(id, theme)
  → Theme stored in Map<PluginLocalIdentity, Theme[]>
  → User selects theme from plugin settings
  → selectTheme() loads CSS and sets mode
```

#### Injecting Custom CSS

```typescript
// Plugin API (libs/src/LSPlugin.ts:3794-3799)
logseq.provideStyle(`
  @import url("https://fonts.googleapis.com/css2?family=JetBrains+Mono");
  .editor { font-family: 'JetBrains Mono'; }
`)
```

#### Theme Application

[src/main/frontend/ui.cljs](src/main/frontend/ui.cljs#L404-L415):

```clojure
(defn apply-custom-theme-effect! [theme]
  (when config/lsp-enabled?
    (when-let [custom-theme (state/sub [:ui/custom-theme (keyword theme)])]
      (if (some? (:name custom-theme))
        (js/LSPluginCore.selectTheme (bean/->js custom-theme)
                                     (bean/->js {:emit false}))
        (state/set-state! :plugin/selected-theme (:url custom-theme))))))
```

---

## 3. Flow Diagrams

### 3.1 Theme Mode Selection

```txt
User clicks "dark" in Settings
        │
        ▼
  state/use-theme-mode!("dark")
        │
        ├─ set-state! :ui/system-theme? false
        ├─ storage/set :ui/system-theme? false
        └─ set-theme-mode!("dark", false)
               │
               ├─ set-state! :ui/theme "dark"
               ├─ storage/set :ui/theme "dark"
               └─ (mobile: set-native-interface-style!)
                        │
                        ▼
           Rum re-render → theme.cljs container
                        │
                        ├─ <html data-theme="dark">
                        ├─ <html class="dark">
                        ├─ <body class="dark-theme">
                        ├─ apply-custom-theme-effect!("dark")
                        └─ plugin-handler/hook-plugin-app :theme-mode-changed
```

### 3.2 Accent Color Selection

```txt
User clicks "blue" swatch
        │
        ▼
  state/set-color-accent!(:blue)
        │
        ├─ swap! state :ui/radix-color :blue
        └─ storage/set :ui/radix-color :blue
                │
                ▼
    Rum re-render → container.cljs
                │
    accent-color = (some-> :blue name) = "blue"
                │
                ▼
    theme.cljs: (.setAttribute doc "data-color" "blue")
                │
                ▼
    CSS selector activates: html[data-color=blue] {
      --lx-accent-01: var(--rx-blue-01);
      --lx-gray-01: var(--rx-slate-01);
      --primary: 206 100% 50%;
      --ls-link-text-color: var(--rx-blue-11);
      ... 50+ variables remapped
    }
```

### 3.3 "Cancel Accent Color" Flow

```txt
User clicks red-striped ⊘ button
        │
        ▼
  state/set-color-accent!(:none)
        │
        ├─ :ui/radix-color = :none
        └─ localStorage = :none
                │
                ▼
    (some-> :none name) = "none"
                │
                ▼
    <html data-color="none">
                │
                ▼
    ⚠ No CSS rule: html[data-color=none] { ... }
                │
                ▼
    All --lx-accent-*, --lx-gray-*, --primary
    overrides DO NOT APPLY
                │
                ▼
    Base CSS / custom theme CSS takes full control
```

---

## 4. Complete DOM Attribute Summary

All appearance settings converge on `<html>` element attributes:

```html
<html data-theme="dark"
<!-- "light" | "dark" -->
data-color="blue"
<!-- accent color name | "none" -->
data-font="default"
<!-- "default" | "serif" | "mono" -->
data-font-global="false"
<!-- "true" | "false" -->
lang="en"
<!-- locale -->
class="dark"
<!-- Tailwind dark mode -->
>
<body class="dark-theme">
  <!-- backward compat -->
</body>
```

CSS selectors then match these combinations:

```css
html[data-theme='dark'][data-color='blue'] {
  /* dark + blue accent */
}
html[data-theme='light'][data-color='logseq'] {
  /* light + default */
}
html[data-font='serif'][data-font-global='true'] {
  /* serif everywhere */
}
```

---

## 5. How Users Customize the UI Going Forward

### 5.1 Built-in Settings (No Code Required)

| Setting       | What It Controls                       | Persistence                                    |
| ------------- | -------------------------------------- | ---------------------------------------------- |
| Theme mode    | Light/dark/follow system               | `localStorage :ui/theme` + `:ui/system-theme?` |
| Accent color  | Entire color palette (16 choices)      | `localStorage :ui/radix-color`                 |
| Font family   | Default (Inter) / Serif / Mono         | `localStorage :ui/editor-font`                 |
| Global font   | Apply font to entire UI vs editor only | `localStorage :ui/editor-font {:global true}`  |
| Wide mode     | Max-width of content area              | state                                          |
| Show brackets | Show/hide `[[` `]]` around page links  | state                                          |

### 5.2 Custom CSS (`logseq/custom.css`)

The most powerful built-in customization. Users can override any CSS variable:

```css
/* Example: Custom accent colors with cancel mode */
:root {
  --lx-accent-09: hsl(280, 80%, 60%);
  --ls-primary-background-color: #1a1a2e;
  --ls-link-text-color: #e94560;
}

/* Target specific themes */
html[data-theme='dark'] {
  --ls-primary-background-color: #0f0f23;
}
```

**In DB mode:** Edit via Logseq's built-in file editor (navigate to `logseq/custom.css` page).

### 5.3 Plugin Themes

Plugins provide complete, switchable themes:

```typescript
// Plugin provides: CSS file + mode declaration
logseq.provideTheme({
  name: 'Dracula',
  url: './theme.css',
  mode: 'dark',
})

// Plugin injects additional CSS
logseq.provideStyle(`
  .cp__header { backdrop-filter: blur(20px); }
`)
```

### 5.4 Customization Priority (Cascade Order)

```txt
1. Browser defaults
   ↓ overridden by
2. Radix color scales (radix-hsl.css)
   ↓ overridden by
3. Accent color mapping (colors.css via data-color)
   ↓ overridden by
4. Theme mode variables (vars-classic.css via data-theme + data-color)
   ↓ overridden by
5. Plugin theme CSS (loaded via <link>)
   ↓ overridden by
6. logseq/custom.css (injected as <style>)
   ↓ overridden by
7. Plugin provideStyle() CSS (injected as <style>)
```

**`custom.css` has high priority** — it overrides both the built-in theme system and most plugin themes, making it the user's final say.

---

## 6. Code Location Reference

| File                                                                                                 | Purpose                             | Key Functions                                                                 |
| ---------------------------------------------------------------------------------------------------- | ----------------------------------- | ----------------------------------------------------------------------------- |
| [src/main/frontend/components/settings.cljs](src/main/frontend/components/settings.cljs#L311-L382)   | Settings UI                         | `theme-modes-row`, `accent-color-row`, `editor-font-family-row`, `appearance` |
| [src/main/frontend/components/theme.cljs](src/main/frontend/components/theme.cljs#L37-L80)           | DOM attribute application           | `container` component                                                         |
| [src/main/frontend/components/container.cljs](src/main/frontend/components/container.cljs#L370-L405) | State → props bridge                | passes `:accent-color`, `:theme`, `:editor-font`                              |
| [src/main/frontend/state.cljs](src/main/frontend/state.cljs#L1247-L1300)                             | State management + persistence      | `use-theme-mode!`, `set-color-accent!`, `set-editor-font!`                    |
| [src/main/frontend/colors.cljs](src/main/frontend/colors.cljs)                                       | Color utility functions             | `color-list`, `variable`, `get-accent-color`                                  |
| [src/main/frontend/ui.cljs](src/main/frontend/ui.cljs#L404-L425)                                     | Plugin theme application            | `apply-custom-theme-effect!`, `setup-system-theme-effect!`                    |
| [src/main/frontend/handler/ui.cljs](src/main/frontend/handler/ui.cljs#L111-L125)                     | Custom CSS loading                  | `add-style-if-exists!`, `reset-custom-css!`                                   |
| [packages/ui/src/radix-hsl.css](packages/ui/src/radix-hsl.css)                                       | Raw Radix color scales              | 30+ palettes × 12 shades                                                      |
| [packages/ui/src/colors.css](packages/ui/src/colors.css)                                             | Accent → CSS variable mapping       | 16 `html[data-color=X]` rulesets                                              |
| [packages/ui/src/vars-classic.css](packages/ui/src/vars-classic.css)                                 | Logseq UI variables per theme/color | `--ls-*` variables                                                            |
| [packages/ui/src/index.css](packages/ui/src/index.css)                                               | Tailwind + shadcn/ui base           | `--background`, `--foreground`, etc.                                          |
| [libs/src/LSPlugin.ts](libs/src/LSPlugin.ts#L14-L26)                                                 | Plugin theme type defs              | `Theme`, `ThemeMode`, `ILSPluginThemeManager`                                 |
| [libs/src/LSPlugin.user.ts](libs/src/LSPlugin.user.ts#L606-L616)                                     | Plugin API calls                    | `provideTheme()`, `provideStyle()`                                            |

---

## 7. Recommendations

### For Users

- **Custom theme compatibility:** If using a custom theme plugin, click the ⊘ "Cancel accent color" button to prevent the built-in Radix accent system from conflicting with the theme's own colors.
- **Maximum customization:** Use `logseq/custom.css` — it has higher CSS specificity than both the accent color system and plugin themes.
- **Font choice:** Enable "Set as global font family" checkbox to apply the selected font beyond just the editor area.

### For Developers

- **Adding a new accent color:** Add the keyword to `colors/color-list` in [colors.cljs](src/main/frontend/colors.cljs#L6), then add matching CSS rules in [packages/ui/src/colors.css](packages/ui/src/colors.css) following the existing pattern (accent 01–12 + gray scale + alpha variants).
- **The `:none` color UX could be improved:** Consider adding an explicit `html[data-color=none]` CSS ruleset that provides sensible defaults, rather than relying on the absence of matches. This would make the "cancel" state more predictable.
- **Theme persistence is localStorage-only:** Theme preferences (`:ui/radix-color`, `:ui/theme`, `:ui/editor-font`) are stored per-browser via `localStorage`, not in the graph database. They do not sync across devices.

### For Plugin Authors

- Use `logseq.provideTheme()` for full themes (CSS file + light/dark mode declaration)
- Use `logseq.provideStyle()` for incremental CSS additions
- Target CSS variables (`--lx-accent-*`, `--ls-*`) rather than hard-coded colors for theme compatibility
- Listen for `:theme-mode-changed` hook to adapt to light/dark switches

---

## 8. Glossary

| Term               | Definition                                                                   |
| ------------------ | ---------------------------------------------------------------------------- |
| **Radix**          | Design system color scale library; provides 12-shade palettes for 30+ colors |
| **`--rx-*`**       | CSS variables for raw Radix HSL color values                                 |
| **`--lx-*`**       | CSS variables for semantic Logseq mappings (accent + gray)                   |
| **`--ls-*`**       | CSS variables for legacy Logseq UI (backgrounds, text, borders)              |
| **`data-color`**   | HTML attribute that selects the active accent color palette                  |
| **`data-theme`**   | HTML attribute for light/dark mode                                           |
| **`data-font`**    | HTML attribute for font family selection                                     |
| **`LSPluginCore`** | JavaScript class managing plugin lifecycle, including theme registration     |
| **custom.css**     | Per-graph CSS file for user overrides                                        |
