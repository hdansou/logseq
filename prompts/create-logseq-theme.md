# Prompt: Create Logseq DB Theme

Use this prompt to generate a new theme for Logseq following the database theming architecture.

## Instructions

Create a complete Logseq theme with the following specifications:

### Theme Information

- **Theme Name**: [THEME_NAME] (e.g., "gruvbox", "rose-pine", "nord")
- **Color Palette**: [PROVIDE COLOR PALETTE WITH HEX VALUES]
- **License**: [LICENSE TYPE] (if applicable)
- **Author**: [AUTHOR/SOURCE]

### Required Structure

The theme must use Logseq's theming architecture with these components:

1. **HTML Selector**: `html[data-color=THEME_NAME]`
2. **Light/Dark Mode Support**: `&[data-theme=light]` and `&[data-theme=dark]`
3. **Color Scales**: 12-step scales for both accent and gray colors (01-12)
4. **Variable Mapping**: Map to both `--lx-*` (new design system) and `--ls-*` (legacy) variables
5. **Tag Styling**: Enhanced styling for `.block-tag` elements

### Color Scale Guidelines

For each theme, define these color scales:

#### Light Mode

```css
--THEME-light-accent-01: /* Lightest accent */
--THEME-light-accent-02: /* Very light accent */
--THEME-light-accent-03: /* Light accent background */
--THEME-light-accent-04: /* Subtle accent background */
--THEME-light-accent-05: /* UI element background */
--THEME-light-accent-06: /* UI element border */
--THEME-light-accent-07: /* Hovered UI border */
--THEME-light-accent-08: /* Solid accent color */
--THEME-light-accent-09: /* Hovered solid accent */
--THEME-light-accent-10: /* Primary accent text */
--THEME-light-accent-11: /* High contrast accent */
--THEME-light-accent-12: /* Darkest accent */

--THEME-light-gray-01: /* Page background */
--THEME-light-gray-02: /* Subtle background */
--THEME-light-gray-03: /* UI element background */
--THEME-light-gray-04: /* Hovered UI element */
--THEME-light-gray-05: /* Active/selected element */
--THEME-light-gray-06: /* Borders/separators (subtle) */
--THEME-light-gray-07: /* Borders/separators (ui) */
--THEME-light-gray-08: /* Borders (hovered) */
--THEME-light-gray-09: /* Solid backgrounds */
--THEME-light-gray-10: /* Hovered solid backgrounds */
--THEME-light-gray-11: /* Low contrast text */
--THEME-light-gray-12: /* High contrast text */
```

#### Dark Mode

Follow the same pattern but inverted: step 01 is darkest, step 12 is lightest.

### Required CSS Variables

Map the following variables for both light and dark modes:

#### New Design System (--lx-\*)

```css
--lx-accent-01 through --lx-accent-12
--lx-gray-01 through --lx-gray-12
```

#### Legacy System (--ls-\*)

```css
--ls-primary-background-color
--ls-secondary-background-color
--ls-tertiary-background-color
--ls-quaternary-background-color
--ls-link-text-color
--ls-link-text-hover-color
--ls-block-ref-link-text-color
--ls-secondary-text-color
--ls-primary-text-color
--ls-border-color
--ls-secondary-border-color
--ls-page-checkbox-color
--ls-selection-background-color
--ls-block-highlight-color
--ls-focus-ring-color
--ls-table-tr-even-background-color
--ls-page-properties-background-color
--ls-block-properties-background-color
--ls-page-inline-code-bg-color
--ls-cloze-text-color
--ls-wb-stroke-color-default
--ls-wb-background-color-default
--ls-wb-text-color-default
--ls-a-chosen-bg
```

#### Additional Variables (for dark mode)

```css
--background
--foreground
--accent
--accent-foreground
--primary
--primary-foreground
--ring
--secondary
--secondary-foreground
--muted
--border
--card
--card-foreground
--popover
--input
```

### Tag Styling Requirements

Include enhanced tag styling with:

- Border radius: 12px
- Padding: 3px 10px
- Margin: 0 2px
- Transition: all 0.15s ease
- Hover effects: translateY(-1px) with subtle box-shadow
- Background and border colors that match theme
- Font weight: 500 for tag links

### Template Structure

```css
/**
 * [THEME_NAME] Theme for Logseq
 * 
 * [DESCRIPTION]
 * 
 * Based on [SOURCE]
 * [LICENSE]
 * [COPYRIGHT]
 * [LINK]
 * 
 * Installation:
 * Add this to your custom.css or theme file
 */

html {
  &[data-color='THEME_NAME'] {
    /* Color scales */
    body,
    .dark-theme,
    .light-theme {
      /* Light mode accent scale */
      --THEME-light-accent-01: #HEX;
      /* ... steps 02-12 ... */

      /* Dark mode accent scale */
      --THEME-dark-accent-01: #HEX;
      /* ... steps 02-12 ... */

      /* Light mode gray scale */
      --THEME-light-gray-01: #HEX;
      /* ... steps 02-12 ... */

      /* Dark mode gray scale */
      --THEME-dark-gray-01: #HEX;
      /* ... steps 02-12 ... */
    }

    /* Light mode */
    &[data-theme='light'] {
      /* Map to --lx-* variables */
      --lx-accent-01: var(--THEME-light-accent-01);
      /* ... all 12 steps ... */

      --lx-gray-01: var(--THEME-light-gray-01);
      /* ... all 12 steps ... */

      /* Map to --ls-* legacy variables */
      --ls-primary-background-color: var(--THEME-light-gray-01);
      /* ... all legacy variables ... */
    }

    /* Dark mode */
    &[data-theme='dark'] {
      /* Additional variables */
      --background: H S% L%;
      --foreground: H S% L%;
      /* ... all additional variables ... */

      /* Map to --lx-* variables */
      --lx-accent-01: var(--THEME-dark-accent-01);
      /* ... all 12 steps ... */

      --lx-gray-01: var(--THEME-dark-gray-01);
      /* ... all 12 steps ... */

      /* Map to --ls-* legacy variables */
      --ls-primary-background-color: var(--THEME-dark-gray-01);
      /* ... all legacy variables ... */
    }
  }
}

/* Enhanced tag styling */
html[data-color='THEME_NAME'] {
  .block-tag {
    border-radius: 12px;
    padding: 3px 10px;
    margin: 0 2px;
    transition: all 0.15s ease;
  }

  &[data-theme='light'] .block-tag {
    background-color: /* appropriate light color */ ;
    border: 1px solid /* appropriate light border */;
  }

  &[data-theme='light'] .block-tag:hover {
    background-color: /* slightly darker */ ;
    border-color: /* slightly darker border */ ;
    transform: translateY(-1px);
    box-shadow: 0 2px 4px rgba(/* accent color with alpha */);
  }

  &[data-theme='dark'] .block-tag {
    background-color: /* appropriate dark color */ ;
    border: 1px solid /* appropriate dark border */;
  }

  &[data-theme='dark'] .block-tag:hover {
    background-color: /* slightly lighter */ ;
    border-color: /* slightly lighter border */ ;
    transform: translateY(-1px);
    box-shadow: 0 2px 4px rgba(/* accent color with alpha */);
  }

  .block-tag a.tag,
  .block-tag a.hash-symbol {
    font-weight: 500;
    opacity: 0.9;
  }

  .block-tag:hover a.tag,
  .block-tag:hover a.hash-symbol {
    opacity: 1;
  }
}
```

## Example Input

**Theme Name**: Nord
**Color Palette**:

```txt
Polar Night:
- nord0: #2E3440
- nord1: #3B4252
- nord2: #434C5E
- nord3: #4C566A

Snow Storm:
- nord4: #D8DEE9
- nord5: #E5E9F0
- nord6: #ECEFF4

Frost:
- nord7: #8FBCBB
- nord8: #88C0D0
- nord9: #81A1C1
- nord10: #5E81AC

Aurora:
- nord11: #BF616A (red)
- nord12: #D08770 (orange)
- nord13: #EBCB8B (yellow)
- nord14: #A3BE8C (green)
- nord15: #B48EAD (purple)
```

**License**: MIT License
**Author**: Arctic Ice Studio

## Expected Output

A complete CSS file with:

1. Proper file header with attribution
2. All 12-step color scales defined for both light and dark modes
3. Complete variable mappings for `--lx-*` and `--ls-*`
4. Enhanced tag styling
5. HSL values for additional dark mode variables
6. Proper nesting structure using `&` operator
7. Comments explaining color choices where helpful

## Validation Checklist

Before finalizing the theme, ensure:

- [ ] Theme uses `html[data-color=THEME_NAME]` selector
- [ ] Both light and dark modes are defined
- [ ] All 12 accent color steps are defined for light and dark
- [ ] All 12 gray color steps are defined for light and dark
- [ ] All `--lx-accent-*` variables are mapped (01-12)
- [ ] All `--lx-gray-*` variables are mapped (01-12)
- [ ] All `--ls-*` legacy variables are mapped
- [ ] Tag styling is included with hover effects
- [ ] Colors progress logically from light to dark (or vice versa)
- [ ] Accent colors are visually distinct from grays
- [ ] Text has sufficient contrast against backgrounds
- [ ] File includes proper attribution and licensing

## Reference Examples

See these existing themes for reference:

- [gruvbox-logseq-theme.css](../gruvbox-logseq-theme.css)
- [rose-pine-logseq-theme.css](../rose-pine-logseq-theme.css)

## Tips

1. **Color Scale Creation**: Use a color manipulation tool to generate smooth gradients between your base colors
2. **Accent Selection**: Choose the most distinctive color from your palette as the primary accent
3. **Gray Scale**: Ensure smooth progression from background to text colors
4. **Testing**: Test in both light and dark modes to ensure readability
5. **Consistency**: Keep spacing and sizing consistent with other Logseq themes
6. **Accessibility**: Aim for WCAG AA contrast ratios (4.5:1 for normal text)
