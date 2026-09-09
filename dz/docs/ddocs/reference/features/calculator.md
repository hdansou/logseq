---
title: 'Calculator Reference'
description: "Reference for Logseq's built-in calculator code block mode"
type: reference
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '0.11.x'
platforms: [desktop, web]
graphType: [file, db]
relatedDocs:
  reference:
    - '[Code Blocks Reference](../syntax/code-blocks.md)'
  investigations:
    - '[Calculator Investigation](../../../07-investigations/calculator-feature-investigation.md)'
---

> **Last revised:** 2026-09-09 (verified against `be800f1711`)
> The built-in calculator is a live `calc` code block mode inside Logseq. You enter expressions line by line and Logseq renders the result for each line beside the editor.

## Overview

The calculator is not a separate app or popup. It is a specialized code block mode backed by the shared code-block editor.

### What it does

- evaluates arithmetic expressions line by line
- supports variables and a `last` value for chaining calculations
- supports formatting directives such as `:format fix 2`
- supports alternate number bases such as hex, octal, and binary
- updates results immediately as you type

### What gets saved

Only the calculator block text is saved in your graph. The visible results are recomputed from that text whenever the block is opened again.

## Quick Start

### Create a calculator block

1. Start editing a block.
2. Type `/Calculator`.
3. Select **Calculator** from the slash command menu.
4. Enter expressions, one per line.

Example:

```calc
6 * 7
last / 2
```

Expected output:

```txt
42
21
```

## Evaluation Model

Each non-blank line is evaluated from top to bottom.

- Blank lines produce no result.
- Lines beginning with `#` are comments.
- Variables defined earlier in the block are available on later lines.
- The special variable `last` stores the most recent non-empty result.

Example:

```calc
# area of a circle
r = 4.5
PI * r^2
:format fix 2
last
```

## Supported Operators

| Syntax | Meaning | Example |
| ------ | ------- | ------- |
| `+` | addition | `1 + 2` |
| `-` | subtraction / negation | `10 - 3`, `-5` |
| `*` | multiplication | `6 * 7` |
| `/` | division | `22 / 7` |
| `^` | exponentiation | `2 ^ 8` |
| `mod` | modulo | `42 mod 5` |
| `!` | factorial | `5!` |
| `%` | percent literal | `15% * 200` |

The calculator follows standard operator precedence. Use parentheses when needed.

## Functions

The parser supports these built-in functions:

| Function | Meaning | Example |
| -------- | ------- | ------- |
| `sqrt(x)` | square root | `sqrt(20.25)` |
| `abs(x)` | absolute value | `abs(-3)` |
| `log(x)` | base-10 logarithm | `log(1000)` |
| `ln(x)` | natural logarithm | `ln(E)` |
| `exp(x)` | $e^x$ | `exp(1)` |
| `sin(x)` | sine, radians | `sin(PI/2)` |
| `cos(x)` | cosine, radians | `cos(PI)` |
| `tan(x)` | tangent, radians | `tan(PI/4)` |
| `asin(x)` | inverse sine | `asin(0.5)` |
| `acos(x)` | inverse cosine | `acos(0)` |
| `atan(x)` | inverse tangent | `atan(1)` |

## Constants

| Name | Value |
| ---- | ----- |
| `PI` | 3.14159265358979323846 |
| `E` | 2.71828182845904523536 |

You cannot redefine these constants.

## Variables

Assign a variable with `=`:

```calc
x = 12
y = x * 3
y - 4
```

Variable names:

- start with letters or underscores
- may contain digits after the first letter
- are scoped to the current calculator block evaluation

### `last`

The special variable `last` stores the previous non-empty result.

```calc
6 * 7
last * 3
sqrt(last)
```

## Comments

Use `#` to comment a full line or the rest of a line:

```calc
# this line is ignored
2 * 4 # inline comment
```

## Result Formatting

Formatting directives start with `:` and affect how later decimal results are displayed.

### Syntax

```calc
:format <mode> [parameter]
```

`fmt` is also accepted as a shorthand for `format`.

| Mode | Aliases | Parameter | Meaning |
| ---- | ------- | --------- | ------- |
| `norm` | `normal` | optional precision | normal output with significant-figure control |
| `fix` | `fixed` | required places | fixed decimal places |
| `sci` | `scientific` | optional places | scientific notation |
| `frac` | `fraction`, `fractions` | optional max denominator | mixed-number fractions |
| `imp` | `improper` | optional max denominator | improper fractions |

Mode names and their aliases are matched case-insensitively — the grammar marks
every one `(?i)` (`src/resources/grammar/calc.bnf#L41-L46`) — so `:format FIX 2`
and `:FMT Fixed 2` are both accepted.

Defaults when a parameter is omitted: `frac` and `imp` use a maximum denominator
of 4095, and `norm` uses a precision of 21 significant digits
(`src/main/frontend/extensions/calc.cljc#L169-L195`).

Examples:

```calc
PI * 4.5^2
:format fix 5
last

:format sci 3
last

:format norm 6
last
```

### Mixed and Improper Fractions

```calc
1.5 * 2.4
:format imp
last

:format frac
last
```

Mixed numbers are also valid input:

```calc
2 1/2 * 3 3/8 - 1 1/4
```

## Number Bases

The calculator accepts numbers in binary, octal, and hexadecimal form.

| Base | Prefix | Example |
| ---- | ------ | ------- |
| Binary | `0b` | `0b1110` |
| Octal | `0o` | `0o20` |
| Hexadecimal | `0x` | `0xFF` |

Digit-group separators are allowed in every base and are stripped before
evaluation, so `1,000,000` and `0xFF,FF` are valid input
(`src/resources/grammar/calc.bnf#L31-L34`, `calc.cljc#L43`).

You can also change the output base with directives. Each has a long alias and
all are case-insensitive (`calc.bnf#L47-L51`):

| Directive | Aliases |
| --------- | ------- |
| `:bin` | `:binary` |
| `:oct` | `:octal` |
| `:hex` | `:hexadecimal` |
| `:dec` | `:decimal` |

`:dec` works by naming a base that the formatter does not special-case, so
output falls back to the current `:format` mode
(`calc.cljc#L180-L206`). Note that a base directive **overrides** the format
mode while it is active — `:hex` wins over a previously set `:format fix 2`.

Example:

```calc
0x10 + 0x10
:hex
last
:bin
last
:dec
last
```

## Scientific Notation and Percentages

Examples:

```calc
1.23e6
15% * 240
200% ^ 3
```

## Failure Behavior

If a line cannot be parsed or evaluated, Logseq renders `?` for that output line.

This usually means one of these happened:

- malformed syntax
- invalid variable reference (`"Can't find variable %s"`, `calc.cljc#L111-L117`)
- unsupported input
- illegal reassignment of a constant such as `PI` (`"Can't redefine constant %s"`, `calc.cljc#L82-L87`)

The rest of the block still renders line by line: each line is parsed and
evaluated independently, and only failures are replaced with `?`
(`calc.cljc#L209-L215`, `calc.cljc#L230-L235`).

`?` is not the only failure surface. Some operations return `NaN` rather than
failing — notably factorial, which is only computed for positive integers below
254 and otherwise yields `NaN` (`calc.cljc#L59-L61`).

## Examples

### Basic Arithmetic

```calc
6 * 7
1 + 2 * (3 - 4)
(2 + 13) / (17 + 23)
```

### Variables and Reuse

```calc
x = -7
2 * x^2 + 11 * x - 21
```

### Trigonometry and Logs

```calc
sin(PI/2)
cos(PI)
tan(PI/3)
log(1000)
ln(10)
exp(1)
```

### Factorials and Modulo

```calc
10! / (4! * 6!)
42 mod 5
42 mod 6
3.75 mod 1.5
```

## Scope and Limitations

- Results are **derived**, not stored separately.
- Variables exist only within the current calculator block evaluation — a fresh
  `env` atom is created per evaluation of the whole block (`calc.cljc#L209-L215`),
  so nothing carries over between calculator blocks.
- Trigonometric functions use **radians**.
- Arithmetic runs on `bignumber.js`, so `+ - * / ^ mod` and factorial keep
  arbitrary precision. The transcendental functions (`log`, `ln`, `exp`, `sin`,
  `cos`, `tan`, `asin`, `acos`, `atan`) and non-integer exponentiation delegate
  to JavaScript `Math` and are therefore only double-precision
  (`calc.cljc#L55-L81`).
- Error output is intentionally compact: failed lines show `?` rather than detailed error text.

> **Unverified:** mobile behaviour. The calculator is a CodeMirror code-block
> mode, and nothing in the source gates it by platform, but this has not been
> exercised on a mobile build. Frontmatter lists `platforms: [desktop, web]`
> accordingly.

## Findings: New Capabilities

- **Format and base directives are case-insensitive and have long aliases** —
  `:format FIXED 2`, `:hexadecimal`, `:binary` all parse
  (`src/resources/grammar/calc.bnf#L41-L51`). Previously documented only in short,
  lowercase form.
- **Digit-group separators are accepted in numeric literals in every base**
  (`calc.bnf#L31-L34`; commas are stripped at `calc.cljc#L43`).
- **A `:format norm` directive clears any `:places` set by `fix`/`sci`**, and
  `:format frac` clears the `improper` flag left by `:format imp`
  (`calc.cljc#L97-L107`) — so the modes are cleanly switchable rather than
  accumulating.
- **`:format` directives return the current `last` value**, which is why a
  directive line renders the previous result rather than a blank
  (`calc.cljc#L91-L110`).

## Implementation Notes

The current implementation is backed by:

- slash command steps in [src/main/frontend/commands.cljs](../../../../../src/main/frontend/commands.cljs#L119-L123), registered in the slash-command table at [line 331-334](../../../../../src/main/frontend/commands.cljs#L331-L334)
- live code-block evaluation in [src/main/frontend/extensions/code.cljs](../../../../../src/main/frontend/extensions/code.cljs#L471-L474), with initial and re-render evaluation at [L590-L615](../../../../../src/main/frontend/extensions/code.cljs#L590-L615) and result rendering at [L638-L639](../../../../../src/main/frontend/extensions/code.cljs#L638-L639)
- parser and evaluator logic in [src/main/frontend/extensions/calc.cljc](../../../../../src/main/frontend/extensions/calc.cljc#L18-L215), with the results component at [L220-L235](../../../../../src/main/frontend/extensions/calc.cljc#L220-L235)
- grammar definition in [src/resources/grammar/calc.bnf](../../../../../src/resources/grammar/calc.bnf#L1-L52)
- slash-command label and description in [src/resources/dicts/en.edn](../../../../../src/resources/dicts/en.edn#L519-L520)

> **Note:** all four line ranges above drifted since the 2026-04-14 revision and
> have been re-pinned against `be800f1711`. The grammar in particular grew from
> 39 to 52 lines.

For a deeper code-level walkthrough, see the [Calculator Investigation](../../../07-investigations/calculator-feature-investigation.md).
