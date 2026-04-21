---
title: 'Calculator Reference'
description: "Reference for Logseq's built-in calculator code block mode"
type: reference
status: stable
lastVerified: '2026-04-14'
logseqVersion: '0.11.x'
platforms: [desktop, web]
graphType: [file, db]
relatedDocs:
  reference:
    - '[Code Blocks Reference](../syntax/code-blocks.md)'
  investigations:
    - '[Calculator Investigation](../../../07-investigations/calculator-feature-investigation.md)'
---

> **Last revised:** 2026-04-14
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

You can also change the output base with directives:

- `:bin`
- `:oct`
- `:hex`
- `:dec`

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
- invalid variable reference
- unsupported input
- illegal reassignment of a constant such as `PI`

The rest of the block still renders line by line.

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
- Variables exist only within the current calculator block evaluation.
- Trigonometric functions use **radians**.
- Mobile support was not explicitly verified in the current code investigation.
- Error output is intentionally compact: failed lines show `?` rather than detailed error text.

## Implementation Notes

The current implementation is backed by:

- slash command registration in [src/main/frontend/commands.cljs](../../../../../src/main/frontend/commands.cljs#L106-L116)
- live code-block evaluation in [src/main/frontend/extensions/code.cljs](../../../../../src/main/frontend/extensions/code.cljs#L461-L464)
- parser and evaluator logic in [src/main/frontend/extensions/calc.cljc](../../../../../src/main/frontend/extensions/calc.cljc#L19-L232)
- grammar definition in [src/resources/grammar/calc.bnf](../../../../../src/resources/grammar/calc.bnf#L1-L39)

For a deeper code-level walkthrough, see the [Calculator Investigation](../../../07-investigations/calculator-feature-investigation.md).
