# Investigation: Logseq Calculator

**Investigation Date:** 2026-04-14  
**Last Verified:** 2026-05-30  
**Status:** Feature confirmed present and stable in shared frontend paths  
**Severity:** Low product risk, low documentation risk  
**Platforms Investigated:** Desktop, Web, shared frontend code; mobile command popup path verified, calculator execution path still not mobile-e2e verified  
**Graph Modes Investigated:** File-based and database graphs

> **Last revised:** 2026-05-30

---

## 1. Executive Summary

### Key Findings

1. ✅ **The calculator feature still exists** and is still wired into the slash command system as **`/Calculator`**. The command inserts a code block whose language is `calc`, then moves focus into the code editor. Source: [src/main/frontend/commands.cljs](../../../src/main/frontend/commands.cljs#L106-L116), [src/main/frontend/commands.cljs](../../../src/main/frontend/commands.cljs#L283-L285)
2. ✅ **Calculator evaluation is synchronous and local**. Every edit re-evaluates the entire calculator block immediately through `calc/eval-lines`; there is no background worker, debounce timer, IPC call, or network dependency in the calculator path. Source: [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L461-L464), [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L209-L215)
3. ✅ **Results are derived state, not persisted state**. The saved artifact is the code block content itself. Output is recomputed from the current block text whenever the editor mounts or changes. Source: [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L575-L575), [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L602-L603)
4. ✅ **The migrated reference page exists and remains aligned with core behavior.** Ongoing risk is documentation drift, not missing coverage. Source: [dz/docs/ddocs/reference/features/calculator.md](../ddocs/reference/features/calculator.md)
5. ⚠️ **Error visibility is intentionally minimal**. Parse failures and runtime errors are rendered as `?` in the UI, which keeps the block clean but hides diagnostic detail from end users. Source: [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L223-L232)

### Critical Risks or Issues

- 🟢 **Low data-loss risk inside the calculator implementation itself**: calculator state is pure and recomputed from block text.
- 🟡 **Medium UX risk for troubleshooting**: users get `?` instead of a parse or evaluation error message.
- 🟢 **Low docs risk**: the feature exists and a maintained reference page is present; primary risk is future drift.
- 🟡 **Platform ambiguity risk**: mobile command-popup infrastructure is present, but no calculator-specific mobile E2E verification was found.

## Findings: New Capabilities

1. ✅ **Mobile command popup support is now directly evidenced in source.** Mobile popup positioning logic explicitly handles editor command popups (`editor.commands` namespace), which is consistent with exposing slash-command flows on mobile UI surfaces. Source: [src/main/mobile/components/popup.cljs](../../../src/main/mobile/components/popup.cljs#L78-L109)

---

## 2. What Exactly Happens When a User Runs `/Calculator`

### Entry Point

The slash command registry defines a `Calculator` command. Its action sequence is:

1. Clear the active slash command text.
2. Convert the current block into a code block with language `calc`.
3. Focus the embedded CodeMirror editor.

**Source:** [src/main/frontend/commands.cljs](../../../src/main/frontend/commands.cljs#L106-L116), [src/main/frontend/commands.cljs](../../../src/main/frontend/commands.cljs#L283-L285)

### Execution Flow

```txt
User types /calculate or /calculator
    ↓
Slash command match: "Calculator"
    ↓
calc-steps
    ↓
[:editor/upsert-type-block :code "calc"]
    ↓
Code block editor mounts
    ↓
state :calc-atom initialized from current block text
    ↓
calc/eval-lines runs over all lines in the block
    ↓
Result column renders one output line per source line
```

### Timing Notes

- **Initial focus handoff:** the `:codemirror/focus` handler uses `setTimeout` with **256ms** to avoid a render-order issue when the slash command is selected with the mouse. Source: [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L606-L623)
- **Evaluation after edits:** calculator evaluation happens directly in the CodeMirror `change` handler with **no debounce window**. Source: [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L461-L464)

### Flow Diagram

```txt
User Action      Slash Command      Block Mutation         Eval Pass           UI Render
   [0ms]              [0ms]             [0-16ms]             [sync]              [sync]
     │                  │                   │                  │                   │
     └──────→ Calculator command ───────→ code:calc ───────→ eval-lines ───────→ result column
                               │
                               └──── focus retry after 256ms for mouse-selected command path
```

---

## 3. Technical Deep Dive

### Component A: Slash Command Registration

**Purpose:** exposes calculator as a user-invoked editor command.  
**Location:** [src/main/frontend/commands.cljs](../../../src/main/frontend/commands.cljs#L106-L116), [src/main/frontend/commands.cljs](../../../src/main/frontend/commands.cljs#L283-L285)

The controlling helper is `calc-steps`, which returns a fixed command vector:

```clojure
[[:editor/input "" {:last-pattern command-trigger}]
 [:editor/upsert-type-block :code "calc"]
 [:codemirror/focus]]
```

**Why it matters:** the calculator is not a separate modal or page. It is a specialized code block mode.

### Component B: Code Block Integration

**Purpose:** binds `calc` mode to live evaluation inside the standard code block editor.  
**Location:** [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L461-L464), [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L575-L575), [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L602-L603)

When the code editor mounts:

- `:calc-atom` is initialized with `(calc/eval-lines code)`.
- If the block language is `calc`, the editor subscribes to CodeMirror `change` events.
- Every change recomputes the full output vector and stores it in `:calc-atom`.

**Data structure:**

```clojure
{:editor-atom (atom nil)
 :calc-atom   (atom ["3" nil "7"]) ; example formatted results
 :code-options (atom options)
 :last-theme  (atom "...")}
```

**Lifecycle:**

- Created: when the code-block React component initializes.
- Updated: every CodeMirror change event.
- Cleared: when the component unmounts.
- Persisted: **No**. Only the block text is persisted.

### Component C: Calculator Interpreter

**Purpose:** parse, evaluate, and format expressions line by line.  
**Location:** [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L19-L20), [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L37-L117), [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L209-L215)

Core pieces:

- `parse`: Instaparse grammar-backed parser
- `constants`: built-in `PI` and `E`
- `new-env`: per-evaluation atom holding variables and formatting state
- `eval`: transforms AST into `BigNumber`-backed results or exceptions
- `eval-lines`: splits the block into lines and evaluates them top-to-bottom using a fresh environment for the whole block

**Important consequence:** variables such as `x` and `last` persist only within one evaluation pass of one block.

### Component D: Grammar Layer

**Purpose:** defines the accepted calculator language.  
**Location:** [src/resources/grammar/calc.bnf](../../../src/resources/grammar/calc.bnf#L1-L39)

Supported constructs found in the grammar:

- arithmetic: `+ - * / ^`
- modulo: `mod`
- factorial: `!`
- percentages: `%`
- scientific notation: `1e6`
- mixed numbers: `2 1/2`
- variables and assignment: `x = 42`
- comments: `# ...`
- formatting directives: `:format`, `:fmt`
- base directives: `:hex`, `:dec`, `:oct`, `:bin`

### Component E: UI Output Rendering

**Purpose:** display calculator outputs adjacent to the source lines.  
**Location:** [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L223-L232)

Rendering rules:

- blank lines render empty output
- successful lines render formatted value
- failures render `?`

This keeps the visual model simple: one source line, one output line.

---

## 4. Data Structures and Persistence

### `constants`

**Location:** [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L22-L23)  
**Type:** plain immutable map  
**Purpose:** built-in constant lookup

```clojure
{"PI" 3.14159265358979323846
 "E"  2.71828182845904523536}
```

### Evaluation Environment (`env`)

**Location:** [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L30-L30), [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L209-L215)  
**Type:** `(atom {})`  
**Purpose:** stores variables, `last`, and formatting mode during one evaluation pass

Example:

```clojure
{"x" 42
 "last" 84
 :mode "fix"
 :places 2}
```

**Lifecycle:**

- Created fresh for each `eval-lines` call.
- Updated as each line executes.
- Discarded after output vector is produced.
- Persisted: **No**.

### `calc-atom`

**Location:** [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L575-L575)  
**Type:** `(atom [...])`  
**Purpose:** React-visible derived output state for the open editor

Example:

```clojure
["42" "126" nil "3.14"]
```

**Lifecycle:**

- Created when editor component initializes.
- Updated on every editor change.
- Lost when component unmounts.
- Persisted: **No**.

### Actual Persistence Mechanism

The calculator feature does **not** persist its own state. Persistence is delegated to the normal block-saving path:

- In **file-based graphs**, the saved artifact is the block text inside markdown files.
- In **database graphs**, the saved artifact is the block text inside the graph database.

After reopening the graph, the calculator output is recomputed from the persisted text. There is no separate calculator database, cache, or snapshot.

---

## 5. Risks, Failure Modes, and Timing Windows

### Risk Analysis

- 🟢 **Low risk: hidden mutable state**  
  The calculator keeps no long-lived hidden state beyond the open editor component.

- 🟡 **Medium risk: opaque failures**  
  Users see `?` for parse or evaluation failures, but the UI does not explain whether the problem is syntax, unsupported input, or a bad variable reference.

- 🟡 **Medium risk: mobile ambiguity**  
  Mobile editor-command popup support is present in source, but calculator-specific mobile execution behavior still lacks direct E2E verification.

- 🟢 **Low risk: persistence mismatch**  
  Because outputs are recomputed from persisted source text, there is little opportunity for source/output divergence after reload.

### Timing Vulnerabilities

There is only one explicit timing-dependent behavior in the calculator-specific path:

- ⏱️ **256ms focus retry** for mouse-selected slash command activation. Source: [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs#L606-L623)

There is **no calculator-specific debounce or batching window** on expression evaluation.

---

## 6. Comparison Tables

### File-Based vs Database Graphs

| Aspect                                | File-Based Graph            | Database Graph              | Winner |
| ------------------------------------- | --------------------------- | --------------------------- | ------ |
| Slash command availability            | Same shared command path    | Same shared command path    | Tie    |
| Evaluation behavior                   | Same `calc/eval-lines` path | Same `calc/eval-lines` path | Tie    |
| What persists                         | Code block text in markdown | Code block text in DB       | Tie    |
| Calculator-specific state persistence | None                        | None                        | Tie    |
| Reload behavior                       | Recompute from text         | Recompute from text         | Tie    |

### Platform Comparison

| Aspect                                                  | Desktop | Web                 | Mobile                                      | Winner      |
| ------------------------------------------------------- | ------- | ------------------- | ------------------------------------------- | ----------- |
| Shared command registration                             | Yes     | Yes                 | Editor-command popup path present           | Desktop/Web |
| Shared calculator evaluator                             | Yes     | Yes                 | Likely shared code, not explicitly verified | Desktop/Web |
| Mobile command popup path for editor commands           | N/A     | N/A                 | Yes                                         | Mobile      |
| Explicit automated evidence found in this investigation | Yes     | Yes (frontend path) | No                                          | Desktop/Web |

---

## 7. Scenario Analysis

### Scenario A: Normal Use

**User action:** create a calculator block and type `1 + 2`

### Timeline

```txt
0ms     User selects /Calculator
0-16ms  Block becomes code block with lang=calc
256ms   Focus retry runs for mouse path
sync    User types expression
sync    eval-lines recomputes outputs
sync    UI shows 3
```

**Expected result:** the output column shows `3`.  
**Actual result from code and e2e coverage:** this is exactly what the app tests for. Source: [clj-e2e/test/logseq/e2e/commands_basic_test.clj](../../../clj-e2e/test/logseq/e2e/commands_basic_test.clj#L241-L248)  
**Risk level:** 🟢 Low

### Scenario B: Variable Reuse Within a Block

**User action:**

```calc
x = 10
x * 3
last + 1
```

**Expected result:** variables and `last` remain available within the same block evaluation pass.  
**Actual result:** supported by interpreter tests. Source: [src/test/frontend/extensions/calc_test.cljc](../../../src/test/frontend/extensions/calc_test.cljc#L152-L194)  
**Risk level:** 🟢 Low

### Scenario C: Syntax or Evaluation Error

**User action:** enter unsupported or malformed syntax

**Expected result:** line should fail cleanly without breaking the rest of the block.  
**Actual result:** the renderer converts failures to `?`. Source: [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc#L223-L232)  
**Risk level:** 🟡 Medium because the UI hides the cause of failure.

---

## 8. Code Location Reference

| File                                                                                                        | Purpose                                 | Key Functions / Data                                                  | Lines                     |
| ----------------------------------------------------------------------------------------------------------- | --------------------------------------- | --------------------------------------------------------------------- | ------------------------- |
| [src/main/frontend/commands.cljs](../../../src/main/frontend/commands.cljs)                                 | Slash command entry point               | `calc-steps`, `Calculator` command entry                              | ~106-116, ~283-285        |
| [src/main/frontend/extensions/code.cljs](../../../src/main/frontend/extensions/code.cljs)                   | Code block integration                  | `change` handler, `:calc-atom`, `:codemirror/focus`, results mounting | ~461-464, ~575, ~602-623  |
| [src/main/frontend/extensions/calc.cljc](../../../src/main/frontend/extensions/calc.cljc)                   | Parser, evaluator, formatter, UI output | `parse`, `constants`, `new-env`, `eval`, `eval-lines`, `results`      | ~19-23, ~37-117, ~209-232 |
| [src/resources/grammar/calc.bnf](../../../src/resources/grammar/calc.bnf)                                   | Calculator grammar                      | directives, operators, variable grammar                               | 1-39                      |
| [src/test/frontend/extensions/calc_test.cljc](../../../src/test/frontend/extensions/calc_test.cljc)         | Unit coverage                           | arithmetic, functions, formatting, variables, bases, failure cases    | 1-279                     |
| [clj-e2e/test/logseq/e2e/commands_basic_test.clj](../../../clj-e2e/test/logseq/e2e/commands_basic_test.clj) | E2E proof that the command works        | `calculator-test`                                                     | ~241-248                  |

> **Removed:** Previous source references to `dz/docs/progress.json` migration backlog status were documentation-process evidence and are no longer used to validate feature behavior in `src/`, `deps/`, or `packages/`.

---

## 9. Recommendations

### For Users

- Use the calculator as a **live `calc` code block**, not as a standalone document feature.
- Keep related variables inside the same block; calculator variables do not persist independently across blocks.
- Treat `?` as “this line failed” rather than “the whole block failed.”
- If mobile support matters for your workflow, verify calculator editing/output behavior on-device because current automated evidence is still desktop/web-heavy.

### For Developers

- Keep [dz/docs/ddocs/reference/features/calculator.md](../ddocs/reference/features/calculator.md) synchronized with `calc.bnf`, evaluator behavior, and slash-command UX.
- Consider exposing optional error detail on hover or in developer mode instead of only rendering `?`.
- Add an explicit mobile verification note or mobile test if calculator is intended to be supported there.
- Add backlinks from the code-block reference page since calculator is implemented as a specialized code block mode.

### For Testing

- Keep the existing E2E slash-command test; it verifies the full user-visible path.
- Add a UI test for failure rendering so `?` remains stable and intentional.
- Add a regression test for `:dec` / `:hex` / `:oct` / `:bin` transitions, since formatting mode is stateful within the per-block environment.
- Add a smoke test for reopening a saved `calc` block and confirming results recompute from persisted text.

---

## 10. Appendices

### Appendix A: Glossary

- **`calc` block**: a code block whose language is set to `calc`
- **`env`**: the per-evaluation atom that stores variables, `last`, and formatting mode
- **derived state**: data recomputed from persisted inputs rather than saved independently
- **Instaparse**: the parsing library used to implement the calculator grammar

### Appendix B: Related Historical Material

> **Removed:** Migration backlog tracking in `dz/docs/progress.json` is historical process data and not part of current source-of-truth feature validation.

### Appendix C: Change History

- **2026-04-14**: Initial investigation created after confirming the feature still exists in code.
- **2026-05-30**: Revalidated against `src/`, `deps/`, and `packages/`; updated mobile evidence wording, removed stale migration-backlog evidence, and refreshed risk framing.
