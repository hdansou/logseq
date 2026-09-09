---
mode: agent
description: Complete accuracy audit of dz/docs/ — one sub-agent per document, validated against source code.
---

You are a code detective tasked with a complete accuracy audit of all documentation under `dz/docs/`.
Load the `logseq-doc-updater` skill and follow its process exactly.

---

## Mission

Systematically validate every document in `dz/docs/` against the current Logseq source code.
The codebase is the single source of truth. Documentation must reflect what the code actually does today.

---

## Phase 0 — Execution Plan (do this first, before touching any file)

Before making any changes:

1. Inventory all `.md` files under `dz/docs/` (recursive).
2. Triage each file into one of:
   - **Validate** — living reference, guide, or investigation that should stay current
   - **Skip** — point-in-time reports, templates, prompts, creation summaries
   - **Candidate for archive** — file that covers a removed or replaced feature
3. Present the triage table to the user with file paths and proposed actions.
4. Get approval before proceeding to Phase 1.

---

## Phase 1 — Process Documents (one sub-agent per document, in parallel batches)

Spawn **one sub-agent per document** for every file marked **Validate**.
Run up to **8 sub-agents concurrently**; wait for each batch to complete before launching the next.

### Sub-agent prompt template

Use this exact prompt for each sub-agent, substituting `{FILE_PATH}` and `{TODAY_DATE}`:

---

> You are a code detective validating Logseq documentation against source code.
>
> **Your single task**
> File to validate: `{FILE_PATH}`
> Today's date: `{TODAY_DATE}`
>
> **Steps**
>
> 1. **Read the document** at `{FILE_PATH}` in full.
> 2. **Extract every technical claim**, including:
>    - File paths and namespace references
>    - Function names and API/MCP tool names and parameters
>    - Schema attributes, property names, class names
>    - CLI commands, flags, and subcommands
>    - Keyboard shortcuts and platform-specific bindings
>    - Version numbers (CLI, SDK, Electron, schema version)
>    - Feature descriptions and behavior
> 3. **Verify each claim against the source code.**
>    Authoritative source directories: `src/`, `deps/`, `packages/`.
>    Reference the validation checklist in `logseq-doc-updater` skill for key paths.
>    Do not assume anything is still true — check it in the code.
> 4. **Classify each finding:**
>    - `ACCURATE` — no change needed
>    - `STALE` — feature exists but details have changed
>    - `REMOVED` — feature no longer exists in the codebase
>    - `NEW` — code reveals a capability not mentioned in the document
> 5. **Update the document in-place:**
>    - Fix all `STALE` claims
>    - Mark `REMOVED` features with a note: `> **Removed:** <brief explanation>`
>    - Add `NEW` findings as a new section: `## Findings: New Capabilities`
>    - Add `> **Last revised:** {TODAY_DATE}` near the top (after title/frontmatter)
>    - If the entire document covers removed functionality, add `status: archived` to
>      front-matter and move the file to the nearest `archive/` subdirectory
> 6. **Return a structured report:**
>
>    ```
>    FILE: {FILE_PATH}
>    STATUS: updated | accurate | archived
>    FINDINGS:
>      - STALE: <claim> → <correction>
>      - REMOVED: <claim>
>      - NEW: <capability>
>      - ACCURATE: <count> claims verified
>    SUMMARY: <one paragraph>
>    ```
>
> **Constraints**
>
> - Never trust the existing doc; always re-verify claims in source code
> - Never fabricate features — if you cannot find it in code, say so explicitly
> - Do not edit any file other than `{FILE_PATH}`

---

### Batching rules

- Batch size: 8 documents per round
- Order batches by section: `01-onboarding` → `02-architecture` → `03-features` →
  `04-api-reference` → `05-developer-guides` → `06-user-guides` → `07-investigations` → `ddocs/`
- After each batch completes, collect all sub-agent reports before launching the next batch

---

## Phase 2 — New Documentation

After all batches complete, review sub-agent reports for `NEW` findings.
For each undocumented capability significant enough to warrant its own doc:

- Create a new doc in the appropriate `dz/docs/` section
- New investigations go into `dz/docs/07-investigations/`
- Use the `code-detective-docs` narrative style for investigation documents

---

## Phase 3 — Tracking

After each batch:

- Mark completed files in `dz/docs/TASK_LIST.md`
- Append change summaries to `dz/docs/DOCUMENTATION_CHANGELOG.md`

After all batches:

- Output a final roll-up: total validated / updated / archived / skipped, and top issue categories

---

## Constraints

- Never trust the existing docs; always re-verify claims in source code
- Never fabricate features — if you cannot find it in code, say so explicitly
- Always wait for Phase 0 approval before spawning sub-agents
- Report blockers; do not guess or fill gaps with assumptions
