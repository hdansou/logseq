# What's new — April 26, 2026 (CLI and sync focus)

This version is written for people who care most about automation, graph operations, and sync internals. The throughline of this release is that Logseq is becoming easier to script and easier to operate at scale.

---

## The CLI becomes a broader graph-operations surface

The `logseq` CLI now covers more of the graph lifecycle: tasks, nodes, assets, search, graph backup, sync debugging, browser opening, and skill management all moved further into first-class commands.

This is not just a feature-count release. The quality of the surface improved too: output modes are more consistent, `show` includes breadcrumbs, tables adapt better to terminal width, command completion goes deeper, import and upsert flows are safer, and db-worker discovery is more reliable.

Several dangerous failure modes were also closed down. Commands that previously modified built-in nodes, silently ignored partial updates, picked the wrong page, mishandled property types, or hid useful sync hints now behave more defensibly.

One notable compatibility change: the CLI shifts from `--data-dir` toward `--root-dir`, so automation that depends on the old flag should be reviewed.

---

## Sync gets more operable

The biggest user-visible sync addition is support for custom sync server URLs, which makes self-hosted deployments much more practical.

Underneath that, a lot of work went into making sync easier to reason about: uploads and pulls are deduplicated more carefully, more of the download pipeline runs inside workers, history-op and checksum handling is stricter, and replay diagnostics are better when something goes wrong.

Graph lifecycle behavior is cleaner too. Downloads clean up temp state more reliably, daemons shut down more gracefully, graph switching no longer forces as much unnecessary work, and remote runtimes stop more predictably when a graph closes or is deleted.

---

## Encrypted graphs recover more safely

Encrypted sync paths got a serious hardening pass. RSA bootstrap is enforced more carefully, stale encrypted key material is cleared when recovery is needed, decrypt failures refresh cached key state, and title-only transactions are preserved more reliably through rebase flows.

That combination matters because most bad sync failures are not single bugs. They are chains of small inconsistencies. This release reduces those chains.

---

## Performance work helps large scripted and synced graphs

Large graph operations are lighter in a few important places: recursive UUID title replacement is faster, several expensive low-level scans were removed or reduced, temporary batch transactions were avoided in common block operations, and large view rendering was made cheaper.

Search indexing now reports progress too, which is especially useful when a CLI-driven or sync-driven workflow lands a lot of content at once.

---

## Release engineering and tooling

The JavaScript workspace migrated to `pnpm`, plugin libraries were refreshed, Electron was adjusted for packaging stability, and test coverage expanded around CLI sync flows, offline scenarios, and plugin end-to-end behavior.

_Full technical changelog: [changelog_2026-03-23_to_2026-04-26.md](changelog_2026-03-23_to_2026-04-26.md)_