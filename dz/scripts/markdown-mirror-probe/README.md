# Markdown Mirror and paste syntax probes

Scratch probes behind the two cheatsheets in `dz/docs/ddocs/reference/syntax/`. They print, they do
not assert. Last run against `be800f1711` on 2026-09-12.

| File | Feeds | Copy into |
|------|-------|-----------|
| `mirror_syntax_probe_test.cljs` | `markdown-mirror-syntax.md`: builds a test DB per construct and renders it with `frontend.worker.markdown-mirror/<mirror-page!` | `src/test/frontend/worker/` |
| `paste_syntax_probe_test.cljs` | `paste-and-type-syntax.md`: runs clipboard text through `paste-handler/editor-on-paste!`, inserts with `outliner-core/insert-blocks!`, renders the mirror; typed text through `wrap-parse-block`; HTML through `html-parser/hiccup->doc` | `src/test/frontend/handler/` |
| `build_cliworker_cheatsheet_page.py` | The "Markdown Mirror Cheatsheet" page in the `cliworker` graph, built with the `logseq` CLI. Property idents are hardcoded for that graph. | run directly |
| `build_cliworker_paste_page.py` | The "Paste and Type Cheatsheet" page in `cliworker`: copyable paste blocks next to live "You get" results that reproduce the paste probe. Imports the helpers above; needs Example Page, Example and Example Tag. | run directly |

```bash
# in a checkout of logseq master with node_modules installed
cp dz/scripts/markdown-mirror-probe/mirror_syntax_probe_test.cljs src/test/frontend/worker/
cp dz/scripts/markdown-mirror-probe/paste_syntax_probe_test.cljs src/test/frontend/handler/
clojure -M:test compile test
node static/tests.js -n frontend.worker.mirror-syntax-probe-test
node static/tests.js -n frontend.handler.paste-syntax-probe-test
```

The probes redefine `state/get-current-repo` to a `logseq_db_` repo: mldoc picks DB parse options from
it, and without it typed text is parsed with file-graph options.

Datetime values print in the machine's local time zone. `html-parser/convert` needs a browser DOM,
so the paste probe feeds the converter parsed HTML trees instead.
