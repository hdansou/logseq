## Git Commit Log (2025-12-06 to 2026-01-11)

### Branch: upstream/copilot/sub-pr-12266
- [47a245fd2] (copilot-swe-agent[bot]) [2025-12-22]: Fix misleading comment in test
  > Co-authored-by: xyhp915 <1779837+xyhp915@users.noreply.github.com>

- [26399ed28] (copilot-swe-agent[bot]) [2025-12-22]: Remove commented-out debug code from test file
  > Co-authored-by: xyhp915 <1779837+xyhp915@users.noreply.github.com>

- [a51e41ae4] (copilot-swe-agent[bot]) [2025-12-22]: Add test coverage for get_tags_by_name API function
  > Co-authored-by: xyhp915 <1779837+xyhp915@users.noreply.github.com>

- [62eef01fb] (copilot-swe-agent[bot]) [2025-12-22]: Initial plan
  > 
- [eb80a4ba7] (charlie) [2025-12-22]: Merge branch 'master' into refactor/apis-types
  > 
- [dbc240903] (charlie) [2025-12-21]: fix: lints
  > 
- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [2d2a44e6b] (charlie) [2025-12-19]: enhances(apis): add apis for retrieving tags by name
  > 
- [513268edb] (charlie) [2025-12-19]: fix(page): prevent checking existing page with custom ident namespace
  > 
- [0c9345ec1] (charlie) [2025-12-19]: Merge branch 'master' into refactor/apis-types
  > 
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [941ecd493] (charlie) [2025-12-18]: chore(libs): update typedoc-plugin-lsp-docs to version 0.0.2 and fix build:docs command
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [3a16bca77] (charlie) [2025-12-18]: fix(property): add number input handling and effect for value updates
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [4bb24a54f] (charlie) [2025-12-17]: fix(apis): incorrect return to create page for the existing case
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [551e06b34] (charlie) [2025-12-17]: enhance(apis): add open pdf viewer function to handle PDF viewing
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [5b0c989fc] (charlie) [2025-12-17]: Merge branch 'master' into refactor/apis-types
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [b5266fb26] (charlie) [2025-12-12]: enhance(tests): improve tag handling tests and extend tag functionality
  > 
- [0e8ca543d] (charlie) [2025-12-12]: Merge branch 'master' into refactor/apis-types
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [dd069d08e] (charlie) [2025-12-11]: test(plugins): add create-and-get-tag test for tag creation and retrieval
  > 
- [1d90a43c8] (charlie) [2025-12-11]: refactor(apis): update getBlockProperty return type to null for clarity
  > 
- [1ce04a731] (charlie) [2025-12-11]: refactor(apis): unify block property parameter types for consistency
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [684554a32] (charlie) [2025-12-11]: Merge branch 'master' into refactor/apis-types
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [636c5a29b] (Charlie) [2025-12-10]: refactor(editor): simplify block retrieval and save logic
  > 
- [ca8abd04c] (Charlie) [2025-12-10]: refactor(api): unify block identifier parameter across functions
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/debug/add-tx-log
- [f7e1a3a6f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, only record rtc related tx-data
  > 
- [137e85beb] (rcmerci) [2026-01-06]: fix lint
  > 
- [e5668a469] (rcmerci) [2026-01-06]: fix lint
  > 
- [73888c0a9] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, update schema
  > 
- [1a33a365f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log
  > - integrate `debug-log-db` into GC process
- update schema with `repo` column and `DATETIME`
- export `reset_debug_log_db`

- [27016657f] (Tienson Qin) [2026-01-06]: add debug db tx-log and ws messages tables
  > 
- [6f513fa51] (Tienson Qin) [2026-01-06]: enhance(ux): make it clear that hourly backup is running on desktop
  > 
- [00ccff81e] (Tienson Qin) [2026-01-06]: more file-graph cleanup
  > 
- [23c6f8101] (charlie) [2026-01-06]: enhance(libs): add get_file_content function for retrieving file content
  > 
- [74bfe36d8] (charlie) [2026-01-06]: enhance(libs): update IDBProxy interface and improve documentation
  > 
- [8d7aaa791] (charlie) [2026-01-06]: enhance(apis): add setFileContent method for built-in file paths
  > 
- [6c1b9fe77] (Tienson Qin) [2026-01-06]: Add set_file_content api to update built-in files like custom.js/css
  > 
- [c2582d3c2] (Tienson Qin) [2026-01-06]: fix: available choices still shows other tags' scoped choices
  > 
- [289741069] (Tienson Qin) [2026-01-06]: tweak words for invalid db
  > 
- [574890505] (Gabriel Horner) [2026-01-05]: fix ref-property tests pointing to property rule
  > Also add back property tests as it is still a useful rule

- [6ba05ad2c] (Tienson Qin) [2026-01-05]: add e2e-test for isolated scope poroperties
  > 
- [79f5c6e3c] (Tienson Qin) [2026-01-05]: fix: tag-scoped properties still showing for other tags
  > 
- [28ba74628] (rcmerci) [2026-01-05]: chore(rtc): add fixme comment
  > 
- [1e61ae7dd] (rcmerci) [2026-01-05]: fix: lint
  > 
- [b82683357] (rcmerci) [2026-01-05]: dev: update agents.md
  > 
- [4dc4e1be7] (rcmerci) [2026-01-05]: enhance(rtc): add :add op to replace :move + :update combo
  > 
- [ae8710f76] (rcmerci) [2026-01-05]: dev: update review.md
  > 
- [41cbba368] (Tienson Qin) [2026-01-05]: enhance(ux): flashcards  (#12299)
  > Enhances the flashcard user experience by adding automatic query property management, fixing critical bugs, and improving the UI for managing card sets.

Key changes:

1. Automatic creation of query property blocks when tagging with #Query or subclasses via pipeline
2. Fixed critical bug in api-insert-new-block! where the end? parameter had inverted conditional logic
3. Added ability to create new #Cards blocks directly from the flashcard modal with a plus button
- [1cf4fa9ef] (Tienson Qin) [2026-01-05]: enhance(ux): add highlights to unlinked references
  > 
- [d1bbb5ad1] (charlie) [2026-01-05]: chore(libs): add development notes
  > 
- [2642765eb] (charlie) [2026-01-04]: chore: update db_properties_skill.md with enhanced property schema APIs and examples
  > 
- [a2bfb3963] (charlie) [2026-01-04]: chore: update version to 0.2.11 and add developer notes for Logseq DB properties
  > 
- [4b7d6c73c] (Tienson Qin) [2026-01-04]: fix: remove rtc test on updated-at
  > Since :block/updated-at should only be overwritten, not retracted

- [f940f708b] (Tienson Qin) [2026-01-04]: fix: rtc should never retract block timestamp
  > 
- [8c10cb64d] (Tienson Qin) [2026-01-04]: wrong ns
  > 
- [50e2bc53f] (Tienson Qin) [2026-01-04]: fix: filter out file graphs
  > 
- [a9a9905b0] (Tienson Qin) [2026-01-04]: feat: tag-scoped property choices (#12295)
  > * feat: tag-scoped property choices

* Able to hide global choices per tag

* add e2e tests
- [fd7e68582] (Tienson Qin) [2026-01-04]: chore: cleanup dicts
  > 
- [f43459f55] (Tienson Qin) [2026-01-04]: fix: wrong mobile event
  > 
- [e95c93629] (Tienson Qin) [2026-01-03]: fix: youtube embed on desktop
  > 
- [5705dd359] (Tienson Qin) [2026-01-03]: chore: code cleanup
  > 
- [ce5017fca] (charlie) [2026-01-03]: fix(tests): update tag properties in create tag test
  > 
- [9bd5390c0] (Tienson Qin) [2026-01-03]: enhance: don't close invite dialog
  > 
- [74d8308a6] (Tienson Qin) [2026-01-03]: chore: remove git commit command
  > 
- [0b77ee902] (Tienson Qin) [2026-01-03]: fix: trim blank for inviting email
  > 
- [baff8a4cc] (charlie) [2026-01-03]: enhance(apis): add support for tag properties in createTag API
  > 
- [664743b28] (Tienson Qin) [2026-01-03]: fix: logseq.Editor.renamePage HTTP API hangs indefinitely
  > related to https://github.com/logseq/db-test/issues/701

- [5459dbeb6] (Tienson Qin) [2026-01-03]: enhance: args for tag properties
  > 
- [8fd006bab] (Tienson Qin) [2026-01-03]: enhance: api createTag supports :tagProperties
  > 
- [ebb985f78] (Paweł Świątkowski) [2026-01-02]: Add ability to sort plugins by added date on marketplace (#11993)
  > Co-authored-by: Charlie <xyhp915@qq.com>
- [1cd561dbd] (Tienson Qin) [2026-01-01]: chore: CI cleanup
  > 
- [d4f4ad8f7] (Tienson Qin) [2026-01-01]: chore: bump version for mobile apps
  > 
- [324d02896] (Tienson Qin) [2026-01-01]: chore: remove rsapi from CI
  > 
- [44f26f91f] (Tienson Qin) [2026-01-01]: fix: add back ssl/keystore.jks for mobile development
  > 
- [497645082] (Tienson Qin) [2026-01-01]: fix: lint
  > 
- [51634b4c5] (Tienson Qin) [2026-01-01]: fix: collapse/expand toggle on mobile
  > 
- [4d46c5e9b] (Tienson Qin) [2026-01-01]: fix: youtube timestamp on web/electron
  > 
- [10846fdaa] (Tienson Qin) [2026-01-01]: fix: asset wrong order
  > 
- [7d493575d] (Tienson Qin) [2026-01-01]: fix: add missing datepicker.css
  > 
- [8fc093c1c] (Tienson Qin) [2026-01-01]: fix: can't set number 9 as property value
  > because it's empty-placeholder

- [44468dd20] (charlie) [2026-01-01]: enhance(apis): simplify tag resolution and enhance tag handling functions
  > 
- [5f42c51c3] (charlie) [2026-01-01]: enhance(property): add data attributes for property title and type
  > 
- [460eefb96] (charlie) [2026-01-01]: enhance(apis): add reset option to upsertBlockProperty for block properties
  > 
- [aafa50dcc] (Tienson Qin) [2026-01-01]: fix: :block/warning value should be keyword
  > 
- [6f59a02f5] (Tienson Qin) [2026-01-01]: fix: mark ensure-graph-uuid as deprecated
  > 
- [99dea608f] (rcmerci) [2026-01-01]: fix(ci): update typos.toml
  > 
- [ed1133095] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683 (2)
  > 
- [76224da1b] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683
  > 
- [0f949b739] (Tienson Qin) [2026-01-01]: fix: tests
  > 
- [b55550ed3] (Tienson Qin) [2026-01-01]: add :logseq.kv/local-graph-uuid
  > 
- [3078f2a90] (Tienson Qin) [2025-12-31]: chore: remove outdated test
  > 
- [15585f148] (Tienson Qin) [2025-12-31]: mod+m to publish
  > 
- [f1a410887] (rcmerci) [2025-12-31]: fix(rtc): fix invalid apply-ops msg
  > 
- [76ebafac6] (Tienson Qin) [2025-12-31]: enhance: preserve block collapsed state when publishing
  > 
- [5f532a6a8] (Gabriel Horner) [2025-12-30]: chore: update now outdated parts of db version section
  > 
- [a6503cc2f] (Gabriel Horner) [2025-12-30]: fix: add back validation of properties graph to CI
  > Accidentally taken out w/ last refactor

- [ca14110a8] (Gabriel Horner) [2025-12-30]: fix: nbb scripts failing to start
  > Caused by new dep that isn't configured for use with nbb.
Can add back when it's actually nbb compatible and has a nbb.edn

- [3b1433647] (Tienson Qin) [2025-12-30]: fix: set limit to passsword kdf iterations on cloudflare workers
  > 
- [e5964202d] (Tienson Qin) [2025-12-30]: Merge pull request #12279 from logseq/feat/page-publish
  > feat: page publish 
- [5f1695737] (Tienson Qin) [2025-12-30]: Merge branch 'master' into feat/page-publish
  > 
- [8cfcefcec] (charlie) [2025-12-30]: enhance(apis): add route handler and improve page block retrieval
  > 
- [5376779d4] (Tienson Qin) [2025-12-30]: deploy new version
  > 
- [8de2542f8] (Tienson Qin) [2025-12-30]: use salted PBKDF2 hash
  > 
- [f9a3fb50b] (Tienson Qin) [2025-12-30]: remove skip_auth
  > 
- [918dc0766] (Tienson Qin) [2025-12-30]: fix: set auth to be required on staging and prod
  > 
- [304195c7d] (Tienson Qin) [2025-12-30]: fix: typo
  > 
- [a0a7748e7] (Tienson Qin) [2025-12-30]: home page
  > 
- [adb7b8e36] (Tienson Qin) [2025-12-30]: fix: tagged nodes
  > 
- [dce1a8f4e] (Tienson Qin) [2025-12-30]: fix: e2e tests
  > 
- [6bc2fb1a0] (Tienson Qin) [2025-12-30]: fix: don't show published url property
  > 
- [45d592ecd] (Tienson Qin) [2025-12-30]: enhance: show close button when search box opened
  > 
- [c97453d1d] (Tienson Qin) [2025-12-30]: search
  > 
- [60e79785f] (Tienson Qin) [2025-12-30]: fix: add back missing dicts for flashcard buttons
  > 
- [c21f2c7f4] (Tienson Qin) [2025-12-30]: Ensure local graphs have uuid
  > 
- [644ef4c34] (Tienson Qin) [2025-12-30]: show authors
  > 
- [d53ea4a38] (Tienson Qin) [2025-12-30]: enhance: update published-url to be property type of :url
  > 
- [f8d494285] (Tienson Qin) [2025-12-30]: enhance: show page updated at
  > 
- [24c929835] (Tienson Qin) [2025-12-30]: fix: tests
  > 
- [12f967437] (Tienson Qin) [2025-12-30]: enhance: short url use "/p/short-id" instead of "/s/short-id"
  > 
- [74d136e49] (Tienson Qin) [2025-12-30]: fix: export tests
  > 
- [bdb6e0218] (Tienson Qin) [2025-12-30]: fix: remove icon from page properties area
  > 
- [8a415e007] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/publish.css
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [3e9786593] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/render.cljs
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [0ca329d63] (Tienson Qin) [2025-12-29]: add custom logseq/publish.css and logseq/publish.js
  > 
- [be69c009d] (Tienson Qin) [2025-12-29]: add user page
  > 
- [9dab647d0] (Tienson Qin) [2025-12-29]: add missing meta
  > 
- [da314220d] (Tienson Qin) [2025-12-29]: feat: unpublish
  > 
- [15d7526ac] (Tienson Qin) [2025-12-29]: fix: ast blocks in block title
  > 
- [2068123b2] (Tienson Qin) [2025-12-29]: fix: heading
  > 
- [0c79f4651] (Tienson Qin) [2025-12-29]: fix: cache published page
  > 
- [8c4be25e9] (Tienson Qin) [2025-12-29]: add timestamp version to invalid cache
  > 
- [7b7ec4706] (Tienson Qin) [2025-12-29]: add deploy script
  > 
- [efaff72f0] (Tienson Qin) [2025-12-29]: fix: mobile display issues
  > 
- [6c600d0a8] (Tienson Qin) [2025-12-29]: add dev script
  > 
- [8d6282121] (Tienson Qin) [2025-12-29]: add custom domain
  > 
- [ff56c6cbc] (Tienson Qin) [2025-12-29]: add publish staging and prod
  > 
- [7e695603d] (Tienson Qin) [2025-12-29]: Merge branch 'master' into feat/page-publish
  > 
- [bcc478b5f] (Tienson Qin) [2025-12-29]: refactor: separate og and db version (#12276)
  > separate og and new version apps

remove file sync, tldraw, excalidraw and zotero
- [56d3bd84d] (Tienson Qin) [2025-12-29]: add publish test
  > 
- [5ff22217d] (charlie) [2025-12-29]: fix(ui): adjust padding for property configuration header
  > 
- [5f185a39c] (charlie) [2025-12-29]: chore(libs): upgrade libs core and yarn.lock
  > 
- [62eb31c32] (Tienson Qin) [2025-12-29]: node embed support
  > 
- [78fbbaffa] (Tienson Qin) [2025-12-29]: fix: page icon
  > 
- [ef7cbd743] (Tienson Qin) [2025-12-29]: fix: url type property rendered as link
  > 
- [b1050da84] (Tienson Qin) [2025-12-29]: enhance: icon and tags position
  > 
- [5ed49d737] (Tienson Qin) [2025-12-29]: fix: positioned properties rendering
  > 
- [a04050d98] (Tienson Qin) [2025-12-29]: macro render
  > 
- [58f5a2ddc] (Tienson Qin) [2025-12-29]: block publish
  > 
- [3e033db0b] (Tienson Qin) [2025-12-29]: press `t o` to toggle collapse/expand
  > 
- [6d551e7b6] (Tienson Qin) [2025-12-29]: dark theme
  > 
- [db55b2d5c] (Tienson Qin) [2025-12-29]: style tweaks
  > 
- [a3d717812] (Tienson Qin) [2025-12-29]: multiple size variants for images
  > 
- [e77690b45] (Tienson Qin) [2025-12-28]: feat: password protected page
  > 
- [26c903f53] (Tienson Qin) [2025-12-28]: 404 page
  > 
- [0c668505c] (Tienson Qin) [2025-12-28]: friendly page for non published
  > 
- [326254c8f] (Tienson Qin) [2025-12-28]: style polish
  > 
- [12fabc63c] (Tienson Qin) [2025-12-28]: refactor: separate worker to multiple namespaces
  > 
- [55bf42887] (Tienson Qin) [2025-12-28]: code cleanup
  > 
- [4b2884abe] (Tienson Qin) [2025-12-28]: move inline style and script to publish.css and publish.js
  > 
- [b182a3736] (Tienson Qin) [2025-12-28]: fix: asset upload
  > 
- [cec45205d] (Tienson Qin) [2025-12-28]: wip: assets publish
  > 
- [aab7cfe2d] (Tienson Qin) [2025-12-28]: support math, code and quote blocks
  > 
- [80388f618] (Tienson Qin) [2025-12-28]: shorten url
  > 
- [0d38c982e] (Tienson Qin) [2025-12-28]: global ref
  > 
- [a682b9499] (Tienson Qin) [2025-12-28]: global tag
  > 
- [84cbb7931] (Tienson Qin) [2025-12-28]: tagged nodes
  > 
- [a4275838b] (Tienson Qin) [2025-12-28]: add graph page
  > 
- [08ab09a44] (Tienson Qin) [2025-12-28]: fix: property issues
  > 
- [7ad7e0b60] (Tienson Qin) [2025-12-28]: wip: properties publish
  > 
- [d4d8b1a7e] (Tienson Qin) [2025-12-28]: fix: add shadow-cljs.edn for publish worker
  > 
- [e9c2269e3] (Tienson Qin) [2025-12-28]: linked refs
  > 
- [36408416c] (Tienson Qin) [2025-12-27]: publish mvp
  > 
- [58cda5538] (Tienson Qin) [2025-12-27]: cljs worker
  > 
- [2a9624347] (Tienson Qin) [2025-12-27]: add publish worker
  > 
- [704312b40] (Tienson Qin) [2025-12-27]: Add publish dep
  > 
- [a2a5bfc09] (Tienson Qin) [2025-12-26]: remove temp workflow
  > 
- [c548542b4] (Tienson Qin) [2025-12-26]: add ci tool
  > 
- [7c7fe5e46] (Tienson Qin) [2025-12-26]: bump version to 2
  > 
- [8fe3cddba] (Tienson Qin) [2025-12-26]: enhance: limit access to db graphs only
  > 
- [8b312a403] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [678cb7dda] (Tienson Qin) [2025-12-26]: fix: init
  > 
- [910cd9514] (Tienson Qin) [2025-12-26]: fix: tests
  > 
- [c5c66aadc] (Tienson Qin) [2025-12-26]: update shadow-cljs.edn to remove tldraw and excalidraw
  > 
- [3d78209a6] (charlie) [2025-12-26]: chore(libs):bump new version
  > 
- [ec3ace5bd] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [cb9ec1d49] (Tienson Qin) [2025-12-26]: add sanitize-filename
  > 
- [c9a92f92b] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [c084a6b9d] (Tienson Qin) [2025-12-26]: fix dicts
  > 
- [ec7a325bc] (Tienson Qin) [2025-12-26]: more cleanup
  > 
- [f1de1ea16] (Tienson Qin) [2025-12-26]: remove git, chokidar, rsapi
  > 
- [467c5200d] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [7d719623a] (Tienson Qin) [2025-12-26]: refactor: prefer conn than repo for outliner core and rtc update
  > 
- [f6188b6cc] (Tienson Qin) [2025-12-25]: cleanup more config/db-based-graph? usage
  > 
- [eceaa7396] (Tienson Qin) [2025-12-25]: more cleanup
  > 
- [f419fbea8] (Tienson Qin) [2025-12-25]: cleanup
  > 
- [d8d6e8b4b] (Tienson Qin) [2025-12-25]: remove file based model
  > 
- [acbb03856] (Tienson Qin) [2025-12-25]: remove diff-merge
  > 
- [9f927137a] (Tienson Qin) [2025-12-25]: remove file based handlers
  > 
- [32cbac5a5] (Tienson Qin) [2025-12-25]: remove file based page handler
  > 
- [4aab5daca] (Tienson Qin) [2025-12-25]: add simplified datepicker
  > 
- [bbe6fa861] (Tienson Qin) [2025-12-25]: remove file based commands
  > 
- [8d1fafb4e] (Tienson Qin) [2025-12-25]: remove outdated datepicker component
  > 
- [eaa5a5733] (Tienson Qin) [2025-12-25]: remove file based api
  > 
- [5490824cf] (Tienson Qin) [2025-12-25]: remove old srs implementation
  > 
- [b3813ade7] (Tienson Qin) [2025-12-25]: remove file sync, tldraw, excalidraw and zotero
  > 
- [da06bc2db] (Tienson Qin) [2025-12-25]: wip: separate og and new version apps
  > 
- [fbe6b998b] (Charlie) [2025-12-25]: enhance(plugins): apis related improvements (#12266)
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/dependabot/npm_and_yarn/packages/ui/storybook-7.6.21
- [f6edd4340] (dependabot[bot]) [2025-12-29]: chore(deps-dev): bump storybook from 7.5.3 to 7.6.21 in /packages/ui
  > Bumps [storybook](https://github.com/storybookjs/storybook/tree/HEAD/code/core) from 7.5.3 to 7.6.21.
- [Release notes](https://github.com/storybookjs/storybook/releases)
- [Changelog](https://github.com/storybookjs/storybook/blob/next/CHANGELOG.md)
- [Commits](https://github.com/storybookjs/storybook/commits/v7.6.21/code/core)

---
updated-dependencies:
- dependency-name: storybook
  dependency-version: 7.6.21
  dependency-type: direct:development
...

Signed-off-by: dependabot[bot] <support@github.com>
- [bcc478b5f] (Tienson Qin) [2025-12-29]: refactor: separate og and db version (#12276)
  > separate og and new version apps

remove file sync, tldraw, excalidraw and zotero
- [5ff22217d] (charlie) [2025-12-29]: fix(ui): adjust padding for property configuration header
  > 
- [5f185a39c] (charlie) [2025-12-29]: chore(libs): upgrade libs core and yarn.lock
  > 
- [a2a5bfc09] (Tienson Qin) [2025-12-26]: remove temp workflow
  > 
- [c548542b4] (Tienson Qin) [2025-12-26]: add ci tool
  > 
- [3d78209a6] (charlie) [2025-12-26]: chore(libs):bump new version
  > 
- [fbe6b998b] (Charlie) [2025-12-25]: enhance(plugins): apis related improvements (#12266)
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/design-improvements
- [e7f2fbd40] (scheinriese) [2025-12-08]: Updated shortcut component on UI crash page
  > 

### Branch: upstream/enhance/add-crypt-key-iterations
- [7d7cdbdee] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [719a3fae7] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/enhance/assets-improvements
- [a3190eb0c] (charlie) [2025-12-08]: Merge branch 'master' into enhance/assets-improvements
  > 
- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [4fd34f64d] (charlie) [2025-12-08]: Merge branch 'master' into enhance/assets-improvements
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/enhance/zotero-import-test
- [4978f7216] (Mega Yu) [2025-12-15]: try to fix issue about zotero import
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/feat/bi-directional-properties
- [09319fea8] (Tienson Qin) [2026-01-05]: remove repeat definitions of ls-api-call!
  > 
- [b65c7aae1] (Tienson Qin) [2026-01-05]: add e2e tests for bidirectional properties
  > 
- [5d7efa624] (Tienson Qin) [2026-01-05]: fix: lint
  > 
- [a0c88221d] (Tienson Qin) [2026-01-05]: fix: lint
  > 
- [a5c7dfc03] (Tienson Qin) [2026-01-05]: cljs port of pluralize.js
  > 
- [f7cf6e4d7] (Tienson Qin) [2026-01-05]: enhance(ux): block collapse state should be isolated in container
  > 
- [611e865d0] (Tienson Qin) [2026-01-05]: fix: wrong effect
  > 
- [4f7c97f48] (Tienson Qin) [2026-01-05]: add enable-bidirectional? property to excludes
  > 
- [393596aa2] (Tienson Qin) [2026-01-05]: fix: lint
  > 
- [a3b8f640f] (Tienson Qin) [2026-01-05]: Update deps/db/src/logseq/db/frontend/property.cljs
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [b94e1cecc] (Tienson Qin) [2026-01-05]: add tests
  > 
- [4b67d31c6] (Tienson Qin) [2026-01-05]: enhance: able to edit property value of :string type
  > able to set plural form for tags

- [d2d52cd30] (Tienson Qin) [2026-01-05]: fix: async query bidirectional properties
  > 
- [4c8b78475] (Tienson Qin) [2026-01-05]: ux enhancements
  > 
- [16171cb2b] (Tienson Qin) [2026-01-05]: fix: bi-directional property should have classes specified
  > 
- [f19ad5df3] (Tienson Qin) [2026-01-05]: feat: Bi-directional property
  > 
- [6ba05ad2c] (Tienson Qin) [2026-01-05]: add e2e-test for isolated scope poroperties
  > 
- [79f5c6e3c] (Tienson Qin) [2026-01-05]: fix: tag-scoped properties still showing for other tags
  > 
- [28ba74628] (rcmerci) [2026-01-05]: chore(rtc): add fixme comment
  > 
- [1e61ae7dd] (rcmerci) [2026-01-05]: fix: lint
  > 
- [b82683357] (rcmerci) [2026-01-05]: dev: update agents.md
  > 
- [4dc4e1be7] (rcmerci) [2026-01-05]: enhance(rtc): add :add op to replace :move + :update combo
  > 
- [ae8710f76] (rcmerci) [2026-01-05]: dev: update review.md
  > 
- [41cbba368] (Tienson Qin) [2026-01-05]: enhance(ux): flashcards  (#12299)
  > Enhances the flashcard user experience by adding automatic query property management, fixing critical bugs, and improving the UI for managing card sets.

Key changes:

1. Automatic creation of query property blocks when tagging with #Query or subclasses via pipeline
2. Fixed critical bug in api-insert-new-block! where the end? parameter had inverted conditional logic
3. Added ability to create new #Cards blocks directly from the flashcard modal with a plus button
- [1cf4fa9ef] (Tienson Qin) [2026-01-05]: enhance(ux): add highlights to unlinked references
  > 
- [d1bbb5ad1] (charlie) [2026-01-05]: chore(libs): add development notes
  > 
- [2642765eb] (charlie) [2026-01-04]: chore: update db_properties_skill.md with enhanced property schema APIs and examples
  > 
- [a2bfb3963] (charlie) [2026-01-04]: chore: update version to 0.2.11 and add developer notes for Logseq DB properties
  > 
- [4b7d6c73c] (Tienson Qin) [2026-01-04]: fix: remove rtc test on updated-at
  > Since :block/updated-at should only be overwritten, not retracted

- [f940f708b] (Tienson Qin) [2026-01-04]: fix: rtc should never retract block timestamp
  > 
- [8c10cb64d] (Tienson Qin) [2026-01-04]: wrong ns
  > 
- [50e2bc53f] (Tienson Qin) [2026-01-04]: fix: filter out file graphs
  > 
- [a9a9905b0] (Tienson Qin) [2026-01-04]: feat: tag-scoped property choices (#12295)
  > * feat: tag-scoped property choices

* Able to hide global choices per tag

* add e2e tests
- [fd7e68582] (Tienson Qin) [2026-01-04]: chore: cleanup dicts
  > 
- [f43459f55] (Tienson Qin) [2026-01-04]: fix: wrong mobile event
  > 
- [e95c93629] (Tienson Qin) [2026-01-03]: fix: youtube embed on desktop
  > 
- [5705dd359] (Tienson Qin) [2026-01-03]: chore: code cleanup
  > 
- [ce5017fca] (charlie) [2026-01-03]: fix(tests): update tag properties in create tag test
  > 
- [9bd5390c0] (Tienson Qin) [2026-01-03]: enhance: don't close invite dialog
  > 
- [74d8308a6] (Tienson Qin) [2026-01-03]: chore: remove git commit command
  > 
- [0b77ee902] (Tienson Qin) [2026-01-03]: fix: trim blank for inviting email
  > 
- [baff8a4cc] (charlie) [2026-01-03]: enhance(apis): add support for tag properties in createTag API
  > 
- [664743b28] (Tienson Qin) [2026-01-03]: fix: logseq.Editor.renamePage HTTP API hangs indefinitely
  > related to https://github.com/logseq/db-test/issues/701

- [5459dbeb6] (Tienson Qin) [2026-01-03]: enhance: args for tag properties
  > 
- [8fd006bab] (Tienson Qin) [2026-01-03]: enhance: api createTag supports :tagProperties
  > 
- [ebb985f78] (Paweł Świątkowski) [2026-01-02]: Add ability to sort plugins by added date on marketplace (#11993)
  > Co-authored-by: Charlie <xyhp915@qq.com>
- [1cd561dbd] (Tienson Qin) [2026-01-01]: chore: CI cleanup
  > 
- [d4f4ad8f7] (Tienson Qin) [2026-01-01]: chore: bump version for mobile apps
  > 
- [324d02896] (Tienson Qin) [2026-01-01]: chore: remove rsapi from CI
  > 
- [44f26f91f] (Tienson Qin) [2026-01-01]: fix: add back ssl/keystore.jks for mobile development
  > 
- [497645082] (Tienson Qin) [2026-01-01]: fix: lint
  > 
- [51634b4c5] (Tienson Qin) [2026-01-01]: fix: collapse/expand toggle on mobile
  > 
- [4d46c5e9b] (Tienson Qin) [2026-01-01]: fix: youtube timestamp on web/electron
  > 
- [10846fdaa] (Tienson Qin) [2026-01-01]: fix: asset wrong order
  > 
- [7d493575d] (Tienson Qin) [2026-01-01]: fix: add missing datepicker.css
  > 
- [8fc093c1c] (Tienson Qin) [2026-01-01]: fix: can't set number 9 as property value
  > because it's empty-placeholder

- [44468dd20] (charlie) [2026-01-01]: enhance(apis): simplify tag resolution and enhance tag handling functions
  > 
- [5f42c51c3] (charlie) [2026-01-01]: enhance(property): add data attributes for property title and type
  > 
- [460eefb96] (charlie) [2026-01-01]: enhance(apis): add reset option to upsertBlockProperty for block properties
  > 
- [aafa50dcc] (Tienson Qin) [2026-01-01]: fix: :block/warning value should be keyword
  > 
- [6f59a02f5] (Tienson Qin) [2026-01-01]: fix: mark ensure-graph-uuid as deprecated
  > 
- [99dea608f] (rcmerci) [2026-01-01]: fix(ci): update typos.toml
  > 
- [ed1133095] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683 (2)
  > 
- [76224da1b] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683
  > 
- [0f949b739] (Tienson Qin) [2026-01-01]: fix: tests
  > 
- [b55550ed3] (Tienson Qin) [2026-01-01]: add :logseq.kv/local-graph-uuid
  > 
- [3078f2a90] (Tienson Qin) [2025-12-31]: chore: remove outdated test
  > 
- [15585f148] (Tienson Qin) [2025-12-31]: mod+m to publish
  > 
- [f1a410887] (rcmerci) [2025-12-31]: fix(rtc): fix invalid apply-ops msg
  > 
- [76ebafac6] (Tienson Qin) [2025-12-31]: enhance: preserve block collapsed state when publishing
  > 
- [5f532a6a8] (Gabriel Horner) [2025-12-30]: chore: update now outdated parts of db version section
  > 
- [a6503cc2f] (Gabriel Horner) [2025-12-30]: fix: add back validation of properties graph to CI
  > Accidentally taken out w/ last refactor

- [ca14110a8] (Gabriel Horner) [2025-12-30]: fix: nbb scripts failing to start
  > Caused by new dep that isn't configured for use with nbb.
Can add back when it's actually nbb compatible and has a nbb.edn

- [3b1433647] (Tienson Qin) [2025-12-30]: fix: set limit to passsword kdf iterations on cloudflare workers
  > 
- [e5964202d] (Tienson Qin) [2025-12-30]: Merge pull request #12279 from logseq/feat/page-publish
  > feat: page publish 
- [5f1695737] (Tienson Qin) [2025-12-30]: Merge branch 'master' into feat/page-publish
  > 
- [8cfcefcec] (charlie) [2025-12-30]: enhance(apis): add route handler and improve page block retrieval
  > 
- [5376779d4] (Tienson Qin) [2025-12-30]: deploy new version
  > 
- [8de2542f8] (Tienson Qin) [2025-12-30]: use salted PBKDF2 hash
  > 
- [f9a3fb50b] (Tienson Qin) [2025-12-30]: remove skip_auth
  > 
- [918dc0766] (Tienson Qin) [2025-12-30]: fix: set auth to be required on staging and prod
  > 
- [304195c7d] (Tienson Qin) [2025-12-30]: fix: typo
  > 
- [a0a7748e7] (Tienson Qin) [2025-12-30]: home page
  > 
- [adb7b8e36] (Tienson Qin) [2025-12-30]: fix: tagged nodes
  > 
- [dce1a8f4e] (Tienson Qin) [2025-12-30]: fix: e2e tests
  > 
- [6bc2fb1a0] (Tienson Qin) [2025-12-30]: fix: don't show published url property
  > 
- [45d592ecd] (Tienson Qin) [2025-12-30]: enhance: show close button when search box opened
  > 
- [c97453d1d] (Tienson Qin) [2025-12-30]: search
  > 
- [60e79785f] (Tienson Qin) [2025-12-30]: fix: add back missing dicts for flashcard buttons
  > 
- [c21f2c7f4] (Tienson Qin) [2025-12-30]: Ensure local graphs have uuid
  > 
- [644ef4c34] (Tienson Qin) [2025-12-30]: show authors
  > 
- [d53ea4a38] (Tienson Qin) [2025-12-30]: enhance: update published-url to be property type of :url
  > 
- [f8d494285] (Tienson Qin) [2025-12-30]: enhance: show page updated at
  > 
- [24c929835] (Tienson Qin) [2025-12-30]: fix: tests
  > 
- [12f967437] (Tienson Qin) [2025-12-30]: enhance: short url use "/p/short-id" instead of "/s/short-id"
  > 
- [74d136e49] (Tienson Qin) [2025-12-30]: fix: export tests
  > 
- [bdb6e0218] (Tienson Qin) [2025-12-30]: fix: remove icon from page properties area
  > 
- [8a415e007] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/publish.css
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [3e9786593] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/render.cljs
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [0ca329d63] (Tienson Qin) [2025-12-29]: add custom logseq/publish.css and logseq/publish.js
  > 
- [be69c009d] (Tienson Qin) [2025-12-29]: add user page
  > 
- [9dab647d0] (Tienson Qin) [2025-12-29]: add missing meta
  > 
- [da314220d] (Tienson Qin) [2025-12-29]: feat: unpublish
  > 
- [15d7526ac] (Tienson Qin) [2025-12-29]: fix: ast blocks in block title
  > 
- [2068123b2] (Tienson Qin) [2025-12-29]: fix: heading
  > 
- [0c79f4651] (Tienson Qin) [2025-12-29]: fix: cache published page
  > 
- [8c4be25e9] (Tienson Qin) [2025-12-29]: add timestamp version to invalid cache
  > 
- [7b7ec4706] (Tienson Qin) [2025-12-29]: add deploy script
  > 
- [efaff72f0] (Tienson Qin) [2025-12-29]: fix: mobile display issues
  > 
- [6c600d0a8] (Tienson Qin) [2025-12-29]: add dev script
  > 
- [8d6282121] (Tienson Qin) [2025-12-29]: add custom domain
  > 
- [ff56c6cbc] (Tienson Qin) [2025-12-29]: add publish staging and prod
  > 
- [7e695603d] (Tienson Qin) [2025-12-29]: Merge branch 'master' into feat/page-publish
  > 
- [bcc478b5f] (Tienson Qin) [2025-12-29]: refactor: separate og and db version (#12276)
  > separate og and new version apps

remove file sync, tldraw, excalidraw and zotero
- [56d3bd84d] (Tienson Qin) [2025-12-29]: add publish test
  > 
- [5ff22217d] (charlie) [2025-12-29]: fix(ui): adjust padding for property configuration header
  > 
- [5f185a39c] (charlie) [2025-12-29]: chore(libs): upgrade libs core and yarn.lock
  > 
- [62eb31c32] (Tienson Qin) [2025-12-29]: node embed support
  > 
- [78fbbaffa] (Tienson Qin) [2025-12-29]: fix: page icon
  > 
- [ef7cbd743] (Tienson Qin) [2025-12-29]: fix: url type property rendered as link
  > 
- [b1050da84] (Tienson Qin) [2025-12-29]: enhance: icon and tags position
  > 
- [5ed49d737] (Tienson Qin) [2025-12-29]: fix: positioned properties rendering
  > 
- [a04050d98] (Tienson Qin) [2025-12-29]: macro render
  > 
- [58f5a2ddc] (Tienson Qin) [2025-12-29]: block publish
  > 
- [3e033db0b] (Tienson Qin) [2025-12-29]: press `t o` to toggle collapse/expand
  > 
- [6d551e7b6] (Tienson Qin) [2025-12-29]: dark theme
  > 
- [db55b2d5c] (Tienson Qin) [2025-12-29]: style tweaks
  > 
- [a3d717812] (Tienson Qin) [2025-12-29]: multiple size variants for images
  > 
- [e77690b45] (Tienson Qin) [2025-12-28]: feat: password protected page
  > 
- [26c903f53] (Tienson Qin) [2025-12-28]: 404 page
  > 
- [0c668505c] (Tienson Qin) [2025-12-28]: friendly page for non published
  > 
- [326254c8f] (Tienson Qin) [2025-12-28]: style polish
  > 
- [12fabc63c] (Tienson Qin) [2025-12-28]: refactor: separate worker to multiple namespaces
  > 
- [55bf42887] (Tienson Qin) [2025-12-28]: code cleanup
  > 
- [4b2884abe] (Tienson Qin) [2025-12-28]: move inline style and script to publish.css and publish.js
  > 
- [b182a3736] (Tienson Qin) [2025-12-28]: fix: asset upload
  > 
- [cec45205d] (Tienson Qin) [2025-12-28]: wip: assets publish
  > 
- [aab7cfe2d] (Tienson Qin) [2025-12-28]: support math, code and quote blocks
  > 
- [80388f618] (Tienson Qin) [2025-12-28]: shorten url
  > 
- [0d38c982e] (Tienson Qin) [2025-12-28]: global ref
  > 
- [a682b9499] (Tienson Qin) [2025-12-28]: global tag
  > 
- [84cbb7931] (Tienson Qin) [2025-12-28]: tagged nodes
  > 
- [a4275838b] (Tienson Qin) [2025-12-28]: add graph page
  > 
- [08ab09a44] (Tienson Qin) [2025-12-28]: fix: property issues
  > 
- [7ad7e0b60] (Tienson Qin) [2025-12-28]: wip: properties publish
  > 
- [d4d8b1a7e] (Tienson Qin) [2025-12-28]: fix: add shadow-cljs.edn for publish worker
  > 
- [e9c2269e3] (Tienson Qin) [2025-12-28]: linked refs
  > 
- [36408416c] (Tienson Qin) [2025-12-27]: publish mvp
  > 
- [58cda5538] (Tienson Qin) [2025-12-27]: cljs worker
  > 
- [2a9624347] (Tienson Qin) [2025-12-27]: add publish worker
  > 
- [704312b40] (Tienson Qin) [2025-12-27]: Add publish dep
  > 
- [a2a5bfc09] (Tienson Qin) [2025-12-26]: remove temp workflow
  > 
- [c548542b4] (Tienson Qin) [2025-12-26]: add ci tool
  > 
- [7c7fe5e46] (Tienson Qin) [2025-12-26]: bump version to 2
  > 
- [8fe3cddba] (Tienson Qin) [2025-12-26]: enhance: limit access to db graphs only
  > 
- [8b312a403] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [678cb7dda] (Tienson Qin) [2025-12-26]: fix: init
  > 
- [910cd9514] (Tienson Qin) [2025-12-26]: fix: tests
  > 
- [c5c66aadc] (Tienson Qin) [2025-12-26]: update shadow-cljs.edn to remove tldraw and excalidraw
  > 
- [3d78209a6] (charlie) [2025-12-26]: chore(libs):bump new version
  > 
- [ec3ace5bd] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [cb9ec1d49] (Tienson Qin) [2025-12-26]: add sanitize-filename
  > 
- [c9a92f92b] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [c084a6b9d] (Tienson Qin) [2025-12-26]: fix dicts
  > 
- [ec7a325bc] (Tienson Qin) [2025-12-26]: more cleanup
  > 
- [f1de1ea16] (Tienson Qin) [2025-12-26]: remove git, chokidar, rsapi
  > 
- [467c5200d] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [7d719623a] (Tienson Qin) [2025-12-26]: refactor: prefer conn than repo for outliner core and rtc update
  > 
- [f6188b6cc] (Tienson Qin) [2025-12-25]: cleanup more config/db-based-graph? usage
  > 
- [eceaa7396] (Tienson Qin) [2025-12-25]: more cleanup
  > 
- [f419fbea8] (Tienson Qin) [2025-12-25]: cleanup
  > 
- [d8d6e8b4b] (Tienson Qin) [2025-12-25]: remove file based model
  > 
- [acbb03856] (Tienson Qin) [2025-12-25]: remove diff-merge
  > 
- [9f927137a] (Tienson Qin) [2025-12-25]: remove file based handlers
  > 
- [32cbac5a5] (Tienson Qin) [2025-12-25]: remove file based page handler
  > 
- [4aab5daca] (Tienson Qin) [2025-12-25]: add simplified datepicker
  > 
- [bbe6fa861] (Tienson Qin) [2025-12-25]: remove file based commands
  > 
- [8d1fafb4e] (Tienson Qin) [2025-12-25]: remove outdated datepicker component
  > 
- [eaa5a5733] (Tienson Qin) [2025-12-25]: remove file based api
  > 
- [5490824cf] (Tienson Qin) [2025-12-25]: remove old srs implementation
  > 
- [b3813ade7] (Tienson Qin) [2025-12-25]: remove file sync, tldraw, excalidraw and zotero
  > 
- [da06bc2db] (Tienson Qin) [2025-12-25]: wip: separate og and new version apps
  > 
- [fbe6b998b] (Charlie) [2025-12-25]: enhance(plugins): apis related improvements (#12266)
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/feat/cliable
- [cb019e9e5] (Tienson Qin) [2026-01-09]: remove source maps for testing
  > 
- [eb7e52257] (Tienson Qin) [2026-01-08]: disable sentry for any-branch test
  > 
- [b6ca3097d] (Tienson Qin) [2026-01-08]: add workflow to deploy any branch
  > 
- [562a68e0e] (rcmerci) [2026-01-08]: enhance(rtc): move rtc api schema to logseq/logseq-schema repo
  > 
- [c8148f66e] (charlie) [2026-01-07]: chore(libs): add Logseq plugin development agent guide
  > 
- [ffdb990d8] (charlie) [2026-01-07]: chore(libs): rename db_properties_notes to db_properties_guide and update README references
  > 
- [1a390a4a1] (rcmerci) [2026-01-06]: Merge pull request #12304 from logseq/debug/add-tx-log
  > feat(rtc): add debug-log tool for rtc
- [f7e1a3a6f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, only record rtc related tx-data
  > 
- [137e85beb] (rcmerci) [2026-01-06]: fix lint
  > 
- [e5668a469] (rcmerci) [2026-01-06]: fix lint
  > 
- [73888c0a9] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, update schema
  > 
- [1a33a365f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log
  > - integrate `debug-log-db` into GC process
- update schema with `repo` column and `DATETIME`
- export `reset_debug_log_db`

- [27016657f] (Tienson Qin) [2026-01-06]: add debug db tx-log and ws messages tables
  > 
- [6f513fa51] (Tienson Qin) [2026-01-06]: enhance(ux): make it clear that hourly backup is running on desktop
  > 
- [00ccff81e] (Tienson Qin) [2026-01-06]: more file-graph cleanup
  > 
- [23c6f8101] (charlie) [2026-01-06]: enhance(libs): add get_file_content function for retrieving file content
  > 
- [74bfe36d8] (charlie) [2026-01-06]: enhance(libs): update IDBProxy interface and improve documentation
  > 
- [8d7aaa791] (charlie) [2026-01-06]: enhance(apis): add setFileContent method for built-in file paths
  > 
- [6c1b9fe77] (Tienson Qin) [2026-01-06]: Add set_file_content api to update built-in files like custom.js/css
  > 
- [c2582d3c2] (Tienson Qin) [2026-01-06]: fix: available choices still shows other tags' scoped choices
  > 
- [289741069] (Tienson Qin) [2026-01-06]: tweak words for invalid db
  > 
- [574890505] (Gabriel Horner) [2026-01-05]: fix ref-property tests pointing to property rule
  > Also add back property tests as it is still a useful rule

- [6ba05ad2c] (Tienson Qin) [2026-01-05]: add e2e-test for isolated scope poroperties
  > 
- [79f5c6e3c] (Tienson Qin) [2026-01-05]: fix: tag-scoped properties still showing for other tags
  > 
- [28ba74628] (rcmerci) [2026-01-05]: chore(rtc): add fixme comment
  > 
- [1e61ae7dd] (rcmerci) [2026-01-05]: fix: lint
  > 
- [b82683357] (rcmerci) [2026-01-05]: dev: update agents.md
  > 
- [4dc4e1be7] (rcmerci) [2026-01-05]: enhance(rtc): add :add op to replace :move + :update combo
  > 
- [ae8710f76] (rcmerci) [2026-01-05]: dev: update review.md
  > 
- [41cbba368] (Tienson Qin) [2026-01-05]: enhance(ux): flashcards  (#12299)
  > Enhances the flashcard user experience by adding automatic query property management, fixing critical bugs, and improving the UI for managing card sets.

Key changes:

1. Automatic creation of query property blocks when tagging with #Query or subclasses via pipeline
2. Fixed critical bug in api-insert-new-block! where the end? parameter had inverted conditional logic
3. Added ability to create new #Cards blocks directly from the flashcard modal with a plus button
- [1cf4fa9ef] (Tienson Qin) [2026-01-05]: enhance(ux): add highlights to unlinked references
  > 
- [d1bbb5ad1] (charlie) [2026-01-05]: chore(libs): add development notes
  > 
- [2642765eb] (charlie) [2026-01-04]: chore: update db_properties_skill.md with enhanced property schema APIs and examples
  > 
- [a2bfb3963] (charlie) [2026-01-04]: chore: update version to 0.2.11 and add developer notes for Logseq DB properties
  > 
- [4b7d6c73c] (Tienson Qin) [2026-01-04]: fix: remove rtc test on updated-at
  > Since :block/updated-at should only be overwritten, not retracted

- [f940f708b] (Tienson Qin) [2026-01-04]: fix: rtc should never retract block timestamp
  > 
- [8c10cb64d] (Tienson Qin) [2026-01-04]: wrong ns
  > 
- [50e2bc53f] (Tienson Qin) [2026-01-04]: fix: filter out file graphs
  > 
- [a9a9905b0] (Tienson Qin) [2026-01-04]: feat: tag-scoped property choices (#12295)
  > * feat: tag-scoped property choices

* Able to hide global choices per tag

* add e2e tests
- [fd7e68582] (Tienson Qin) [2026-01-04]: chore: cleanup dicts
  > 
- [f43459f55] (Tienson Qin) [2026-01-04]: fix: wrong mobile event
  > 
- [e95c93629] (Tienson Qin) [2026-01-03]: fix: youtube embed on desktop
  > 
- [5705dd359] (Tienson Qin) [2026-01-03]: chore: code cleanup
  > 
- [ce5017fca] (charlie) [2026-01-03]: fix(tests): update tag properties in create tag test
  > 
- [9bd5390c0] (Tienson Qin) [2026-01-03]: enhance: don't close invite dialog
  > 
- [74d8308a6] (Tienson Qin) [2026-01-03]: chore: remove git commit command
  > 
- [0b77ee902] (Tienson Qin) [2026-01-03]: fix: trim blank for inviting email
  > 
- [baff8a4cc] (charlie) [2026-01-03]: enhance(apis): add support for tag properties in createTag API
  > 
- [664743b28] (Tienson Qin) [2026-01-03]: fix: logseq.Editor.renamePage HTTP API hangs indefinitely
  > related to https://github.com/logseq/db-test/issues/701

- [5459dbeb6] (Tienson Qin) [2026-01-03]: enhance: args for tag properties
  > 
- [8fd006bab] (Tienson Qin) [2026-01-03]: enhance: api createTag supports :tagProperties
  > 
- [ebb985f78] (Paweł Świątkowski) [2026-01-02]: Add ability to sort plugins by added date on marketplace (#11993)
  > Co-authored-by: Charlie <xyhp915@qq.com>
- [1cd561dbd] (Tienson Qin) [2026-01-01]: chore: CI cleanup
  > 
- [d4f4ad8f7] (Tienson Qin) [2026-01-01]: chore: bump version for mobile apps
  > 
- [324d02896] (Tienson Qin) [2026-01-01]: chore: remove rsapi from CI
  > 
- [44f26f91f] (Tienson Qin) [2026-01-01]: fix: add back ssl/keystore.jks for mobile development
  > 
- [497645082] (Tienson Qin) [2026-01-01]: fix: lint
  > 
- [51634b4c5] (Tienson Qin) [2026-01-01]: fix: collapse/expand toggle on mobile
  > 
- [4d46c5e9b] (Tienson Qin) [2026-01-01]: fix: youtube timestamp on web/electron
  > 
- [10846fdaa] (Tienson Qin) [2026-01-01]: fix: asset wrong order
  > 
- [7d493575d] (Tienson Qin) [2026-01-01]: fix: add missing datepicker.css
  > 
- [8fc093c1c] (Tienson Qin) [2026-01-01]: fix: can't set number 9 as property value
  > because it's empty-placeholder

- [44468dd20] (charlie) [2026-01-01]: enhance(apis): simplify tag resolution and enhance tag handling functions
  > 
- [5f42c51c3] (charlie) [2026-01-01]: enhance(property): add data attributes for property title and type
  > 
- [460eefb96] (charlie) [2026-01-01]: enhance(apis): add reset option to upsertBlockProperty for block properties
  > 
- [aafa50dcc] (Tienson Qin) [2026-01-01]: fix: :block/warning value should be keyword
  > 
- [6f59a02f5] (Tienson Qin) [2026-01-01]: fix: mark ensure-graph-uuid as deprecated
  > 
- [99dea608f] (rcmerci) [2026-01-01]: fix(ci): update typos.toml
  > 
- [ed1133095] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683 (2)
  > 
- [76224da1b] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683
  > 
- [0f949b739] (Tienson Qin) [2026-01-01]: fix: tests
  > 
- [b55550ed3] (Tienson Qin) [2026-01-01]: add :logseq.kv/local-graph-uuid
  > 
- [3078f2a90] (Tienson Qin) [2025-12-31]: chore: remove outdated test
  > 
- [15585f148] (Tienson Qin) [2025-12-31]: mod+m to publish
  > 
- [f1a410887] (rcmerci) [2025-12-31]: fix(rtc): fix invalid apply-ops msg
  > 
- [76ebafac6] (Tienson Qin) [2025-12-31]: enhance: preserve block collapsed state when publishing
  > 
- [5f532a6a8] (Gabriel Horner) [2025-12-30]: chore: update now outdated parts of db version section
  > 
- [a6503cc2f] (Gabriel Horner) [2025-12-30]: fix: add back validation of properties graph to CI
  > Accidentally taken out w/ last refactor

- [ca14110a8] (Gabriel Horner) [2025-12-30]: fix: nbb scripts failing to start
  > Caused by new dep that isn't configured for use with nbb.
Can add back when it's actually nbb compatible and has a nbb.edn

- [3b1433647] (Tienson Qin) [2025-12-30]: fix: set limit to passsword kdf iterations on cloudflare workers
  > 
- [e5964202d] (Tienson Qin) [2025-12-30]: Merge pull request #12279 from logseq/feat/page-publish
  > feat: page publish 
- [5f1695737] (Tienson Qin) [2025-12-30]: Merge branch 'master' into feat/page-publish
  > 
- [8cfcefcec] (charlie) [2025-12-30]: enhance(apis): add route handler and improve page block retrieval
  > 
- [5376779d4] (Tienson Qin) [2025-12-30]: deploy new version
  > 
- [8de2542f8] (Tienson Qin) [2025-12-30]: use salted PBKDF2 hash
  > 
- [f9a3fb50b] (Tienson Qin) [2025-12-30]: remove skip_auth
  > 
- [918dc0766] (Tienson Qin) [2025-12-30]: fix: set auth to be required on staging and prod
  > 
- [304195c7d] (Tienson Qin) [2025-12-30]: fix: typo
  > 
- [a0a7748e7] (Tienson Qin) [2025-12-30]: home page
  > 
- [adb7b8e36] (Tienson Qin) [2025-12-30]: fix: tagged nodes
  > 
- [dce1a8f4e] (Tienson Qin) [2025-12-30]: fix: e2e tests
  > 
- [6bc2fb1a0] (Tienson Qin) [2025-12-30]: fix: don't show published url property
  > 
- [45d592ecd] (Tienson Qin) [2025-12-30]: enhance: show close button when search box opened
  > 
- [c97453d1d] (Tienson Qin) [2025-12-30]: search
  > 
- [60e79785f] (Tienson Qin) [2025-12-30]: fix: add back missing dicts for flashcard buttons
  > 
- [c21f2c7f4] (Tienson Qin) [2025-12-30]: Ensure local graphs have uuid
  > 
- [644ef4c34] (Tienson Qin) [2025-12-30]: show authors
  > 
- [d53ea4a38] (Tienson Qin) [2025-12-30]: enhance: update published-url to be property type of :url
  > 
- [f8d494285] (Tienson Qin) [2025-12-30]: enhance: show page updated at
  > 
- [24c929835] (Tienson Qin) [2025-12-30]: fix: tests
  > 
- [12f967437] (Tienson Qin) [2025-12-30]: enhance: short url use "/p/short-id" instead of "/s/short-id"
  > 
- [74d136e49] (Tienson Qin) [2025-12-30]: fix: export tests
  > 
- [bdb6e0218] (Tienson Qin) [2025-12-30]: fix: remove icon from page properties area
  > 
- [8a415e007] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/publish.css
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [3e9786593] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/render.cljs
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [0ca329d63] (Tienson Qin) [2025-12-29]: add custom logseq/publish.css and logseq/publish.js
  > 
- [be69c009d] (Tienson Qin) [2025-12-29]: add user page
  > 
- [9dab647d0] (Tienson Qin) [2025-12-29]: add missing meta
  > 
- [da314220d] (Tienson Qin) [2025-12-29]: feat: unpublish
  > 
- [15d7526ac] (Tienson Qin) [2025-12-29]: fix: ast blocks in block title
  > 
- [2068123b2] (Tienson Qin) [2025-12-29]: fix: heading
  > 
- [0c79f4651] (Tienson Qin) [2025-12-29]: fix: cache published page
  > 
- [8c4be25e9] (Tienson Qin) [2025-12-29]: add timestamp version to invalid cache
  > 
- [7b7ec4706] (Tienson Qin) [2025-12-29]: add deploy script
  > 
- [efaff72f0] (Tienson Qin) [2025-12-29]: fix: mobile display issues
  > 
- [6c600d0a8] (Tienson Qin) [2025-12-29]: add dev script
  > 
- [8d6282121] (Tienson Qin) [2025-12-29]: add custom domain
  > 
- [ff56c6cbc] (Tienson Qin) [2025-12-29]: add publish staging and prod
  > 
- [7e695603d] (Tienson Qin) [2025-12-29]: Merge branch 'master' into feat/page-publish
  > 
- [bcc478b5f] (Tienson Qin) [2025-12-29]: refactor: separate og and db version (#12276)
  > separate og and new version apps

remove file sync, tldraw, excalidraw and zotero
- [56d3bd84d] (Tienson Qin) [2025-12-29]: add publish test
  > 
- [5ff22217d] (charlie) [2025-12-29]: fix(ui): adjust padding for property configuration header
  > 
- [5f185a39c] (charlie) [2025-12-29]: chore(libs): upgrade libs core and yarn.lock
  > 
- [62eb31c32] (Tienson Qin) [2025-12-29]: node embed support
  > 
- [78fbbaffa] (Tienson Qin) [2025-12-29]: fix: page icon
  > 
- [ef7cbd743] (Tienson Qin) [2025-12-29]: fix: url type property rendered as link
  > 
- [b1050da84] (Tienson Qin) [2025-12-29]: enhance: icon and tags position
  > 
- [5ed49d737] (Tienson Qin) [2025-12-29]: fix: positioned properties rendering
  > 
- [a04050d98] (Tienson Qin) [2025-12-29]: macro render
  > 
- [58f5a2ddc] (Tienson Qin) [2025-12-29]: block publish
  > 
- [3e033db0b] (Tienson Qin) [2025-12-29]: press `t o` to toggle collapse/expand
  > 
- [6d551e7b6] (Tienson Qin) [2025-12-29]: dark theme
  > 
- [db55b2d5c] (Tienson Qin) [2025-12-29]: style tweaks
  > 
- [a3d717812] (Tienson Qin) [2025-12-29]: multiple size variants for images
  > 
- [e77690b45] (Tienson Qin) [2025-12-28]: feat: password protected page
  > 
- [26c903f53] (Tienson Qin) [2025-12-28]: 404 page
  > 
- [0c668505c] (Tienson Qin) [2025-12-28]: friendly page for non published
  > 
- [326254c8f] (Tienson Qin) [2025-12-28]: style polish
  > 
- [12fabc63c] (Tienson Qin) [2025-12-28]: refactor: separate worker to multiple namespaces
  > 
- [55bf42887] (Tienson Qin) [2025-12-28]: code cleanup
  > 
- [4b2884abe] (Tienson Qin) [2025-12-28]: move inline style and script to publish.css and publish.js
  > 
- [b182a3736] (Tienson Qin) [2025-12-28]: fix: asset upload
  > 
- [cec45205d] (Tienson Qin) [2025-12-28]: wip: assets publish
  > 
- [aab7cfe2d] (Tienson Qin) [2025-12-28]: support math, code and quote blocks
  > 
- [80388f618] (Tienson Qin) [2025-12-28]: shorten url
  > 
- [0d38c982e] (Tienson Qin) [2025-12-28]: global ref
  > 
- [a682b9499] (Tienson Qin) [2025-12-28]: global tag
  > 
- [84cbb7931] (Tienson Qin) [2025-12-28]: tagged nodes
  > 
- [a4275838b] (Tienson Qin) [2025-12-28]: add graph page
  > 
- [08ab09a44] (Tienson Qin) [2025-12-28]: fix: property issues
  > 
- [7ad7e0b60] (Tienson Qin) [2025-12-28]: wip: properties publish
  > 
- [d4d8b1a7e] (Tienson Qin) [2025-12-28]: fix: add shadow-cljs.edn for publish worker
  > 
- [e9c2269e3] (Tienson Qin) [2025-12-28]: linked refs
  > 
- [36408416c] (Tienson Qin) [2025-12-27]: publish mvp
  > 
- [58cda5538] (Tienson Qin) [2025-12-27]: cljs worker
  > 
- [2a9624347] (Tienson Qin) [2025-12-27]: add publish worker
  > 
- [704312b40] (Tienson Qin) [2025-12-27]: Add publish dep
  > 
- [a2a5bfc09] (Tienson Qin) [2025-12-26]: remove temp workflow
  > 
- [c548542b4] (Tienson Qin) [2025-12-26]: add ci tool
  > 
- [7c7fe5e46] (Tienson Qin) [2025-12-26]: bump version to 2
  > 
- [8fe3cddba] (Tienson Qin) [2025-12-26]: enhance: limit access to db graphs only
  > 
- [8b312a403] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [678cb7dda] (Tienson Qin) [2025-12-26]: fix: init
  > 
- [910cd9514] (Tienson Qin) [2025-12-26]: fix: tests
  > 
- [c5c66aadc] (Tienson Qin) [2025-12-26]: update shadow-cljs.edn to remove tldraw and excalidraw
  > 
- [3d78209a6] (charlie) [2025-12-26]: chore(libs):bump new version
  > 
- [ec3ace5bd] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [cb9ec1d49] (Tienson Qin) [2025-12-26]: add sanitize-filename
  > 
- [c9a92f92b] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [c084a6b9d] (Tienson Qin) [2025-12-26]: fix dicts
  > 
- [ec7a325bc] (Tienson Qin) [2025-12-26]: more cleanup
  > 
- [f1de1ea16] (Tienson Qin) [2025-12-26]: remove git, chokidar, rsapi
  > 
- [467c5200d] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [7d719623a] (Tienson Qin) [2025-12-26]: refactor: prefer conn than repo for outliner core and rtc update
  > 
- [f6188b6cc] (Tienson Qin) [2025-12-25]: cleanup more config/db-based-graph? usage
  > 
- [eceaa7396] (Tienson Qin) [2025-12-25]: more cleanup
  > 
- [f419fbea8] (Tienson Qin) [2025-12-25]: cleanup
  > 
- [d8d6e8b4b] (Tienson Qin) [2025-12-25]: remove file based model
  > 
- [acbb03856] (Tienson Qin) [2025-12-25]: remove diff-merge
  > 
- [9f927137a] (Tienson Qin) [2025-12-25]: remove file based handlers
  > 
- [32cbac5a5] (Tienson Qin) [2025-12-25]: remove file based page handler
  > 
- [4aab5daca] (Tienson Qin) [2025-12-25]: add simplified datepicker
  > 
- [bbe6fa861] (Tienson Qin) [2025-12-25]: remove file based commands
  > 
- [8d1fafb4e] (Tienson Qin) [2025-12-25]: remove outdated datepicker component
  > 
- [eaa5a5733] (Tienson Qin) [2025-12-25]: remove file based api
  > 
- [5490824cf] (Tienson Qin) [2025-12-25]: remove old srs implementation
  > 
- [b3813ade7] (Tienson Qin) [2025-12-25]: remove file sync, tldraw, excalidraw and zotero
  > 
- [da06bc2db] (Tienson Qin) [2025-12-25]: wip: separate og and new version apps
  > 
- [fbe6b998b] (Charlie) [2025-12-25]: enhance(plugins): apis related improvements (#12266)
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/feat/multi-nav-stacks
- [6d167fe67] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [768b85a2c] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 

### Branch: upstream/feat/worker-sync
- [88d1b5622] (rcmerci) [2026-01-12]: doc(db-sync): update fix-blocks-cycle.md
  > 
- [935678216] (rcmerci) [2026-01-11]: fix(db-sync): worker server unit-tests
  > 
- [10e368fb3] (rcmerci) [2026-01-11]: chore: fmt
  > 
- [6e2cae24b] (rcmerci) [2026-01-11]: enhance(db-sync): ensure only one ws per repo
  > 
- [b949f707e] (rcmerci) [2026-01-11]: fix(db-sync): recv 'changed' when in tx/batch-wait
  > 
- [410d4a27f] (rcmerci) [2026-01-11]: dev(db-sync): add db-sync debug icon in header
  > 
- [f23ab5c5b] (rcmerci) [2026-01-11]: refactor(db-sync): add state machine doc & refactor
  > 
- [b7e926f71] (rcmerci) [2026-01-11]: enhance(db-sync): remove declare
  > 
- [2bc5783b7] (rcmerci) [2026-01-11]: enhance(db-sync): update doc and align with codebase
  > 
- [611a52443] (rcmerci) [2026-01-11]: enhance(db-sync): fail fast when encounter bugs
  > 
- [32945f644] (rcmerci) [2026-01-11]: fix(db-sync): fix blocks-cycle, add doc docs/agent-guide/db-sync/fix-blocks-cycle.md
  > 
- [4cbb01f78] (rcmerci) [2026-01-11]: fix(db-sync): fix test
  > 
- [8fbcf1686] (rcmerci) [2026-01-11]: fix(db-sync): wrong ws resp when tx/reject
  > 
- [b0309307f] (Tienson Qin) [2026-01-11]: wip: missing parent && order conflicts
  > 
- [f8daccba4] (Tienson Qin) [2026-01-11]: fix: fn order
  > 
- [1b79d7d0d] (rcmerci) [2026-01-10]: doc(db-sync): sync db-sync docs and codebase again
  > 
- [fcc24fff2] (rcmerci) [2026-01-10]: feat(db-sync): add schemas for all client&server ws&http requests&responses
  > 
- [a32f46fb6] (rcmerci) [2026-01-10]: fix(db-sync): dev:db-sync-test only compile
  > 
- [9f530799b] (rcmerci) [2026-01-10]: enhance(db-sync): protocol.md add http endpoints
  > fix: make t_before mandatory

- [0864b485f] (rcmerci) [2026-01-10]: refactor(db-sync): rename worker-sync to db-sync
  > 
- [cdd380bdc] (Tienson Qin) [2026-01-10]: use promesa instead of js-await
  > 
- [5219d928d] (rcmerci) [2026-01-10]: docs(worker-sync): add guide and protocol
  > 
- [c06a63b03] (Tienson Qin) [2026-01-10]: enable db validation on worker
  > 
- [660616208] (Tienson Qin) [2026-01-10]: fix: property db attrs need to be at the beginning of tx-data
  > 
- [7aee601c3] (Tienson Qin) [2026-01-10]: no need for de-normalize
  > 
- [2dde10531] (Tienson Qin) [2026-01-10]: move retract handler to normalize
  > 
- [3fbd1c842] (Tienson Qin) [2026-01-10]: fix: set local tx when downloading graph
  > 
- [7bbc5af84] (Tienson Qin) [2026-01-10]: fix: replace :db/retract datoms with :db.fn/retractEntity
  > 
- [52bac8aef] (rcmerci) [2026-01-10]: enhance(worker-sync): add ws reconnect backoff
  > 
- [7d2d098cf] (Tienson Qin) [2026-01-09]: fix: wrong pipeline data when applying remote tx
  > 
- [e20319852] (Tienson Qin) [2026-01-09]: server apply multiple txs at once
  > 
- [96da11607] (Tienson Qin) [2026-01-09]: fix: user persisted local-tx
  > 
- [a54ecc7d2] (rcmerci) [2026-01-09]: docs(worker-sync): add accuracy note
  > 
- [01fc791cd] (rcmerci) [2026-01-09]: docs(worker-sync): expand dev start note
  > 
- [c2738c9dc] (rcmerci) [2026-01-09]: enhance(worker-sync): foreground dev task
  > 
- [965f93de7] (rcmerci) [2026-01-09]: update agents.md and docs/agent-guide/worker-sync.md
  > 
- [cb032ea14] (Tienson Qin) [2026-01-09]: remove tx api, use tx/batch instead
  > 
- [a217cdede] (Tienson Qin) [2026-01-09]: enable client db validation
  > 
- [5a05b5f10] (Tienson Qin) [2026-01-09]: Add head request
  > 
- [1e8c89d60] (Tienson Qin) [2026-01-09]: use staging ws url
  > 
- [b9d6f472c] (Tienson Qin) [2026-01-09]: authorization
  > 
- [582b741b6] (Tienson Qin) [2026-01-09]: use wss
  > 
- [18fb335c4] (Tienson Qin) [2026-01-08]: Merge branch 'master' into feat/worker-sync
  > 
- [b6ca3097d] (Tienson Qin) [2026-01-08]: add workflow to deploy any branch
  > 
- [75575bd39] (Tienson Qin) [2026-01-08]: clean ns
  > 
- [2c6723db3] (Tienson Qin) [2026-01-08]: remove debug datoms call
  > 
- [887c0cc48] (Tienson Qin) [2026-01-08]: disable snapshot as it's memory consume
  > 
- [e8f2d2d7e] (Tienson Qin) [2026-01-08]: increase download batch size
  > 
- [0be52e538] (Tienson Qin) [2026-01-08]: increase upload batch size
  > 
- [996299e50] (Tienson Qin) [2026-01-08]: add staging for worker sync
  > 
- [49c68d7d4] (Tienson Qin) [2026-01-08]: fix: close other ws conns when starting new
  > 
- [274e7b63e] (Tienson Qin) [2026-01-08]: fix: both upload and download graph
  > tested for 4k movies

- [562a68e0e] (rcmerci) [2026-01-08]: enhance(rtc): move rtc api schema to logseq/logseq-schema repo
  > 
- [5a8920013] (Tienson Qin) [2026-01-08]: fix: download
  > 
- [fc4351c71] (Tienson Qin) [2026-01-08]: fix: import
  > 
- [e89fdb92b] (Tienson Qin) [2026-01-08]: download use sqlite rows
  > 
- [dcff00759] (Tienson Qin) [2026-01-08]: fix: prefer server graph id
  > 
- [402d21425] (Tienson Qin) [2026-01-08]: switch to sqlite table rows upload
  > 
- [36488b5a2] (Tienson Qin) [2026-01-08]: upload graph
  > 
- [4b572a4fe] (Tienson Qin) [2026-01-08]: asset
  > 
- [b8ff7b526] (Tienson Qin) [2026-01-08]: re-use normalization fns
  > 
- [18f7a1c32] (Tienson Qin) [2026-01-08]: separate for tests
  > 
- [9dd574550] (Tienson Qin) [2026-01-08]: fix: parent cycle
  > 
- [7fc898df9] (Tienson Qin) [2026-01-08]: handle special cases
  > 
- [81a13d8c1] (Tienson Qin) [2026-01-08]: fix: no need to pull periodically
  > 
- [c00307dc0] (Tienson Qin) [2026-01-07]: fix: able to sync
  > 
- [4670a67bd] (Tienson Qin) [2026-01-07]: able to download remote graph
  > 
- [0c8967919] (Tienson Qin) [2026-01-07]: fix: ws route
  > 
- [b08860bf3] (Tienson Qin) [2026-01-07]: fix: http api
  > 
- [93269f83f] (Tienson Qin) [2026-01-07]: initial sync graph
  > 
- [98be903d5] (Tienson Qin) [2026-01-07]: fix: persist local txs
  > 
- [6f1ec3229] (Tienson Qin) [2026-01-07]: fix: http path
  > 
- [3090074ff] (Tienson Qin) [2026-01-07]: handle 3 cases
  > 
- [f1d15f5c6] (Tienson Qin) [2026-01-07]: integrate worker sync
  > 
- [2b56aa9b5] (Tienson Qin) [2026-01-07]: initial impl of sync with cloudflare DO
  > 
- [c8148f66e] (charlie) [2026-01-07]: chore(libs): add Logseq plugin development agent guide
  > 
- [ffdb990d8] (charlie) [2026-01-07]: chore(libs): rename db_properties_notes to db_properties_guide and update README references
  > 
- [1a390a4a1] (rcmerci) [2026-01-06]: Merge pull request #12304 from logseq/debug/add-tx-log
  > feat(rtc): add debug-log tool for rtc
- [f7e1a3a6f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, only record rtc related tx-data
  > 
- [137e85beb] (rcmerci) [2026-01-06]: fix lint
  > 
- [e5668a469] (rcmerci) [2026-01-06]: fix lint
  > 
- [73888c0a9] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, update schema
  > 
- [1a33a365f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log
  > - integrate `debug-log-db` into GC process
- update schema with `repo` column and `DATETIME`
- export `reset_debug_log_db`

- [27016657f] (Tienson Qin) [2026-01-06]: add debug db tx-log and ws messages tables
  > 
- [6f513fa51] (Tienson Qin) [2026-01-06]: enhance(ux): make it clear that hourly backup is running on desktop
  > 
- [00ccff81e] (Tienson Qin) [2026-01-06]: more file-graph cleanup
  > 
- [23c6f8101] (charlie) [2026-01-06]: enhance(libs): add get_file_content function for retrieving file content
  > 
- [74bfe36d8] (charlie) [2026-01-06]: enhance(libs): update IDBProxy interface and improve documentation
  > 
- [8d7aaa791] (charlie) [2026-01-06]: enhance(apis): add setFileContent method for built-in file paths
  > 
- [6c1b9fe77] (Tienson Qin) [2026-01-06]: Add set_file_content api to update built-in files like custom.js/css
  > 
- [c2582d3c2] (Tienson Qin) [2026-01-06]: fix: available choices still shows other tags' scoped choices
  > 
- [289741069] (Tienson Qin) [2026-01-06]: tweak words for invalid db
  > 
- [574890505] (Gabriel Horner) [2026-01-05]: fix ref-property tests pointing to property rule
  > Also add back property tests as it is still a useful rule

- [6ba05ad2c] (Tienson Qin) [2026-01-05]: add e2e-test for isolated scope poroperties
  > 
- [79f5c6e3c] (Tienson Qin) [2026-01-05]: fix: tag-scoped properties still showing for other tags
  > 
- [28ba74628] (rcmerci) [2026-01-05]: chore(rtc): add fixme comment
  > 
- [1e61ae7dd] (rcmerci) [2026-01-05]: fix: lint
  > 
- [b82683357] (rcmerci) [2026-01-05]: dev: update agents.md
  > 
- [4dc4e1be7] (rcmerci) [2026-01-05]: enhance(rtc): add :add op to replace :move + :update combo
  > 
- [ae8710f76] (rcmerci) [2026-01-05]: dev: update review.md
  > 
- [41cbba368] (Tienson Qin) [2026-01-05]: enhance(ux): flashcards  (#12299)
  > Enhances the flashcard user experience by adding automatic query property management, fixing critical bugs, and improving the UI for managing card sets.

Key changes:

1. Automatic creation of query property blocks when tagging with #Query or subclasses via pipeline
2. Fixed critical bug in api-insert-new-block! where the end? parameter had inverted conditional logic
3. Added ability to create new #Cards blocks directly from the flashcard modal with a plus button
- [1cf4fa9ef] (Tienson Qin) [2026-01-05]: enhance(ux): add highlights to unlinked references
  > 
- [d1bbb5ad1] (charlie) [2026-01-05]: chore(libs): add development notes
  > 
- [2642765eb] (charlie) [2026-01-04]: chore: update db_properties_skill.md with enhanced property schema APIs and examples
  > 
- [a2bfb3963] (charlie) [2026-01-04]: chore: update version to 0.2.11 and add developer notes for Logseq DB properties
  > 
- [4b7d6c73c] (Tienson Qin) [2026-01-04]: fix: remove rtc test on updated-at
  > Since :block/updated-at should only be overwritten, not retracted

- [f940f708b] (Tienson Qin) [2026-01-04]: fix: rtc should never retract block timestamp
  > 
- [8c10cb64d] (Tienson Qin) [2026-01-04]: wrong ns
  > 
- [50e2bc53f] (Tienson Qin) [2026-01-04]: fix: filter out file graphs
  > 
- [a9a9905b0] (Tienson Qin) [2026-01-04]: feat: tag-scoped property choices (#12295)
  > * feat: tag-scoped property choices

* Able to hide global choices per tag

* add e2e tests
- [fd7e68582] (Tienson Qin) [2026-01-04]: chore: cleanup dicts
  > 
- [f43459f55] (Tienson Qin) [2026-01-04]: fix: wrong mobile event
  > 
- [e95c93629] (Tienson Qin) [2026-01-03]: fix: youtube embed on desktop
  > 
- [5705dd359] (Tienson Qin) [2026-01-03]: chore: code cleanup
  > 
- [ce5017fca] (charlie) [2026-01-03]: fix(tests): update tag properties in create tag test
  > 
- [9bd5390c0] (Tienson Qin) [2026-01-03]: enhance: don't close invite dialog
  > 
- [74d8308a6] (Tienson Qin) [2026-01-03]: chore: remove git commit command
  > 
- [0b77ee902] (Tienson Qin) [2026-01-03]: fix: trim blank for inviting email
  > 
- [baff8a4cc] (charlie) [2026-01-03]: enhance(apis): add support for tag properties in createTag API
  > 
- [664743b28] (Tienson Qin) [2026-01-03]: fix: logseq.Editor.renamePage HTTP API hangs indefinitely
  > related to https://github.com/logseq/db-test/issues/701

- [5459dbeb6] (Tienson Qin) [2026-01-03]: enhance: args for tag properties
  > 
- [8fd006bab] (Tienson Qin) [2026-01-03]: enhance: api createTag supports :tagProperties
  > 
- [ebb985f78] (Paweł Świątkowski) [2026-01-02]: Add ability to sort plugins by added date on marketplace (#11993)
  > Co-authored-by: Charlie <xyhp915@qq.com>
- [1cd561dbd] (Tienson Qin) [2026-01-01]: chore: CI cleanup
  > 
- [d4f4ad8f7] (Tienson Qin) [2026-01-01]: chore: bump version for mobile apps
  > 
- [324d02896] (Tienson Qin) [2026-01-01]: chore: remove rsapi from CI
  > 
- [44f26f91f] (Tienson Qin) [2026-01-01]: fix: add back ssl/keystore.jks for mobile development
  > 
- [497645082] (Tienson Qin) [2026-01-01]: fix: lint
  > 
- [51634b4c5] (Tienson Qin) [2026-01-01]: fix: collapse/expand toggle on mobile
  > 
- [4d46c5e9b] (Tienson Qin) [2026-01-01]: fix: youtube timestamp on web/electron
  > 
- [10846fdaa] (Tienson Qin) [2026-01-01]: fix: asset wrong order
  > 
- [7d493575d] (Tienson Qin) [2026-01-01]: fix: add missing datepicker.css
  > 
- [8fc093c1c] (Tienson Qin) [2026-01-01]: fix: can't set number 9 as property value
  > because it's empty-placeholder

- [44468dd20] (charlie) [2026-01-01]: enhance(apis): simplify tag resolution and enhance tag handling functions
  > 
- [5f42c51c3] (charlie) [2026-01-01]: enhance(property): add data attributes for property title and type
  > 
- [460eefb96] (charlie) [2026-01-01]: enhance(apis): add reset option to upsertBlockProperty for block properties
  > 
- [aafa50dcc] (Tienson Qin) [2026-01-01]: fix: :block/warning value should be keyword
  > 
- [6f59a02f5] (Tienson Qin) [2026-01-01]: fix: mark ensure-graph-uuid as deprecated
  > 
- [99dea608f] (rcmerci) [2026-01-01]: fix(ci): update typos.toml
  > 
- [ed1133095] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683 (2)
  > 
- [76224da1b] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683
  > 
- [0f949b739] (Tienson Qin) [2026-01-01]: fix: tests
  > 
- [b55550ed3] (Tienson Qin) [2026-01-01]: add :logseq.kv/local-graph-uuid
  > 
- [3078f2a90] (Tienson Qin) [2025-12-31]: chore: remove outdated test
  > 
- [15585f148] (Tienson Qin) [2025-12-31]: mod+m to publish
  > 
- [f1a410887] (rcmerci) [2025-12-31]: fix(rtc): fix invalid apply-ops msg
  > 
- [76ebafac6] (Tienson Qin) [2025-12-31]: enhance: preserve block collapsed state when publishing
  > 
- [5f532a6a8] (Gabriel Horner) [2025-12-30]: chore: update now outdated parts of db version section
  > 
- [a6503cc2f] (Gabriel Horner) [2025-12-30]: fix: add back validation of properties graph to CI
  > Accidentally taken out w/ last refactor

- [ca14110a8] (Gabriel Horner) [2025-12-30]: fix: nbb scripts failing to start
  > Caused by new dep that isn't configured for use with nbb.
Can add back when it's actually nbb compatible and has a nbb.edn

- [3b1433647] (Tienson Qin) [2025-12-30]: fix: set limit to passsword kdf iterations on cloudflare workers
  > 
- [e5964202d] (Tienson Qin) [2025-12-30]: Merge pull request #12279 from logseq/feat/page-publish
  > feat: page publish 
- [5f1695737] (Tienson Qin) [2025-12-30]: Merge branch 'master' into feat/page-publish
  > 
- [8cfcefcec] (charlie) [2025-12-30]: enhance(apis): add route handler and improve page block retrieval
  > 
- [5376779d4] (Tienson Qin) [2025-12-30]: deploy new version
  > 
- [8de2542f8] (Tienson Qin) [2025-12-30]: use salted PBKDF2 hash
  > 
- [f9a3fb50b] (Tienson Qin) [2025-12-30]: remove skip_auth
  > 
- [918dc0766] (Tienson Qin) [2025-12-30]: fix: set auth to be required on staging and prod
  > 
- [304195c7d] (Tienson Qin) [2025-12-30]: fix: typo
  > 
- [a0a7748e7] (Tienson Qin) [2025-12-30]: home page
  > 
- [adb7b8e36] (Tienson Qin) [2025-12-30]: fix: tagged nodes
  > 
- [dce1a8f4e] (Tienson Qin) [2025-12-30]: fix: e2e tests
  > 
- [6bc2fb1a0] (Tienson Qin) [2025-12-30]: fix: don't show published url property
  > 
- [45d592ecd] (Tienson Qin) [2025-12-30]: enhance: show close button when search box opened
  > 
- [c97453d1d] (Tienson Qin) [2025-12-30]: search
  > 
- [60e79785f] (Tienson Qin) [2025-12-30]: fix: add back missing dicts for flashcard buttons
  > 
- [c21f2c7f4] (Tienson Qin) [2025-12-30]: Ensure local graphs have uuid
  > 
- [644ef4c34] (Tienson Qin) [2025-12-30]: show authors
  > 
- [d53ea4a38] (Tienson Qin) [2025-12-30]: enhance: update published-url to be property type of :url
  > 
- [f8d494285] (Tienson Qin) [2025-12-30]: enhance: show page updated at
  > 
- [24c929835] (Tienson Qin) [2025-12-30]: fix: tests
  > 
- [12f967437] (Tienson Qin) [2025-12-30]: enhance: short url use "/p/short-id" instead of "/s/short-id"
  > 
- [74d136e49] (Tienson Qin) [2025-12-30]: fix: export tests
  > 
- [bdb6e0218] (Tienson Qin) [2025-12-30]: fix: remove icon from page properties area
  > 
- [8a415e007] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/publish.css
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [3e9786593] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/render.cljs
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [0ca329d63] (Tienson Qin) [2025-12-29]: add custom logseq/publish.css and logseq/publish.js
  > 
- [be69c009d] (Tienson Qin) [2025-12-29]: add user page
  > 
- [9dab647d0] (Tienson Qin) [2025-12-29]: add missing meta
  > 
- [da314220d] (Tienson Qin) [2025-12-29]: feat: unpublish
  > 
- [15d7526ac] (Tienson Qin) [2025-12-29]: fix: ast blocks in block title
  > 
- [2068123b2] (Tienson Qin) [2025-12-29]: fix: heading
  > 
- [0c79f4651] (Tienson Qin) [2025-12-29]: fix: cache published page
  > 
- [8c4be25e9] (Tienson Qin) [2025-12-29]: add timestamp version to invalid cache
  > 
- [7b7ec4706] (Tienson Qin) [2025-12-29]: add deploy script
  > 
- [efaff72f0] (Tienson Qin) [2025-12-29]: fix: mobile display issues
  > 
- [6c600d0a8] (Tienson Qin) [2025-12-29]: add dev script
  > 
- [8d6282121] (Tienson Qin) [2025-12-29]: add custom domain
  > 
- [ff56c6cbc] (Tienson Qin) [2025-12-29]: add publish staging and prod
  > 
- [7e695603d] (Tienson Qin) [2025-12-29]: Merge branch 'master' into feat/page-publish
  > 
- [bcc478b5f] (Tienson Qin) [2025-12-29]: refactor: separate og and db version (#12276)
  > separate og and new version apps

remove file sync, tldraw, excalidraw and zotero
- [56d3bd84d] (Tienson Qin) [2025-12-29]: add publish test
  > 
- [5ff22217d] (charlie) [2025-12-29]: fix(ui): adjust padding for property configuration header
  > 
- [5f185a39c] (charlie) [2025-12-29]: chore(libs): upgrade libs core and yarn.lock
  > 
- [62eb31c32] (Tienson Qin) [2025-12-29]: node embed support
  > 
- [78fbbaffa] (Tienson Qin) [2025-12-29]: fix: page icon
  > 
- [ef7cbd743] (Tienson Qin) [2025-12-29]: fix: url type property rendered as link
  > 
- [b1050da84] (Tienson Qin) [2025-12-29]: enhance: icon and tags position
  > 
- [5ed49d737] (Tienson Qin) [2025-12-29]: fix: positioned properties rendering
  > 
- [a04050d98] (Tienson Qin) [2025-12-29]: macro render
  > 
- [58f5a2ddc] (Tienson Qin) [2025-12-29]: block publish
  > 
- [3e033db0b] (Tienson Qin) [2025-12-29]: press `t o` to toggle collapse/expand
  > 
- [6d551e7b6] (Tienson Qin) [2025-12-29]: dark theme
  > 
- [db55b2d5c] (Tienson Qin) [2025-12-29]: style tweaks
  > 
- [a3d717812] (Tienson Qin) [2025-12-29]: multiple size variants for images
  > 
- [e77690b45] (Tienson Qin) [2025-12-28]: feat: password protected page
  > 
- [26c903f53] (Tienson Qin) [2025-12-28]: 404 page
  > 
- [0c668505c] (Tienson Qin) [2025-12-28]: friendly page for non published
  > 
- [326254c8f] (Tienson Qin) [2025-12-28]: style polish
  > 
- [12fabc63c] (Tienson Qin) [2025-12-28]: refactor: separate worker to multiple namespaces
  > 
- [55bf42887] (Tienson Qin) [2025-12-28]: code cleanup
  > 
- [4b2884abe] (Tienson Qin) [2025-12-28]: move inline style and script to publish.css and publish.js
  > 
- [b182a3736] (Tienson Qin) [2025-12-28]: fix: asset upload
  > 
- [cec45205d] (Tienson Qin) [2025-12-28]: wip: assets publish
  > 
- [aab7cfe2d] (Tienson Qin) [2025-12-28]: support math, code and quote blocks
  > 
- [80388f618] (Tienson Qin) [2025-12-28]: shorten url
  > 
- [0d38c982e] (Tienson Qin) [2025-12-28]: global ref
  > 
- [a682b9499] (Tienson Qin) [2025-12-28]: global tag
  > 
- [84cbb7931] (Tienson Qin) [2025-12-28]: tagged nodes
  > 
- [a4275838b] (Tienson Qin) [2025-12-28]: add graph page
  > 
- [08ab09a44] (Tienson Qin) [2025-12-28]: fix: property issues
  > 
- [7ad7e0b60] (Tienson Qin) [2025-12-28]: wip: properties publish
  > 
- [d4d8b1a7e] (Tienson Qin) [2025-12-28]: fix: add shadow-cljs.edn for publish worker
  > 
- [e9c2269e3] (Tienson Qin) [2025-12-28]: linked refs
  > 
- [36408416c] (Tienson Qin) [2025-12-27]: publish mvp
  > 
- [58cda5538] (Tienson Qin) [2025-12-27]: cljs worker
  > 
- [2a9624347] (Tienson Qin) [2025-12-27]: add publish worker
  > 
- [704312b40] (Tienson Qin) [2025-12-27]: Add publish dep
  > 
- [a2a5bfc09] (Tienson Qin) [2025-12-26]: remove temp workflow
  > 
- [c548542b4] (Tienson Qin) [2025-12-26]: add ci tool
  > 
- [7c7fe5e46] (Tienson Qin) [2025-12-26]: bump version to 2
  > 
- [8fe3cddba] (Tienson Qin) [2025-12-26]: enhance: limit access to db graphs only
  > 
- [8b312a403] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [678cb7dda] (Tienson Qin) [2025-12-26]: fix: init
  > 
- [910cd9514] (Tienson Qin) [2025-12-26]: fix: tests
  > 
- [c5c66aadc] (Tienson Qin) [2025-12-26]: update shadow-cljs.edn to remove tldraw and excalidraw
  > 
- [3d78209a6] (charlie) [2025-12-26]: chore(libs):bump new version
  > 
- [ec3ace5bd] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [cb9ec1d49] (Tienson Qin) [2025-12-26]: add sanitize-filename
  > 
- [c9a92f92b] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [c084a6b9d] (Tienson Qin) [2025-12-26]: fix dicts
  > 
- [ec7a325bc] (Tienson Qin) [2025-12-26]: more cleanup
  > 
- [f1de1ea16] (Tienson Qin) [2025-12-26]: remove git, chokidar, rsapi
  > 
- [467c5200d] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [7d719623a] (Tienson Qin) [2025-12-26]: refactor: prefer conn than repo for outliner core and rtc update
  > 
- [f6188b6cc] (Tienson Qin) [2025-12-25]: cleanup more config/db-based-graph? usage
  > 
- [eceaa7396] (Tienson Qin) [2025-12-25]: more cleanup
  > 
- [f419fbea8] (Tienson Qin) [2025-12-25]: cleanup
  > 
- [d8d6e8b4b] (Tienson Qin) [2025-12-25]: remove file based model
  > 
- [acbb03856] (Tienson Qin) [2025-12-25]: remove diff-merge
  > 
- [9f927137a] (Tienson Qin) [2025-12-25]: remove file based handlers
  > 
- [32cbac5a5] (Tienson Qin) [2025-12-25]: remove file based page handler
  > 
- [4aab5daca] (Tienson Qin) [2025-12-25]: add simplified datepicker
  > 
- [bbe6fa861] (Tienson Qin) [2025-12-25]: remove file based commands
  > 
- [8d1fafb4e] (Tienson Qin) [2025-12-25]: remove outdated datepicker component
  > 
- [eaa5a5733] (Tienson Qin) [2025-12-25]: remove file based api
  > 
- [5490824cf] (Tienson Qin) [2025-12-25]: remove old srs implementation
  > 
- [b3813ade7] (Tienson Qin) [2025-12-25]: remove file sync, tldraw, excalidraw and zotero
  > 
- [da06bc2db] (Tienson Qin) [2025-12-25]: wip: separate og and new version apps
  > 
- [fbe6b998b] (Charlie) [2025-12-25]: enhance(plugins): apis related improvements (#12266)
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/feature/unified-icon-picker
- [c727928a1] (scheinriese) [2025-12-10]: Redesign icon picker with improved UX and collapsible sections
  > - Restructure layout from absolute positioning to natural flow
- Move color picker and delete button to search section (always visible)
- Update tabs with underline indicator for active state
- Add collapsible section headers with keyboard shortcuts (Option+Command+1/2/3)
- Make search input edge-to-edge with improved styling (no focus ring, visible separator)
- Fix frequently used to show only one instance per text/avatar type
- Make avatar initials always uppercase for consistency
- Fix avatar sizing: 36x36px button with 28x28px avatar in picker
- Implement context-specific avatar font sizes: 14px (page title), 8px (sidebar), 12px (picker)
- All avatar fonts use font-weight 500

- [6ac7ccb12] (scheinriese) [2025-12-09]: Improved color setting for all icon types
  > 
- [e241f2f4f] (scheinriese) [2025-12-09]: Refactor icon style + no trailing of text in left sidebar depending on icon type
  > 
- [27c9a23d6] (scheinriese) [2025-12-09]: Reuse RTC Avatar component in icon picker
  > 

### Branch: upstream/fix/zotero-import
- [4f29f7b17] (Mega Yu) [2026-01-08]: fix zotero picture highlight block does not consider as asset after importing
  > 
- [5ed5e9c7b] (Mega Yu) [2026-01-08]: compatible with old configuration
  > 
- [6119efad1] (Mega Yu) [2026-01-08]: Fix zotero highlighting block error after importing from file
  > 
- [116c25b2f] (Mega Yu) [2026-01-07]: Import support for zotero linked file like using zotfile
  > 
- [c89e663dd] (Mega Yu) [2026-01-07]: Fix the inconsistency between imported Zotero label and actual filename
  > 
- [280f8ebc5] (Mega Yu) [2026-01-07]: Fix import Zotero not recognized as asset
  > 
- [23c6f8101] (charlie) [2026-01-06]: enhance(libs): add get_file_content function for retrieving file content
  > 
- [74bfe36d8] (charlie) [2026-01-06]: enhance(libs): update IDBProxy interface and improve documentation
  > 
- [8d7aaa791] (charlie) [2026-01-06]: enhance(apis): add setFileContent method for built-in file paths
  > 
- [6c1b9fe77] (Tienson Qin) [2026-01-06]: Add set_file_content api to update built-in files like custom.js/css
  > 
- [c2582d3c2] (Tienson Qin) [2026-01-06]: fix: available choices still shows other tags' scoped choices
  > 
- [289741069] (Tienson Qin) [2026-01-06]: tweak words for invalid db
  > 
- [574890505] (Gabriel Horner) [2026-01-05]: fix ref-property tests pointing to property rule
  > Also add back property tests as it is still a useful rule

- [6ba05ad2c] (Tienson Qin) [2026-01-05]: add e2e-test for isolated scope poroperties
  > 
- [79f5c6e3c] (Tienson Qin) [2026-01-05]: fix: tag-scoped properties still showing for other tags
  > 
- [28ba74628] (rcmerci) [2026-01-05]: chore(rtc): add fixme comment
  > 
- [1e61ae7dd] (rcmerci) [2026-01-05]: fix: lint
  > 
- [b82683357] (rcmerci) [2026-01-05]: dev: update agents.md
  > 
- [4dc4e1be7] (rcmerci) [2026-01-05]: enhance(rtc): add :add op to replace :move + :update combo
  > 
- [ae8710f76] (rcmerci) [2026-01-05]: dev: update review.md
  > 
- [41cbba368] (Tienson Qin) [2026-01-05]: enhance(ux): flashcards  (#12299)
  > Enhances the flashcard user experience by adding automatic query property management, fixing critical bugs, and improving the UI for managing card sets.

Key changes:

1. Automatic creation of query property blocks when tagging with #Query or subclasses via pipeline
2. Fixed critical bug in api-insert-new-block! where the end? parameter had inverted conditional logic
3. Added ability to create new #Cards blocks directly from the flashcard modal with a plus button
- [1cf4fa9ef] (Tienson Qin) [2026-01-05]: enhance(ux): add highlights to unlinked references
  > 
- [d1bbb5ad1] (charlie) [2026-01-05]: chore(libs): add development notes
  > 
- [2642765eb] (charlie) [2026-01-04]: chore: update db_properties_skill.md with enhanced property schema APIs and examples
  > 
- [a2bfb3963] (charlie) [2026-01-04]: chore: update version to 0.2.11 and add developer notes for Logseq DB properties
  > 
- [4b7d6c73c] (Tienson Qin) [2026-01-04]: fix: remove rtc test on updated-at
  > Since :block/updated-at should only be overwritten, not retracted

- [f940f708b] (Tienson Qin) [2026-01-04]: fix: rtc should never retract block timestamp
  > 
- [8c10cb64d] (Tienson Qin) [2026-01-04]: wrong ns
  > 
- [50e2bc53f] (Tienson Qin) [2026-01-04]: fix: filter out file graphs
  > 
- [a9a9905b0] (Tienson Qin) [2026-01-04]: feat: tag-scoped property choices (#12295)
  > * feat: tag-scoped property choices

* Able to hide global choices per tag

* add e2e tests
- [fd7e68582] (Tienson Qin) [2026-01-04]: chore: cleanup dicts
  > 
- [f43459f55] (Tienson Qin) [2026-01-04]: fix: wrong mobile event
  > 
- [e95c93629] (Tienson Qin) [2026-01-03]: fix: youtube embed on desktop
  > 
- [5705dd359] (Tienson Qin) [2026-01-03]: chore: code cleanup
  > 
- [ce5017fca] (charlie) [2026-01-03]: fix(tests): update tag properties in create tag test
  > 
- [9bd5390c0] (Tienson Qin) [2026-01-03]: enhance: don't close invite dialog
  > 
- [74d8308a6] (Tienson Qin) [2026-01-03]: chore: remove git commit command
  > 
- [0b77ee902] (Tienson Qin) [2026-01-03]: fix: trim blank for inviting email
  > 
- [baff8a4cc] (charlie) [2026-01-03]: enhance(apis): add support for tag properties in createTag API
  > 
- [664743b28] (Tienson Qin) [2026-01-03]: fix: logseq.Editor.renamePage HTTP API hangs indefinitely
  > related to https://github.com/logseq/db-test/issues/701

- [5459dbeb6] (Tienson Qin) [2026-01-03]: enhance: args for tag properties
  > 
- [8fd006bab] (Tienson Qin) [2026-01-03]: enhance: api createTag supports :tagProperties
  > 
- [ebb985f78] (Paweł Świątkowski) [2026-01-02]: Add ability to sort plugins by added date on marketplace (#11993)
  > Co-authored-by: Charlie <xyhp915@qq.com>
- [1cd561dbd] (Tienson Qin) [2026-01-01]: chore: CI cleanup
  > 
- [d4f4ad8f7] (Tienson Qin) [2026-01-01]: chore: bump version for mobile apps
  > 
- [324d02896] (Tienson Qin) [2026-01-01]: chore: remove rsapi from CI
  > 
- [44f26f91f] (Tienson Qin) [2026-01-01]: fix: add back ssl/keystore.jks for mobile development
  > 
- [497645082] (Tienson Qin) [2026-01-01]: fix: lint
  > 
- [51634b4c5] (Tienson Qin) [2026-01-01]: fix: collapse/expand toggle on mobile
  > 
- [4d46c5e9b] (Tienson Qin) [2026-01-01]: fix: youtube timestamp on web/electron
  > 
- [10846fdaa] (Tienson Qin) [2026-01-01]: fix: asset wrong order
  > 
- [7d493575d] (Tienson Qin) [2026-01-01]: fix: add missing datepicker.css
  > 
- [8fc093c1c] (Tienson Qin) [2026-01-01]: fix: can't set number 9 as property value
  > because it's empty-placeholder

- [44468dd20] (charlie) [2026-01-01]: enhance(apis): simplify tag resolution and enhance tag handling functions
  > 
- [5f42c51c3] (charlie) [2026-01-01]: enhance(property): add data attributes for property title and type
  > 
- [460eefb96] (charlie) [2026-01-01]: enhance(apis): add reset option to upsertBlockProperty for block properties
  > 
- [aafa50dcc] (Tienson Qin) [2026-01-01]: fix: :block/warning value should be keyword
  > 
- [6f59a02f5] (Tienson Qin) [2026-01-01]: fix: mark ensure-graph-uuid as deprecated
  > 
- [99dea608f] (rcmerci) [2026-01-01]: fix(ci): update typos.toml
  > 
- [ed1133095] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683 (2)
  > 
- [76224da1b] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683
  > 
- [0f949b739] (Tienson Qin) [2026-01-01]: fix: tests
  > 
- [b55550ed3] (Tienson Qin) [2026-01-01]: add :logseq.kv/local-graph-uuid
  > 
- [3078f2a90] (Tienson Qin) [2025-12-31]: chore: remove outdated test
  > 
- [15585f148] (Tienson Qin) [2025-12-31]: mod+m to publish
  > 
- [f1a410887] (rcmerci) [2025-12-31]: fix(rtc): fix invalid apply-ops msg
  > 
- [76ebafac6] (Tienson Qin) [2025-12-31]: enhance: preserve block collapsed state when publishing
  > 
- [5f532a6a8] (Gabriel Horner) [2025-12-30]: chore: update now outdated parts of db version section
  > 
- [a6503cc2f] (Gabriel Horner) [2025-12-30]: fix: add back validation of properties graph to CI
  > Accidentally taken out w/ last refactor

- [ca14110a8] (Gabriel Horner) [2025-12-30]: fix: nbb scripts failing to start
  > Caused by new dep that isn't configured for use with nbb.
Can add back when it's actually nbb compatible and has a nbb.edn

- [3b1433647] (Tienson Qin) [2025-12-30]: fix: set limit to passsword kdf iterations on cloudflare workers
  > 
- [e5964202d] (Tienson Qin) [2025-12-30]: Merge pull request #12279 from logseq/feat/page-publish
  > feat: page publish 
- [5f1695737] (Tienson Qin) [2025-12-30]: Merge branch 'master' into feat/page-publish
  > 
- [8cfcefcec] (charlie) [2025-12-30]: enhance(apis): add route handler and improve page block retrieval
  > 
- [5376779d4] (Tienson Qin) [2025-12-30]: deploy new version
  > 
- [8de2542f8] (Tienson Qin) [2025-12-30]: use salted PBKDF2 hash
  > 
- [f9a3fb50b] (Tienson Qin) [2025-12-30]: remove skip_auth
  > 
- [918dc0766] (Tienson Qin) [2025-12-30]: fix: set auth to be required on staging and prod
  > 
- [304195c7d] (Tienson Qin) [2025-12-30]: fix: typo
  > 
- [a0a7748e7] (Tienson Qin) [2025-12-30]: home page
  > 
- [adb7b8e36] (Tienson Qin) [2025-12-30]: fix: tagged nodes
  > 
- [dce1a8f4e] (Tienson Qin) [2025-12-30]: fix: e2e tests
  > 
- [6bc2fb1a0] (Tienson Qin) [2025-12-30]: fix: don't show published url property
  > 
- [45d592ecd] (Tienson Qin) [2025-12-30]: enhance: show close button when search box opened
  > 
- [c97453d1d] (Tienson Qin) [2025-12-30]: search
  > 
- [60e79785f] (Tienson Qin) [2025-12-30]: fix: add back missing dicts for flashcard buttons
  > 
- [c21f2c7f4] (Tienson Qin) [2025-12-30]: Ensure local graphs have uuid
  > 
- [644ef4c34] (Tienson Qin) [2025-12-30]: show authors
  > 
- [d53ea4a38] (Tienson Qin) [2025-12-30]: enhance: update published-url to be property type of :url
  > 
- [f8d494285] (Tienson Qin) [2025-12-30]: enhance: show page updated at
  > 
- [24c929835] (Tienson Qin) [2025-12-30]: fix: tests
  > 
- [12f967437] (Tienson Qin) [2025-12-30]: enhance: short url use "/p/short-id" instead of "/s/short-id"
  > 
- [74d136e49] (Tienson Qin) [2025-12-30]: fix: export tests
  > 
- [bdb6e0218] (Tienson Qin) [2025-12-30]: fix: remove icon from page properties area
  > 
- [8a415e007] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/publish.css
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [3e9786593] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/render.cljs
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [0ca329d63] (Tienson Qin) [2025-12-29]: add custom logseq/publish.css and logseq/publish.js
  > 
- [be69c009d] (Tienson Qin) [2025-12-29]: add user page
  > 
- [9dab647d0] (Tienson Qin) [2025-12-29]: add missing meta
  > 
- [da314220d] (Tienson Qin) [2025-12-29]: feat: unpublish
  > 
- [15d7526ac] (Tienson Qin) [2025-12-29]: fix: ast blocks in block title
  > 
- [2068123b2] (Tienson Qin) [2025-12-29]: fix: heading
  > 
- [0c79f4651] (Tienson Qin) [2025-12-29]: fix: cache published page
  > 
- [8c4be25e9] (Tienson Qin) [2025-12-29]: add timestamp version to invalid cache
  > 
- [7b7ec4706] (Tienson Qin) [2025-12-29]: add deploy script
  > 
- [efaff72f0] (Tienson Qin) [2025-12-29]: fix: mobile display issues
  > 
- [6c600d0a8] (Tienson Qin) [2025-12-29]: add dev script
  > 
- [8d6282121] (Tienson Qin) [2025-12-29]: add custom domain
  > 
- [ff56c6cbc] (Tienson Qin) [2025-12-29]: add publish staging and prod
  > 
- [7e695603d] (Tienson Qin) [2025-12-29]: Merge branch 'master' into feat/page-publish
  > 
- [bcc478b5f] (Tienson Qin) [2025-12-29]: refactor: separate og and db version (#12276)
  > separate og and new version apps

remove file sync, tldraw, excalidraw and zotero
- [56d3bd84d] (Tienson Qin) [2025-12-29]: add publish test
  > 
- [5ff22217d] (charlie) [2025-12-29]: fix(ui): adjust padding for property configuration header
  > 
- [5f185a39c] (charlie) [2025-12-29]: chore(libs): upgrade libs core and yarn.lock
  > 
- [62eb31c32] (Tienson Qin) [2025-12-29]: node embed support
  > 
- [78fbbaffa] (Tienson Qin) [2025-12-29]: fix: page icon
  > 
- [ef7cbd743] (Tienson Qin) [2025-12-29]: fix: url type property rendered as link
  > 
- [b1050da84] (Tienson Qin) [2025-12-29]: enhance: icon and tags position
  > 
- [5ed49d737] (Tienson Qin) [2025-12-29]: fix: positioned properties rendering
  > 
- [a04050d98] (Tienson Qin) [2025-12-29]: macro render
  > 
- [58f5a2ddc] (Tienson Qin) [2025-12-29]: block publish
  > 
- [3e033db0b] (Tienson Qin) [2025-12-29]: press `t o` to toggle collapse/expand
  > 
- [6d551e7b6] (Tienson Qin) [2025-12-29]: dark theme
  > 
- [db55b2d5c] (Tienson Qin) [2025-12-29]: style tweaks
  > 
- [a3d717812] (Tienson Qin) [2025-12-29]: multiple size variants for images
  > 
- [e77690b45] (Tienson Qin) [2025-12-28]: feat: password protected page
  > 
- [26c903f53] (Tienson Qin) [2025-12-28]: 404 page
  > 
- [0c668505c] (Tienson Qin) [2025-12-28]: friendly page for non published
  > 
- [326254c8f] (Tienson Qin) [2025-12-28]: style polish
  > 
- [12fabc63c] (Tienson Qin) [2025-12-28]: refactor: separate worker to multiple namespaces
  > 
- [55bf42887] (Tienson Qin) [2025-12-28]: code cleanup
  > 
- [4b2884abe] (Tienson Qin) [2025-12-28]: move inline style and script to publish.css and publish.js
  > 
- [b182a3736] (Tienson Qin) [2025-12-28]: fix: asset upload
  > 
- [cec45205d] (Tienson Qin) [2025-12-28]: wip: assets publish
  > 
- [aab7cfe2d] (Tienson Qin) [2025-12-28]: support math, code and quote blocks
  > 
- [80388f618] (Tienson Qin) [2025-12-28]: shorten url
  > 
- [0d38c982e] (Tienson Qin) [2025-12-28]: global ref
  > 
- [a682b9499] (Tienson Qin) [2025-12-28]: global tag
  > 
- [84cbb7931] (Tienson Qin) [2025-12-28]: tagged nodes
  > 
- [a4275838b] (Tienson Qin) [2025-12-28]: add graph page
  > 
- [08ab09a44] (Tienson Qin) [2025-12-28]: fix: property issues
  > 
- [7ad7e0b60] (Tienson Qin) [2025-12-28]: wip: properties publish
  > 
- [d4d8b1a7e] (Tienson Qin) [2025-12-28]: fix: add shadow-cljs.edn for publish worker
  > 
- [e9c2269e3] (Tienson Qin) [2025-12-28]: linked refs
  > 
- [36408416c] (Tienson Qin) [2025-12-27]: publish mvp
  > 
- [58cda5538] (Tienson Qin) [2025-12-27]: cljs worker
  > 
- [2a9624347] (Tienson Qin) [2025-12-27]: add publish worker
  > 
- [704312b40] (Tienson Qin) [2025-12-27]: Add publish dep
  > 
- [a2a5bfc09] (Tienson Qin) [2025-12-26]: remove temp workflow
  > 
- [c548542b4] (Tienson Qin) [2025-12-26]: add ci tool
  > 
- [7c7fe5e46] (Tienson Qin) [2025-12-26]: bump version to 2
  > 
- [8fe3cddba] (Tienson Qin) [2025-12-26]: enhance: limit access to db graphs only
  > 
- [8b312a403] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [678cb7dda] (Tienson Qin) [2025-12-26]: fix: init
  > 
- [910cd9514] (Tienson Qin) [2025-12-26]: fix: tests
  > 
- [c5c66aadc] (Tienson Qin) [2025-12-26]: update shadow-cljs.edn to remove tldraw and excalidraw
  > 
- [3d78209a6] (charlie) [2025-12-26]: chore(libs):bump new version
  > 
- [ec3ace5bd] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [cb9ec1d49] (Tienson Qin) [2025-12-26]: add sanitize-filename
  > 
- [c9a92f92b] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [c084a6b9d] (Tienson Qin) [2025-12-26]: fix dicts
  > 
- [ec7a325bc] (Tienson Qin) [2025-12-26]: more cleanup
  > 
- [f1de1ea16] (Tienson Qin) [2025-12-26]: remove git, chokidar, rsapi
  > 
- [467c5200d] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [7d719623a] (Tienson Qin) [2025-12-26]: refactor: prefer conn than repo for outliner core and rtc update
  > 
- [f6188b6cc] (Tienson Qin) [2025-12-25]: cleanup more config/db-based-graph? usage
  > 
- [eceaa7396] (Tienson Qin) [2025-12-25]: more cleanup
  > 
- [f419fbea8] (Tienson Qin) [2025-12-25]: cleanup
  > 
- [d8d6e8b4b] (Tienson Qin) [2025-12-25]: remove file based model
  > 
- [acbb03856] (Tienson Qin) [2025-12-25]: remove diff-merge
  > 
- [9f927137a] (Tienson Qin) [2025-12-25]: remove file based handlers
  > 
- [32cbac5a5] (Tienson Qin) [2025-12-25]: remove file based page handler
  > 
- [4aab5daca] (Tienson Qin) [2025-12-25]: add simplified datepicker
  > 
- [bbe6fa861] (Tienson Qin) [2025-12-25]: remove file based commands
  > 
- [8d1fafb4e] (Tienson Qin) [2025-12-25]: remove outdated datepicker component
  > 
- [eaa5a5733] (Tienson Qin) [2025-12-25]: remove file based api
  > 
- [5490824cf] (Tienson Qin) [2025-12-25]: remove old srs implementation
  > 
- [b3813ade7] (Tienson Qin) [2025-12-25]: remove file sync, tldraw, excalidraw and zotero
  > 
- [da06bc2db] (Tienson Qin) [2025-12-25]: wip: separate og and new version apps
  > 
- [fbe6b998b] (Charlie) [2025-12-25]: enhance(plugins): apis related improvements (#12266)
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/master
- [8a51a8a7b] (Thiago O. Maciel) [2026-01-10]: fix mobile docs
  > 
- [cb019e9e5] (Tienson Qin) [2026-01-09]: remove source maps for testing
  > 
- [eb7e52257] (Tienson Qin) [2026-01-08]: disable sentry for any-branch test
  > 
- [b6ca3097d] (Tienson Qin) [2026-01-08]: add workflow to deploy any branch
  > 
- [562a68e0e] (rcmerci) [2026-01-08]: enhance(rtc): move rtc api schema to logseq/logseq-schema repo
  > 
- [c8148f66e] (charlie) [2026-01-07]: chore(libs): add Logseq plugin development agent guide
  > 
- [ffdb990d8] (charlie) [2026-01-07]: chore(libs): rename db_properties_notes to db_properties_guide and update README references
  > 
- [1a390a4a1] (rcmerci) [2026-01-06]: Merge pull request #12304 from logseq/debug/add-tx-log
  > feat(rtc): add debug-log tool for rtc
- [f7e1a3a6f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, only record rtc related tx-data
  > 
- [137e85beb] (rcmerci) [2026-01-06]: fix lint
  > 
- [e5668a469] (rcmerci) [2026-01-06]: fix lint
  > 
- [73888c0a9] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, update schema
  > 
- [1a33a365f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log
  > - integrate `debug-log-db` into GC process
- update schema with `repo` column and `DATETIME`
- export `reset_debug_log_db`

- [27016657f] (Tienson Qin) [2026-01-06]: add debug db tx-log and ws messages tables
  > 
- [6f513fa51] (Tienson Qin) [2026-01-06]: enhance(ux): make it clear that hourly backup is running on desktop
  > 
- [00ccff81e] (Tienson Qin) [2026-01-06]: more file-graph cleanup
  > 
- [23c6f8101] (charlie) [2026-01-06]: enhance(libs): add get_file_content function for retrieving file content
  > 
- [74bfe36d8] (charlie) [2026-01-06]: enhance(libs): update IDBProxy interface and improve documentation
  > 
- [8d7aaa791] (charlie) [2026-01-06]: enhance(apis): add setFileContent method for built-in file paths
  > 
- [6c1b9fe77] (Tienson Qin) [2026-01-06]: Add set_file_content api to update built-in files like custom.js/css
  > 
- [c2582d3c2] (Tienson Qin) [2026-01-06]: fix: available choices still shows other tags' scoped choices
  > 
- [289741069] (Tienson Qin) [2026-01-06]: tweak words for invalid db
  > 
- [574890505] (Gabriel Horner) [2026-01-05]: fix ref-property tests pointing to property rule
  > Also add back property tests as it is still a useful rule

- [6ba05ad2c] (Tienson Qin) [2026-01-05]: add e2e-test for isolated scope poroperties
  > 
- [79f5c6e3c] (Tienson Qin) [2026-01-05]: fix: tag-scoped properties still showing for other tags
  > 
- [28ba74628] (rcmerci) [2026-01-05]: chore(rtc): add fixme comment
  > 
- [1e61ae7dd] (rcmerci) [2026-01-05]: fix: lint
  > 
- [b82683357] (rcmerci) [2026-01-05]: dev: update agents.md
  > 
- [4dc4e1be7] (rcmerci) [2026-01-05]: enhance(rtc): add :add op to replace :move + :update combo
  > 
- [ae8710f76] (rcmerci) [2026-01-05]: dev: update review.md
  > 
- [41cbba368] (Tienson Qin) [2026-01-05]: enhance(ux): flashcards  (#12299)
  > Enhances the flashcard user experience by adding automatic query property management, fixing critical bugs, and improving the UI for managing card sets.

Key changes:

1. Automatic creation of query property blocks when tagging with #Query or subclasses via pipeline
2. Fixed critical bug in api-insert-new-block! where the end? parameter had inverted conditional logic
3. Added ability to create new #Cards blocks directly from the flashcard modal with a plus button
- [1cf4fa9ef] (Tienson Qin) [2026-01-05]: enhance(ux): add highlights to unlinked references
  > 
- [d1bbb5ad1] (charlie) [2026-01-05]: chore(libs): add development notes
  > 
- [2642765eb] (charlie) [2026-01-04]: chore: update db_properties_skill.md with enhanced property schema APIs and examples
  > 
- [a2bfb3963] (charlie) [2026-01-04]: chore: update version to 0.2.11 and add developer notes for Logseq DB properties
  > 
- [4b7d6c73c] (Tienson Qin) [2026-01-04]: fix: remove rtc test on updated-at
  > Since :block/updated-at should only be overwritten, not retracted

- [f940f708b] (Tienson Qin) [2026-01-04]: fix: rtc should never retract block timestamp
  > 
- [8c10cb64d] (Tienson Qin) [2026-01-04]: wrong ns
  > 
- [50e2bc53f] (Tienson Qin) [2026-01-04]: fix: filter out file graphs
  > 
- [a9a9905b0] (Tienson Qin) [2026-01-04]: feat: tag-scoped property choices (#12295)
  > * feat: tag-scoped property choices

* Able to hide global choices per tag

* add e2e tests
- [fd7e68582] (Tienson Qin) [2026-01-04]: chore: cleanup dicts
  > 
- [f43459f55] (Tienson Qin) [2026-01-04]: fix: wrong mobile event
  > 
- [e95c93629] (Tienson Qin) [2026-01-03]: fix: youtube embed on desktop
  > 
- [5705dd359] (Tienson Qin) [2026-01-03]: chore: code cleanup
  > 
- [ce5017fca] (charlie) [2026-01-03]: fix(tests): update tag properties in create tag test
  > 
- [9bd5390c0] (Tienson Qin) [2026-01-03]: enhance: don't close invite dialog
  > 
- [74d8308a6] (Tienson Qin) [2026-01-03]: chore: remove git commit command
  > 
- [0b77ee902] (Tienson Qin) [2026-01-03]: fix: trim blank for inviting email
  > 
- [baff8a4cc] (charlie) [2026-01-03]: enhance(apis): add support for tag properties in createTag API
  > 
- [664743b28] (Tienson Qin) [2026-01-03]: fix: logseq.Editor.renamePage HTTP API hangs indefinitely
  > related to https://github.com/logseq/db-test/issues/701

- [5459dbeb6] (Tienson Qin) [2026-01-03]: enhance: args for tag properties
  > 
- [8fd006bab] (Tienson Qin) [2026-01-03]: enhance: api createTag supports :tagProperties
  > 
- [ebb985f78] (Paweł Świątkowski) [2026-01-02]: Add ability to sort plugins by added date on marketplace (#11993)
  > Co-authored-by: Charlie <xyhp915@qq.com>
- [1cd561dbd] (Tienson Qin) [2026-01-01]: chore: CI cleanup
  > 
- [d4f4ad8f7] (Tienson Qin) [2026-01-01]: chore: bump version for mobile apps
  > 
- [324d02896] (Tienson Qin) [2026-01-01]: chore: remove rsapi from CI
  > 
- [44f26f91f] (Tienson Qin) [2026-01-01]: fix: add back ssl/keystore.jks for mobile development
  > 
- [497645082] (Tienson Qin) [2026-01-01]: fix: lint
  > 
- [51634b4c5] (Tienson Qin) [2026-01-01]: fix: collapse/expand toggle on mobile
  > 
- [4d46c5e9b] (Tienson Qin) [2026-01-01]: fix: youtube timestamp on web/electron
  > 
- [10846fdaa] (Tienson Qin) [2026-01-01]: fix: asset wrong order
  > 
- [7d493575d] (Tienson Qin) [2026-01-01]: fix: add missing datepicker.css
  > 
- [8fc093c1c] (Tienson Qin) [2026-01-01]: fix: can't set number 9 as property value
  > because it's empty-placeholder

- [44468dd20] (charlie) [2026-01-01]: enhance(apis): simplify tag resolution and enhance tag handling functions
  > 
- [5f42c51c3] (charlie) [2026-01-01]: enhance(property): add data attributes for property title and type
  > 
- [460eefb96] (charlie) [2026-01-01]: enhance(apis): add reset option to upsertBlockProperty for block properties
  > 
- [aafa50dcc] (Tienson Qin) [2026-01-01]: fix: :block/warning value should be keyword
  > 
- [6f59a02f5] (Tienson Qin) [2026-01-01]: fix: mark ensure-graph-uuid as deprecated
  > 
- [99dea608f] (rcmerci) [2026-01-01]: fix(ci): update typos.toml
  > 
- [ed1133095] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683 (2)
  > 
- [76224da1b] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683
  > 
- [0f949b739] (Tienson Qin) [2026-01-01]: fix: tests
  > 
- [b55550ed3] (Tienson Qin) [2026-01-01]: add :logseq.kv/local-graph-uuid
  > 
- [3078f2a90] (Tienson Qin) [2025-12-31]: chore: remove outdated test
  > 
- [15585f148] (Tienson Qin) [2025-12-31]: mod+m to publish
  > 
- [f1a410887] (rcmerci) [2025-12-31]: fix(rtc): fix invalid apply-ops msg
  > 
- [76ebafac6] (Tienson Qin) [2025-12-31]: enhance: preserve block collapsed state when publishing
  > 
- [5f532a6a8] (Gabriel Horner) [2025-12-30]: chore: update now outdated parts of db version section
  > 
- [a6503cc2f] (Gabriel Horner) [2025-12-30]: fix: add back validation of properties graph to CI
  > Accidentally taken out w/ last refactor

- [ca14110a8] (Gabriel Horner) [2025-12-30]: fix: nbb scripts failing to start
  > Caused by new dep that isn't configured for use with nbb.
Can add back when it's actually nbb compatible and has a nbb.edn

- [3b1433647] (Tienson Qin) [2025-12-30]: fix: set limit to passsword kdf iterations on cloudflare workers
  > 
- [e5964202d] (Tienson Qin) [2025-12-30]: Merge pull request #12279 from logseq/feat/page-publish
  > feat: page publish 
- [5f1695737] (Tienson Qin) [2025-12-30]: Merge branch 'master' into feat/page-publish
  > 
- [8cfcefcec] (charlie) [2025-12-30]: enhance(apis): add route handler and improve page block retrieval
  > 
- [5376779d4] (Tienson Qin) [2025-12-30]: deploy new version
  > 
- [8de2542f8] (Tienson Qin) [2025-12-30]: use salted PBKDF2 hash
  > 
- [f9a3fb50b] (Tienson Qin) [2025-12-30]: remove skip_auth
  > 
- [918dc0766] (Tienson Qin) [2025-12-30]: fix: set auth to be required on staging and prod
  > 
- [304195c7d] (Tienson Qin) [2025-12-30]: fix: typo
  > 
- [a0a7748e7] (Tienson Qin) [2025-12-30]: home page
  > 
- [adb7b8e36] (Tienson Qin) [2025-12-30]: fix: tagged nodes
  > 
- [dce1a8f4e] (Tienson Qin) [2025-12-30]: fix: e2e tests
  > 
- [6bc2fb1a0] (Tienson Qin) [2025-12-30]: fix: don't show published url property
  > 
- [45d592ecd] (Tienson Qin) [2025-12-30]: enhance: show close button when search box opened
  > 
- [c97453d1d] (Tienson Qin) [2025-12-30]: search
  > 
- [60e79785f] (Tienson Qin) [2025-12-30]: fix: add back missing dicts for flashcard buttons
  > 
- [c21f2c7f4] (Tienson Qin) [2025-12-30]: Ensure local graphs have uuid
  > 
- [644ef4c34] (Tienson Qin) [2025-12-30]: show authors
  > 
- [d53ea4a38] (Tienson Qin) [2025-12-30]: enhance: update published-url to be property type of :url
  > 
- [f8d494285] (Tienson Qin) [2025-12-30]: enhance: show page updated at
  > 
- [24c929835] (Tienson Qin) [2025-12-30]: fix: tests
  > 
- [12f967437] (Tienson Qin) [2025-12-30]: enhance: short url use "/p/short-id" instead of "/s/short-id"
  > 
- [74d136e49] (Tienson Qin) [2025-12-30]: fix: export tests
  > 
- [bdb6e0218] (Tienson Qin) [2025-12-30]: fix: remove icon from page properties area
  > 
- [8a415e007] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/publish.css
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [3e9786593] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/render.cljs
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [0ca329d63] (Tienson Qin) [2025-12-29]: add custom logseq/publish.css and logseq/publish.js
  > 
- [be69c009d] (Tienson Qin) [2025-12-29]: add user page
  > 
- [9dab647d0] (Tienson Qin) [2025-12-29]: add missing meta
  > 
- [da314220d] (Tienson Qin) [2025-12-29]: feat: unpublish
  > 
- [15d7526ac] (Tienson Qin) [2025-12-29]: fix: ast blocks in block title
  > 
- [2068123b2] (Tienson Qin) [2025-12-29]: fix: heading
  > 
- [0c79f4651] (Tienson Qin) [2025-12-29]: fix: cache published page
  > 
- [8c4be25e9] (Tienson Qin) [2025-12-29]: add timestamp version to invalid cache
  > 
- [7b7ec4706] (Tienson Qin) [2025-12-29]: add deploy script
  > 
- [efaff72f0] (Tienson Qin) [2025-12-29]: fix: mobile display issues
  > 
- [6c600d0a8] (Tienson Qin) [2025-12-29]: add dev script
  > 
- [8d6282121] (Tienson Qin) [2025-12-29]: add custom domain
  > 
- [ff56c6cbc] (Tienson Qin) [2025-12-29]: add publish staging and prod
  > 
- [7e695603d] (Tienson Qin) [2025-12-29]: Merge branch 'master' into feat/page-publish
  > 
- [bcc478b5f] (Tienson Qin) [2025-12-29]: refactor: separate og and db version (#12276)
  > separate og and new version apps

remove file sync, tldraw, excalidraw and zotero
- [56d3bd84d] (Tienson Qin) [2025-12-29]: add publish test
  > 
- [5ff22217d] (charlie) [2025-12-29]: fix(ui): adjust padding for property configuration header
  > 
- [5f185a39c] (charlie) [2025-12-29]: chore(libs): upgrade libs core and yarn.lock
  > 
- [62eb31c32] (Tienson Qin) [2025-12-29]: node embed support
  > 
- [78fbbaffa] (Tienson Qin) [2025-12-29]: fix: page icon
  > 
- [ef7cbd743] (Tienson Qin) [2025-12-29]: fix: url type property rendered as link
  > 
- [b1050da84] (Tienson Qin) [2025-12-29]: enhance: icon and tags position
  > 
- [5ed49d737] (Tienson Qin) [2025-12-29]: fix: positioned properties rendering
  > 
- [a04050d98] (Tienson Qin) [2025-12-29]: macro render
  > 
- [58f5a2ddc] (Tienson Qin) [2025-12-29]: block publish
  > 
- [3e033db0b] (Tienson Qin) [2025-12-29]: press `t o` to toggle collapse/expand
  > 
- [6d551e7b6] (Tienson Qin) [2025-12-29]: dark theme
  > 
- [db55b2d5c] (Tienson Qin) [2025-12-29]: style tweaks
  > 
- [a3d717812] (Tienson Qin) [2025-12-29]: multiple size variants for images
  > 
- [e77690b45] (Tienson Qin) [2025-12-28]: feat: password protected page
  > 
- [26c903f53] (Tienson Qin) [2025-12-28]: 404 page
  > 
- [0c668505c] (Tienson Qin) [2025-12-28]: friendly page for non published
  > 
- [326254c8f] (Tienson Qin) [2025-12-28]: style polish
  > 
- [12fabc63c] (Tienson Qin) [2025-12-28]: refactor: separate worker to multiple namespaces
  > 
- [55bf42887] (Tienson Qin) [2025-12-28]: code cleanup
  > 
- [4b2884abe] (Tienson Qin) [2025-12-28]: move inline style and script to publish.css and publish.js
  > 
- [b182a3736] (Tienson Qin) [2025-12-28]: fix: asset upload
  > 
- [cec45205d] (Tienson Qin) [2025-12-28]: wip: assets publish
  > 
- [aab7cfe2d] (Tienson Qin) [2025-12-28]: support math, code and quote blocks
  > 
- [80388f618] (Tienson Qin) [2025-12-28]: shorten url
  > 
- [0d38c982e] (Tienson Qin) [2025-12-28]: global ref
  > 
- [a682b9499] (Tienson Qin) [2025-12-28]: global tag
  > 
- [84cbb7931] (Tienson Qin) [2025-12-28]: tagged nodes
  > 
- [a4275838b] (Tienson Qin) [2025-12-28]: add graph page
  > 
- [08ab09a44] (Tienson Qin) [2025-12-28]: fix: property issues
  > 
- [7ad7e0b60] (Tienson Qin) [2025-12-28]: wip: properties publish
  > 
- [d4d8b1a7e] (Tienson Qin) [2025-12-28]: fix: add shadow-cljs.edn for publish worker
  > 
- [e9c2269e3] (Tienson Qin) [2025-12-28]: linked refs
  > 
- [36408416c] (Tienson Qin) [2025-12-27]: publish mvp
  > 
- [58cda5538] (Tienson Qin) [2025-12-27]: cljs worker
  > 
- [2a9624347] (Tienson Qin) [2025-12-27]: add publish worker
  > 
- [704312b40] (Tienson Qin) [2025-12-27]: Add publish dep
  > 
- [a2a5bfc09] (Tienson Qin) [2025-12-26]: remove temp workflow
  > 
- [c548542b4] (Tienson Qin) [2025-12-26]: add ci tool
  > 
- [7c7fe5e46] (Tienson Qin) [2025-12-26]: bump version to 2
  > 
- [8fe3cddba] (Tienson Qin) [2025-12-26]: enhance: limit access to db graphs only
  > 
- [8b312a403] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [678cb7dda] (Tienson Qin) [2025-12-26]: fix: init
  > 
- [910cd9514] (Tienson Qin) [2025-12-26]: fix: tests
  > 
- [c5c66aadc] (Tienson Qin) [2025-12-26]: update shadow-cljs.edn to remove tldraw and excalidraw
  > 
- [3d78209a6] (charlie) [2025-12-26]: chore(libs):bump new version
  > 
- [ec3ace5bd] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [cb9ec1d49] (Tienson Qin) [2025-12-26]: add sanitize-filename
  > 
- [c9a92f92b] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [c084a6b9d] (Tienson Qin) [2025-12-26]: fix dicts
  > 
- [ec7a325bc] (Tienson Qin) [2025-12-26]: more cleanup
  > 
- [f1de1ea16] (Tienson Qin) [2025-12-26]: remove git, chokidar, rsapi
  > 
- [467c5200d] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [7d719623a] (Tienson Qin) [2025-12-26]: refactor: prefer conn than repo for outliner core and rtc update
  > 
- [f6188b6cc] (Tienson Qin) [2025-12-25]: cleanup more config/db-based-graph? usage
  > 
- [eceaa7396] (Tienson Qin) [2025-12-25]: more cleanup
  > 
- [f419fbea8] (Tienson Qin) [2025-12-25]: cleanup
  > 
- [d8d6e8b4b] (Tienson Qin) [2025-12-25]: remove file based model
  > 
- [acbb03856] (Tienson Qin) [2025-12-25]: remove diff-merge
  > 
- [9f927137a] (Tienson Qin) [2025-12-25]: remove file based handlers
  > 
- [32cbac5a5] (Tienson Qin) [2025-12-25]: remove file based page handler
  > 
- [4aab5daca] (Tienson Qin) [2025-12-25]: add simplified datepicker
  > 
- [bbe6fa861] (Tienson Qin) [2025-12-25]: remove file based commands
  > 
- [8d1fafb4e] (Tienson Qin) [2025-12-25]: remove outdated datepicker component
  > 
- [eaa5a5733] (Tienson Qin) [2025-12-25]: remove file based api
  > 
- [5490824cf] (Tienson Qin) [2025-12-25]: remove old srs implementation
  > 
- [b3813ade7] (Tienson Qin) [2025-12-25]: remove file sync, tldraw, excalidraw and zotero
  > 
- [da06bc2db] (Tienson Qin) [2025-12-25]: wip: separate og and new version apps
  > 
- [fbe6b998b] (Charlie) [2025-12-25]: enhance(plugins): apis related improvements (#12266)
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/perf/long-page-pagination
- [103812cd8] (Tienson Qin) [2025-12-18]: perf: long page rendering
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/refactor/apis-types
- [772db767c] (charlie) [2025-12-25]: Merge branch 'master' into refactor/apis-types
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [a203ee020] (charlie) [2025-12-25]: enhance(apis): add tests for retrieving tags by name with case insensitivity and filtering
  > 
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [dcb34d41e] (charlie) [2025-12-24]: Merge branch 'master' into refactor/apis-types
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [b6c9f8136] (charlie) [2025-12-23]: enhance(apis): improve handling of class ident namespaces in tag(class) creation
  > 
- [eb80a4ba7] (charlie) [2025-12-22]: Merge branch 'master' into refactor/apis-types
  > 
- [dbc240903] (charlie) [2025-12-21]: fix: lints
  > 
- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [2d2a44e6b] (charlie) [2025-12-19]: enhances(apis): add apis for retrieving tags by name
  > 
- [513268edb] (charlie) [2025-12-19]: fix(page): prevent checking existing page with custom ident namespace
  > 
- [0c9345ec1] (charlie) [2025-12-19]: Merge branch 'master' into refactor/apis-types
  > 
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [941ecd493] (charlie) [2025-12-18]: chore(libs): update typedoc-plugin-lsp-docs to version 0.0.2 and fix build:docs command
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [3a16bca77] (charlie) [2025-12-18]: fix(property): add number input handling and effect for value updates
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [4bb24a54f] (charlie) [2025-12-17]: fix(apis): incorrect return to create page for the existing case
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [551e06b34] (charlie) [2025-12-17]: enhance(apis): add open pdf viewer function to handle PDF viewing
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [5b0c989fc] (charlie) [2025-12-17]: Merge branch 'master' into refactor/apis-types
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [b5266fb26] (charlie) [2025-12-12]: enhance(tests): improve tag handling tests and extend tag functionality
  > 
- [0e8ca543d] (charlie) [2025-12-12]: Merge branch 'master' into refactor/apis-types
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [dd069d08e] (charlie) [2025-12-11]: test(plugins): add create-and-get-tag test for tag creation and retrieval
  > 
- [1d90a43c8] (charlie) [2025-12-11]: refactor(apis): update getBlockProperty return type to null for clarity
  > 
- [1ce04a731] (charlie) [2025-12-11]: refactor(apis): unify block property parameter types for consistency
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [684554a32] (charlie) [2025-12-11]: Merge branch 'master' into refactor/apis-types
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [636c5a29b] (Charlie) [2025-12-10]: refactor(editor): simplify block retrieval and save logic
  > 
- [ca8abd04c] (Charlie) [2025-12-10]: refactor(api): unify block identifier parameter across functions
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/refactor/more-og-removal
- [adf3d99d8] (Gabriel Horner) [2026-01-09]: fix: remove file graph imports
  > Remove mention of removed JSON import in descriptions. Also mv frontend.util.fs
to where they are used instead of keeping an outdated file ns

- [a4baaf37c] (Gabriel Horner) [2026-01-09]: fix: remove file graph :block/file and dependent code
  > Also remove all file graph references in publishing and
add back publishing.db tests with db graph.
Also remove file graph behavior for cmd-k and files

- [761c07444] (Gabriel Horner) [2026-01-09]: chore: remove unused tutorial files
  > 
- [a0e16a46a] (Gabriel Horner) [2026-01-09]: fix: rm file graph config from user config
  > Also remove unused helpers for file config keys and file graph code in
mobile.intent.  These removals also result in removing
frontend.common.file.util as it's no longer used

- [6aca9d45a] (Gabriel Horner) [2026-01-09]: chore: remove file-specific graph-parser code
  > Also removed file graph code for markdown export as a prereq

- [822409b7e] (Gabriel Horner) [2026-01-09]: chore: convert last of frontend tests to db graphs
  > Now able to remove unused frontend.test.* namespaces.
Also removed file-graph specific handling of old attrs like
:block/ref-pages

- [c8229caf0] (Gabriel Horner) [2026-01-09]: fix: remove file graph code from query-dsl
  > Allows removing unused file-rules and common.marker. Also remove
any incorrect or unused references to file graph markers like NOW

- [7a199d69d] (Gabriel Horner) [2026-01-09]: chore: convert query-dsl tests to db graphs
  > Able to remove some dead code and add :build.test/title which
is reusable for future tests

- [e4a0b0170] (Gabriel Horner) [2026-01-09]: chore: convert more frontend tests to db graph
  > Removed file graph specific tests in outliner.core-test and export-test.
Also fixed test for export-test as file graph exports didn't
reference pages in blocks

- [ab4f04f48] (Gabriel Horner) [2026-01-09]: fix: convert model tests to use db graphs
  > Tweaked sqlite.build to allow :block/alias. Also modified
one test's assertions since there aren't pre-blocks anymore.
model tests are running as DB_GRAPH=1 with a hack for now

- [75ac6d14a] (Gabriel Horner) [2026-01-09]: fix: frontend and graph-parser tests and lints
  > Removed file specific test

- [1b9c79587] (Gabriel Horner) [2026-01-09]: fix: remove gp-db ns, especially since it's unused
  > or incorrect in case of graph-view. Removed file graph
specific tests that depended on gp-db

- [c0db1fb1f] (Gabriel Horner) [2026-01-09]: fix: remove needless db-graph? checks in outliner
  > 
- [44728890d] (Gabriel Horner) [2026-01-09]: chore: remove unused file ns graph-parser.cli
  > 
- [30e845b76] (Gabriel Horner) [2026-01-09]: fix: remove lint config for nonexistent fns
  > 
- [cb019e9e5] (Tienson Qin) [2026-01-09]: remove source maps for testing
  > 
- [eb7e52257] (Tienson Qin) [2026-01-08]: disable sentry for any-branch test
  > 
- [b6ca3097d] (Tienson Qin) [2026-01-08]: add workflow to deploy any branch
  > 
- [562a68e0e] (rcmerci) [2026-01-08]: enhance(rtc): move rtc api schema to logseq/logseq-schema repo
  > 
- [c8148f66e] (charlie) [2026-01-07]: chore(libs): add Logseq plugin development agent guide
  > 
- [ffdb990d8] (charlie) [2026-01-07]: chore(libs): rename db_properties_notes to db_properties_guide and update README references
  > 
- [1a390a4a1] (rcmerci) [2026-01-06]: Merge pull request #12304 from logseq/debug/add-tx-log
  > feat(rtc): add debug-log tool for rtc
- [f7e1a3a6f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, only record rtc related tx-data
  > 
- [137e85beb] (rcmerci) [2026-01-06]: fix lint
  > 
- [e5668a469] (rcmerci) [2026-01-06]: fix lint
  > 
- [73888c0a9] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, update schema
  > 
- [1a33a365f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log
  > - integrate `debug-log-db` into GC process
- update schema with `repo` column and `DATETIME`
- export `reset_debug_log_db`

- [27016657f] (Tienson Qin) [2026-01-06]: add debug db tx-log and ws messages tables
  > 
- [6f513fa51] (Tienson Qin) [2026-01-06]: enhance(ux): make it clear that hourly backup is running on desktop
  > 
- [00ccff81e] (Tienson Qin) [2026-01-06]: more file-graph cleanup
  > 
- [23c6f8101] (charlie) [2026-01-06]: enhance(libs): add get_file_content function for retrieving file content
  > 
- [74bfe36d8] (charlie) [2026-01-06]: enhance(libs): update IDBProxy interface and improve documentation
  > 
- [8d7aaa791] (charlie) [2026-01-06]: enhance(apis): add setFileContent method for built-in file paths
  > 
- [6c1b9fe77] (Tienson Qin) [2026-01-06]: Add set_file_content api to update built-in files like custom.js/css
  > 
- [c2582d3c2] (Tienson Qin) [2026-01-06]: fix: available choices still shows other tags' scoped choices
  > 
- [289741069] (Tienson Qin) [2026-01-06]: tweak words for invalid db
  > 
- [574890505] (Gabriel Horner) [2026-01-05]: fix ref-property tests pointing to property rule
  > Also add back property tests as it is still a useful rule

- [6ba05ad2c] (Tienson Qin) [2026-01-05]: add e2e-test for isolated scope poroperties
  > 
- [79f5c6e3c] (Tienson Qin) [2026-01-05]: fix: tag-scoped properties still showing for other tags
  > 
- [28ba74628] (rcmerci) [2026-01-05]: chore(rtc): add fixme comment
  > 
- [1e61ae7dd] (rcmerci) [2026-01-05]: fix: lint
  > 
- [b82683357] (rcmerci) [2026-01-05]: dev: update agents.md
  > 
- [4dc4e1be7] (rcmerci) [2026-01-05]: enhance(rtc): add :add op to replace :move + :update combo
  > 
- [ae8710f76] (rcmerci) [2026-01-05]: dev: update review.md
  > 
- [41cbba368] (Tienson Qin) [2026-01-05]: enhance(ux): flashcards  (#12299)
  > Enhances the flashcard user experience by adding automatic query property management, fixing critical bugs, and improving the UI for managing card sets.

Key changes:

1. Automatic creation of query property blocks when tagging with #Query or subclasses via pipeline
2. Fixed critical bug in api-insert-new-block! where the end? parameter had inverted conditional logic
3. Added ability to create new #Cards blocks directly from the flashcard modal with a plus button
- [1cf4fa9ef] (Tienson Qin) [2026-01-05]: enhance(ux): add highlights to unlinked references
  > 
- [d1bbb5ad1] (charlie) [2026-01-05]: chore(libs): add development notes
  > 
- [2642765eb] (charlie) [2026-01-04]: chore: update db_properties_skill.md with enhanced property schema APIs and examples
  > 
- [a2bfb3963] (charlie) [2026-01-04]: chore: update version to 0.2.11 and add developer notes for Logseq DB properties
  > 
- [4b7d6c73c] (Tienson Qin) [2026-01-04]: fix: remove rtc test on updated-at
  > Since :block/updated-at should only be overwritten, not retracted

- [f940f708b] (Tienson Qin) [2026-01-04]: fix: rtc should never retract block timestamp
  > 
- [8c10cb64d] (Tienson Qin) [2026-01-04]: wrong ns
  > 
- [50e2bc53f] (Tienson Qin) [2026-01-04]: fix: filter out file graphs
  > 
- [a9a9905b0] (Tienson Qin) [2026-01-04]: feat: tag-scoped property choices (#12295)
  > * feat: tag-scoped property choices

* Able to hide global choices per tag

* add e2e tests
- [fd7e68582] (Tienson Qin) [2026-01-04]: chore: cleanup dicts
  > 
- [f43459f55] (Tienson Qin) [2026-01-04]: fix: wrong mobile event
  > 
- [e95c93629] (Tienson Qin) [2026-01-03]: fix: youtube embed on desktop
  > 
- [5705dd359] (Tienson Qin) [2026-01-03]: chore: code cleanup
  > 
- [ce5017fca] (charlie) [2026-01-03]: fix(tests): update tag properties in create tag test
  > 
- [9bd5390c0] (Tienson Qin) [2026-01-03]: enhance: don't close invite dialog
  > 
- [74d8308a6] (Tienson Qin) [2026-01-03]: chore: remove git commit command
  > 
- [0b77ee902] (Tienson Qin) [2026-01-03]: fix: trim blank for inviting email
  > 
- [baff8a4cc] (charlie) [2026-01-03]: enhance(apis): add support for tag properties in createTag API
  > 
- [664743b28] (Tienson Qin) [2026-01-03]: fix: logseq.Editor.renamePage HTTP API hangs indefinitely
  > related to https://github.com/logseq/db-test/issues/701

- [5459dbeb6] (Tienson Qin) [2026-01-03]: enhance: args for tag properties
  > 
- [8fd006bab] (Tienson Qin) [2026-01-03]: enhance: api createTag supports :tagProperties
  > 
- [ebb985f78] (Paweł Świątkowski) [2026-01-02]: Add ability to sort plugins by added date on marketplace (#11993)
  > Co-authored-by: Charlie <xyhp915@qq.com>
- [1cd561dbd] (Tienson Qin) [2026-01-01]: chore: CI cleanup
  > 
- [d4f4ad8f7] (Tienson Qin) [2026-01-01]: chore: bump version for mobile apps
  > 
- [324d02896] (Tienson Qin) [2026-01-01]: chore: remove rsapi from CI
  > 
- [44f26f91f] (Tienson Qin) [2026-01-01]: fix: add back ssl/keystore.jks for mobile development
  > 
- [497645082] (Tienson Qin) [2026-01-01]: fix: lint
  > 
- [51634b4c5] (Tienson Qin) [2026-01-01]: fix: collapse/expand toggle on mobile
  > 
- [4d46c5e9b] (Tienson Qin) [2026-01-01]: fix: youtube timestamp on web/electron
  > 
- [10846fdaa] (Tienson Qin) [2026-01-01]: fix: asset wrong order
  > 
- [7d493575d] (Tienson Qin) [2026-01-01]: fix: add missing datepicker.css
  > 
- [8fc093c1c] (Tienson Qin) [2026-01-01]: fix: can't set number 9 as property value
  > because it's empty-placeholder

- [44468dd20] (charlie) [2026-01-01]: enhance(apis): simplify tag resolution and enhance tag handling functions
  > 
- [5f42c51c3] (charlie) [2026-01-01]: enhance(property): add data attributes for property title and type
  > 
- [460eefb96] (charlie) [2026-01-01]: enhance(apis): add reset option to upsertBlockProperty for block properties
  > 
- [aafa50dcc] (Tienson Qin) [2026-01-01]: fix: :block/warning value should be keyword
  > 
- [6f59a02f5] (Tienson Qin) [2026-01-01]: fix: mark ensure-graph-uuid as deprecated
  > 
- [99dea608f] (rcmerci) [2026-01-01]: fix(ci): update typos.toml
  > 
- [ed1133095] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683 (2)
  > 
- [76224da1b] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683
  > 
- [0f949b739] (Tienson Qin) [2026-01-01]: fix: tests
  > 
- [b55550ed3] (Tienson Qin) [2026-01-01]: add :logseq.kv/local-graph-uuid
  > 
- [3078f2a90] (Tienson Qin) [2025-12-31]: chore: remove outdated test
  > 
- [15585f148] (Tienson Qin) [2025-12-31]: mod+m to publish
  > 
- [f1a410887] (rcmerci) [2025-12-31]: fix(rtc): fix invalid apply-ops msg
  > 
- [76ebafac6] (Tienson Qin) [2025-12-31]: enhance: preserve block collapsed state when publishing
  > 
- [5f532a6a8] (Gabriel Horner) [2025-12-30]: chore: update now outdated parts of db version section
  > 
- [a6503cc2f] (Gabriel Horner) [2025-12-30]: fix: add back validation of properties graph to CI
  > Accidentally taken out w/ last refactor

- [ca14110a8] (Gabriel Horner) [2025-12-30]: fix: nbb scripts failing to start
  > Caused by new dep that isn't configured for use with nbb.
Can add back when it's actually nbb compatible and has a nbb.edn

- [3b1433647] (Tienson Qin) [2025-12-30]: fix: set limit to passsword kdf iterations on cloudflare workers
  > 
- [e5964202d] (Tienson Qin) [2025-12-30]: Merge pull request #12279 from logseq/feat/page-publish
  > feat: page publish 
- [5f1695737] (Tienson Qin) [2025-12-30]: Merge branch 'master' into feat/page-publish
  > 
- [8cfcefcec] (charlie) [2025-12-30]: enhance(apis): add route handler and improve page block retrieval
  > 
- [5376779d4] (Tienson Qin) [2025-12-30]: deploy new version
  > 
- [8de2542f8] (Tienson Qin) [2025-12-30]: use salted PBKDF2 hash
  > 
- [f9a3fb50b] (Tienson Qin) [2025-12-30]: remove skip_auth
  > 
- [918dc0766] (Tienson Qin) [2025-12-30]: fix: set auth to be required on staging and prod
  > 
- [304195c7d] (Tienson Qin) [2025-12-30]: fix: typo
  > 
- [a0a7748e7] (Tienson Qin) [2025-12-30]: home page
  > 
- [adb7b8e36] (Tienson Qin) [2025-12-30]: fix: tagged nodes
  > 
- [dce1a8f4e] (Tienson Qin) [2025-12-30]: fix: e2e tests
  > 
- [6bc2fb1a0] (Tienson Qin) [2025-12-30]: fix: don't show published url property
  > 
- [45d592ecd] (Tienson Qin) [2025-12-30]: enhance: show close button when search box opened
  > 
- [c97453d1d] (Tienson Qin) [2025-12-30]: search
  > 
- [60e79785f] (Tienson Qin) [2025-12-30]: fix: add back missing dicts for flashcard buttons
  > 
- [c21f2c7f4] (Tienson Qin) [2025-12-30]: Ensure local graphs have uuid
  > 
- [644ef4c34] (Tienson Qin) [2025-12-30]: show authors
  > 
- [d53ea4a38] (Tienson Qin) [2025-12-30]: enhance: update published-url to be property type of :url
  > 
- [f8d494285] (Tienson Qin) [2025-12-30]: enhance: show page updated at
  > 
- [24c929835] (Tienson Qin) [2025-12-30]: fix: tests
  > 
- [12f967437] (Tienson Qin) [2025-12-30]: enhance: short url use "/p/short-id" instead of "/s/short-id"
  > 
- [74d136e49] (Tienson Qin) [2025-12-30]: fix: export tests
  > 
- [bdb6e0218] (Tienson Qin) [2025-12-30]: fix: remove icon from page properties area
  > 
- [8a415e007] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/publish.css
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [3e9786593] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/render.cljs
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [0ca329d63] (Tienson Qin) [2025-12-29]: add custom logseq/publish.css and logseq/publish.js
  > 
- [be69c009d] (Tienson Qin) [2025-12-29]: add user page
  > 
- [9dab647d0] (Tienson Qin) [2025-12-29]: add missing meta
  > 
- [da314220d] (Tienson Qin) [2025-12-29]: feat: unpublish
  > 
- [15d7526ac] (Tienson Qin) [2025-12-29]: fix: ast blocks in block title
  > 
- [2068123b2] (Tienson Qin) [2025-12-29]: fix: heading
  > 
- [0c79f4651] (Tienson Qin) [2025-12-29]: fix: cache published page
  > 
- [8c4be25e9] (Tienson Qin) [2025-12-29]: add timestamp version to invalid cache
  > 
- [7b7ec4706] (Tienson Qin) [2025-12-29]: add deploy script
  > 
- [efaff72f0] (Tienson Qin) [2025-12-29]: fix: mobile display issues
  > 
- [6c600d0a8] (Tienson Qin) [2025-12-29]: add dev script
  > 
- [8d6282121] (Tienson Qin) [2025-12-29]: add custom domain
  > 
- [ff56c6cbc] (Tienson Qin) [2025-12-29]: add publish staging and prod
  > 
- [7e695603d] (Tienson Qin) [2025-12-29]: Merge branch 'master' into feat/page-publish
  > 
- [bcc478b5f] (Tienson Qin) [2025-12-29]: refactor: separate og and db version (#12276)
  > separate og and new version apps

remove file sync, tldraw, excalidraw and zotero
- [56d3bd84d] (Tienson Qin) [2025-12-29]: add publish test
  > 
- [5ff22217d] (charlie) [2025-12-29]: fix(ui): adjust padding for property configuration header
  > 
- [5f185a39c] (charlie) [2025-12-29]: chore(libs): upgrade libs core and yarn.lock
  > 
- [62eb31c32] (Tienson Qin) [2025-12-29]: node embed support
  > 
- [78fbbaffa] (Tienson Qin) [2025-12-29]: fix: page icon
  > 
- [ef7cbd743] (Tienson Qin) [2025-12-29]: fix: url type property rendered as link
  > 
- [b1050da84] (Tienson Qin) [2025-12-29]: enhance: icon and tags position
  > 
- [5ed49d737] (Tienson Qin) [2025-12-29]: fix: positioned properties rendering
  > 
- [a04050d98] (Tienson Qin) [2025-12-29]: macro render
  > 
- [58f5a2ddc] (Tienson Qin) [2025-12-29]: block publish
  > 
- [3e033db0b] (Tienson Qin) [2025-12-29]: press `t o` to toggle collapse/expand
  > 
- [6d551e7b6] (Tienson Qin) [2025-12-29]: dark theme
  > 
- [db55b2d5c] (Tienson Qin) [2025-12-29]: style tweaks
  > 
- [a3d717812] (Tienson Qin) [2025-12-29]: multiple size variants for images
  > 
- [e77690b45] (Tienson Qin) [2025-12-28]: feat: password protected page
  > 
- [26c903f53] (Tienson Qin) [2025-12-28]: 404 page
  > 
- [0c668505c] (Tienson Qin) [2025-12-28]: friendly page for non published
  > 
- [326254c8f] (Tienson Qin) [2025-12-28]: style polish
  > 
- [12fabc63c] (Tienson Qin) [2025-12-28]: refactor: separate worker to multiple namespaces
  > 
- [55bf42887] (Tienson Qin) [2025-12-28]: code cleanup
  > 
- [4b2884abe] (Tienson Qin) [2025-12-28]: move inline style and script to publish.css and publish.js
  > 
- [b182a3736] (Tienson Qin) [2025-12-28]: fix: asset upload
  > 
- [cec45205d] (Tienson Qin) [2025-12-28]: wip: assets publish
  > 
- [aab7cfe2d] (Tienson Qin) [2025-12-28]: support math, code and quote blocks
  > 
- [80388f618] (Tienson Qin) [2025-12-28]: shorten url
  > 
- [0d38c982e] (Tienson Qin) [2025-12-28]: global ref
  > 
- [a682b9499] (Tienson Qin) [2025-12-28]: global tag
  > 
- [84cbb7931] (Tienson Qin) [2025-12-28]: tagged nodes
  > 
- [a4275838b] (Tienson Qin) [2025-12-28]: add graph page
  > 
- [08ab09a44] (Tienson Qin) [2025-12-28]: fix: property issues
  > 
- [7ad7e0b60] (Tienson Qin) [2025-12-28]: wip: properties publish
  > 
- [d4d8b1a7e] (Tienson Qin) [2025-12-28]: fix: add shadow-cljs.edn for publish worker
  > 
- [e9c2269e3] (Tienson Qin) [2025-12-28]: linked refs
  > 
- [36408416c] (Tienson Qin) [2025-12-27]: publish mvp
  > 
- [58cda5538] (Tienson Qin) [2025-12-27]: cljs worker
  > 
- [2a9624347] (Tienson Qin) [2025-12-27]: add publish worker
  > 
- [704312b40] (Tienson Qin) [2025-12-27]: Add publish dep
  > 
- [a2a5bfc09] (Tienson Qin) [2025-12-26]: remove temp workflow
  > 
- [c548542b4] (Tienson Qin) [2025-12-26]: add ci tool
  > 
- [7c7fe5e46] (Tienson Qin) [2025-12-26]: bump version to 2
  > 
- [8fe3cddba] (Tienson Qin) [2025-12-26]: enhance: limit access to db graphs only
  > 
- [8b312a403] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [678cb7dda] (Tienson Qin) [2025-12-26]: fix: init
  > 
- [910cd9514] (Tienson Qin) [2025-12-26]: fix: tests
  > 
- [c5c66aadc] (Tienson Qin) [2025-12-26]: update shadow-cljs.edn to remove tldraw and excalidraw
  > 
- [3d78209a6] (charlie) [2025-12-26]: chore(libs):bump new version
  > 
- [ec3ace5bd] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [cb9ec1d49] (Tienson Qin) [2025-12-26]: add sanitize-filename
  > 
- [c9a92f92b] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [c084a6b9d] (Tienson Qin) [2025-12-26]: fix dicts
  > 
- [ec7a325bc] (Tienson Qin) [2025-12-26]: more cleanup
  > 
- [f1de1ea16] (Tienson Qin) [2025-12-26]: remove git, chokidar, rsapi
  > 
- [467c5200d] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [7d719623a] (Tienson Qin) [2025-12-26]: refactor: prefer conn than repo for outliner core and rtc update
  > 
- [f6188b6cc] (Tienson Qin) [2025-12-25]: cleanup more config/db-based-graph? usage
  > 
- [eceaa7396] (Tienson Qin) [2025-12-25]: more cleanup
  > 
- [f419fbea8] (Tienson Qin) [2025-12-25]: cleanup
  > 
- [d8d6e8b4b] (Tienson Qin) [2025-12-25]: remove file based model
  > 
- [acbb03856] (Tienson Qin) [2025-12-25]: remove diff-merge
  > 
- [9f927137a] (Tienson Qin) [2025-12-25]: remove file based handlers
  > 
- [32cbac5a5] (Tienson Qin) [2025-12-25]: remove file based page handler
  > 
- [4aab5daca] (Tienson Qin) [2025-12-25]: add simplified datepicker
  > 
- [bbe6fa861] (Tienson Qin) [2025-12-25]: remove file based commands
  > 
- [8d1fafb4e] (Tienson Qin) [2025-12-25]: remove outdated datepicker component
  > 
- [eaa5a5733] (Tienson Qin) [2025-12-25]: remove file based api
  > 
- [5490824cf] (Tienson Qin) [2025-12-25]: remove old srs implementation
  > 
- [b3813ade7] (Tienson Qin) [2025-12-25]: remove file sync, tldraw, excalidraw and zotero
  > 
- [da06bc2db] (Tienson Qin) [2025-12-25]: wip: separate og and new version apps
  > 
- [fbe6b998b] (Charlie) [2025-12-25]: enhance(plugins): apis related improvements (#12266)
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/test/db
- [c8148f66e] (charlie) [2026-01-07]: chore(libs): add Logseq plugin development agent guide
  > 
- [ffdb990d8] (charlie) [2026-01-07]: chore(libs): rename db_properties_notes to db_properties_guide and update README references
  > 
- [1a390a4a1] (rcmerci) [2026-01-06]: Merge pull request #12304 from logseq/debug/add-tx-log
  > feat(rtc): add debug-log tool for rtc
- [f7e1a3a6f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, only record rtc related tx-data
  > 
- [137e85beb] (rcmerci) [2026-01-06]: fix lint
  > 
- [e5668a469] (rcmerci) [2026-01-06]: fix lint
  > 
- [73888c0a9] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log, update schema
  > 
- [1a33a365f] (rcmerci) [2026-01-06]: dev(rtc): refactor debug-log
  > - integrate `debug-log-db` into GC process
- update schema with `repo` column and `DATETIME`
- export `reset_debug_log_db`

- [27016657f] (Tienson Qin) [2026-01-06]: add debug db tx-log and ws messages tables
  > 
- [6f513fa51] (Tienson Qin) [2026-01-06]: enhance(ux): make it clear that hourly backup is running on desktop
  > 
- [00ccff81e] (Tienson Qin) [2026-01-06]: more file-graph cleanup
  > 
- [23c6f8101] (charlie) [2026-01-06]: enhance(libs): add get_file_content function for retrieving file content
  > 
- [74bfe36d8] (charlie) [2026-01-06]: enhance(libs): update IDBProxy interface and improve documentation
  > 
- [8d7aaa791] (charlie) [2026-01-06]: enhance(apis): add setFileContent method for built-in file paths
  > 
- [6c1b9fe77] (Tienson Qin) [2026-01-06]: Add set_file_content api to update built-in files like custom.js/css
  > 
- [c2582d3c2] (Tienson Qin) [2026-01-06]: fix: available choices still shows other tags' scoped choices
  > 
- [289741069] (Tienson Qin) [2026-01-06]: tweak words for invalid db
  > 
- [574890505] (Gabriel Horner) [2026-01-05]: fix ref-property tests pointing to property rule
  > Also add back property tests as it is still a useful rule

- [6ba05ad2c] (Tienson Qin) [2026-01-05]: add e2e-test for isolated scope poroperties
  > 
- [79f5c6e3c] (Tienson Qin) [2026-01-05]: fix: tag-scoped properties still showing for other tags
  > 
- [28ba74628] (rcmerci) [2026-01-05]: chore(rtc): add fixme comment
  > 
- [1e61ae7dd] (rcmerci) [2026-01-05]: fix: lint
  > 
- [b82683357] (rcmerci) [2026-01-05]: dev: update agents.md
  > 
- [4dc4e1be7] (rcmerci) [2026-01-05]: enhance(rtc): add :add op to replace :move + :update combo
  > 
- [ae8710f76] (rcmerci) [2026-01-05]: dev: update review.md
  > 
- [41cbba368] (Tienson Qin) [2026-01-05]: enhance(ux): flashcards  (#12299)
  > Enhances the flashcard user experience by adding automatic query property management, fixing critical bugs, and improving the UI for managing card sets.

Key changes:

1. Automatic creation of query property blocks when tagging with #Query or subclasses via pipeline
2. Fixed critical bug in api-insert-new-block! where the end? parameter had inverted conditional logic
3. Added ability to create new #Cards blocks directly from the flashcard modal with a plus button
- [1cf4fa9ef] (Tienson Qin) [2026-01-05]: enhance(ux): add highlights to unlinked references
  > 
- [d1bbb5ad1] (charlie) [2026-01-05]: chore(libs): add development notes
  > 
- [2642765eb] (charlie) [2026-01-04]: chore: update db_properties_skill.md with enhanced property schema APIs and examples
  > 
- [a2bfb3963] (charlie) [2026-01-04]: chore: update version to 0.2.11 and add developer notes for Logseq DB properties
  > 
- [4b7d6c73c] (Tienson Qin) [2026-01-04]: fix: remove rtc test on updated-at
  > Since :block/updated-at should only be overwritten, not retracted

- [f940f708b] (Tienson Qin) [2026-01-04]: fix: rtc should never retract block timestamp
  > 
- [8c10cb64d] (Tienson Qin) [2026-01-04]: wrong ns
  > 
- [50e2bc53f] (Tienson Qin) [2026-01-04]: fix: filter out file graphs
  > 
- [a9a9905b0] (Tienson Qin) [2026-01-04]: feat: tag-scoped property choices (#12295)
  > * feat: tag-scoped property choices

* Able to hide global choices per tag

* add e2e tests
- [fd7e68582] (Tienson Qin) [2026-01-04]: chore: cleanup dicts
  > 
- [f43459f55] (Tienson Qin) [2026-01-04]: fix: wrong mobile event
  > 
- [e95c93629] (Tienson Qin) [2026-01-03]: fix: youtube embed on desktop
  > 
- [5705dd359] (Tienson Qin) [2026-01-03]: chore: code cleanup
  > 
- [ce5017fca] (charlie) [2026-01-03]: fix(tests): update tag properties in create tag test
  > 
- [9bd5390c0] (Tienson Qin) [2026-01-03]: enhance: don't close invite dialog
  > 
- [74d8308a6] (Tienson Qin) [2026-01-03]: chore: remove git commit command
  > 
- [0b77ee902] (Tienson Qin) [2026-01-03]: fix: trim blank for inviting email
  > 
- [baff8a4cc] (charlie) [2026-01-03]: enhance(apis): add support for tag properties in createTag API
  > 
- [664743b28] (Tienson Qin) [2026-01-03]: fix: logseq.Editor.renamePage HTTP API hangs indefinitely
  > related to https://github.com/logseq/db-test/issues/701

- [5459dbeb6] (Tienson Qin) [2026-01-03]: enhance: args for tag properties
  > 
- [8fd006bab] (Tienson Qin) [2026-01-03]: enhance: api createTag supports :tagProperties
  > 
- [ebb985f78] (Paweł Świątkowski) [2026-01-02]: Add ability to sort plugins by added date on marketplace (#11993)
  > Co-authored-by: Charlie <xyhp915@qq.com>
- [1cd561dbd] (Tienson Qin) [2026-01-01]: chore: CI cleanup
  > 
- [d4f4ad8f7] (Tienson Qin) [2026-01-01]: chore: bump version for mobile apps
  > 
- [324d02896] (Tienson Qin) [2026-01-01]: chore: remove rsapi from CI
  > 
- [44f26f91f] (Tienson Qin) [2026-01-01]: fix: add back ssl/keystore.jks for mobile development
  > 
- [497645082] (Tienson Qin) [2026-01-01]: fix: lint
  > 
- [51634b4c5] (Tienson Qin) [2026-01-01]: fix: collapse/expand toggle on mobile
  > 
- [4d46c5e9b] (Tienson Qin) [2026-01-01]: fix: youtube timestamp on web/electron
  > 
- [10846fdaa] (Tienson Qin) [2026-01-01]: fix: asset wrong order
  > 
- [7d493575d] (Tienson Qin) [2026-01-01]: fix: add missing datepicker.css
  > 
- [8fc093c1c] (Tienson Qin) [2026-01-01]: fix: can't set number 9 as property value
  > because it's empty-placeholder

- [44468dd20] (charlie) [2026-01-01]: enhance(apis): simplify tag resolution and enhance tag handling functions
  > 
- [5f42c51c3] (charlie) [2026-01-01]: enhance(property): add data attributes for property title and type
  > 
- [460eefb96] (charlie) [2026-01-01]: enhance(apis): add reset option to upsertBlockProperty for block properties
  > 
- [aafa50dcc] (Tienson Qin) [2026-01-01]: fix: :block/warning value should be keyword
  > 
- [6f59a02f5] (Tienson Qin) [2026-01-01]: fix: mark ensure-graph-uuid as deprecated
  > 
- [99dea608f] (rcmerci) [2026-01-01]: fix(ci): update typos.toml
  > 
- [ed1133095] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683 (2)
  > 
- [76224da1b] (rcmerci) [2026-01-01]: test(e2e,rtc): add test for https://github.com/logseq/db-test/issues/683
  > 
- [0f949b739] (Tienson Qin) [2026-01-01]: fix: tests
  > 
- [b55550ed3] (Tienson Qin) [2026-01-01]: add :logseq.kv/local-graph-uuid
  > 
- [3078f2a90] (Tienson Qin) [2025-12-31]: chore: remove outdated test
  > 
- [15585f148] (Tienson Qin) [2025-12-31]: mod+m to publish
  > 
- [f1a410887] (rcmerci) [2025-12-31]: fix(rtc): fix invalid apply-ops msg
  > 
- [76ebafac6] (Tienson Qin) [2025-12-31]: enhance: preserve block collapsed state when publishing
  > 
- [5f532a6a8] (Gabriel Horner) [2025-12-30]: chore: update now outdated parts of db version section
  > 
- [a6503cc2f] (Gabriel Horner) [2025-12-30]: fix: add back validation of properties graph to CI
  > Accidentally taken out w/ last refactor

- [ca14110a8] (Gabriel Horner) [2025-12-30]: fix: nbb scripts failing to start
  > Caused by new dep that isn't configured for use with nbb.
Can add back when it's actually nbb compatible and has a nbb.edn

- [3b1433647] (Tienson Qin) [2025-12-30]: fix: set limit to passsword kdf iterations on cloudflare workers
  > 
- [e5964202d] (Tienson Qin) [2025-12-30]: Merge pull request #12279 from logseq/feat/page-publish
  > feat: page publish 
- [5f1695737] (Tienson Qin) [2025-12-30]: Merge branch 'master' into feat/page-publish
  > 
- [8cfcefcec] (charlie) [2025-12-30]: enhance(apis): add route handler and improve page block retrieval
  > 
- [5376779d4] (Tienson Qin) [2025-12-30]: deploy new version
  > 
- [8de2542f8] (Tienson Qin) [2025-12-30]: use salted PBKDF2 hash
  > 
- [f9a3fb50b] (Tienson Qin) [2025-12-30]: remove skip_auth
  > 
- [918dc0766] (Tienson Qin) [2025-12-30]: fix: set auth to be required on staging and prod
  > 
- [304195c7d] (Tienson Qin) [2025-12-30]: fix: typo
  > 
- [a0a7748e7] (Tienson Qin) [2025-12-30]: home page
  > 
- [adb7b8e36] (Tienson Qin) [2025-12-30]: fix: tagged nodes
  > 
- [dce1a8f4e] (Tienson Qin) [2025-12-30]: fix: e2e tests
  > 
- [6bc2fb1a0] (Tienson Qin) [2025-12-30]: fix: don't show published url property
  > 
- [45d592ecd] (Tienson Qin) [2025-12-30]: enhance: show close button when search box opened
  > 
- [c97453d1d] (Tienson Qin) [2025-12-30]: search
  > 
- [60e79785f] (Tienson Qin) [2025-12-30]: fix: add back missing dicts for flashcard buttons
  > 
- [c21f2c7f4] (Tienson Qin) [2025-12-30]: Ensure local graphs have uuid
  > 
- [644ef4c34] (Tienson Qin) [2025-12-30]: show authors
  > 
- [d53ea4a38] (Tienson Qin) [2025-12-30]: enhance: update published-url to be property type of :url
  > 
- [f8d494285] (Tienson Qin) [2025-12-30]: enhance: show page updated at
  > 
- [24c929835] (Tienson Qin) [2025-12-30]: fix: tests
  > 
- [12f967437] (Tienson Qin) [2025-12-30]: enhance: short url use "/p/short-id" instead of "/s/short-id"
  > 
- [74d136e49] (Tienson Qin) [2025-12-30]: fix: export tests
  > 
- [bdb6e0218] (Tienson Qin) [2025-12-30]: fix: remove icon from page properties area
  > 
- [8a415e007] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/publish.css
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [3e9786593] (Tienson Qin) [2025-12-30]: Update deps/publish/src/logseq/publish/render.cljs
  > Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [0ca329d63] (Tienson Qin) [2025-12-29]: add custom logseq/publish.css and logseq/publish.js
  > 
- [be69c009d] (Tienson Qin) [2025-12-29]: add user page
  > 
- [9dab647d0] (Tienson Qin) [2025-12-29]: add missing meta
  > 
- [da314220d] (Tienson Qin) [2025-12-29]: feat: unpublish
  > 
- [15d7526ac] (Tienson Qin) [2025-12-29]: fix: ast blocks in block title
  > 
- [2068123b2] (Tienson Qin) [2025-12-29]: fix: heading
  > 
- [0c79f4651] (Tienson Qin) [2025-12-29]: fix: cache published page
  > 
- [8c4be25e9] (Tienson Qin) [2025-12-29]: add timestamp version to invalid cache
  > 
- [7b7ec4706] (Tienson Qin) [2025-12-29]: add deploy script
  > 
- [efaff72f0] (Tienson Qin) [2025-12-29]: fix: mobile display issues
  > 
- [6c600d0a8] (Tienson Qin) [2025-12-29]: add dev script
  > 
- [8d6282121] (Tienson Qin) [2025-12-29]: add custom domain
  > 
- [ff56c6cbc] (Tienson Qin) [2025-12-29]: add publish staging and prod
  > 
- [7e695603d] (Tienson Qin) [2025-12-29]: Merge branch 'master' into feat/page-publish
  > 
- [bcc478b5f] (Tienson Qin) [2025-12-29]: refactor: separate og and db version (#12276)
  > separate og and new version apps

remove file sync, tldraw, excalidraw and zotero
- [56d3bd84d] (Tienson Qin) [2025-12-29]: add publish test
  > 
- [5ff22217d] (charlie) [2025-12-29]: fix(ui): adjust padding for property configuration header
  > 
- [5f185a39c] (charlie) [2025-12-29]: chore(libs): upgrade libs core and yarn.lock
  > 
- [62eb31c32] (Tienson Qin) [2025-12-29]: node embed support
  > 
- [78fbbaffa] (Tienson Qin) [2025-12-29]: fix: page icon
  > 
- [ef7cbd743] (Tienson Qin) [2025-12-29]: fix: url type property rendered as link
  > 
- [b1050da84] (Tienson Qin) [2025-12-29]: enhance: icon and tags position
  > 
- [5ed49d737] (Tienson Qin) [2025-12-29]: fix: positioned properties rendering
  > 
- [a04050d98] (Tienson Qin) [2025-12-29]: macro render
  > 
- [58f5a2ddc] (Tienson Qin) [2025-12-29]: block publish
  > 
- [3e033db0b] (Tienson Qin) [2025-12-29]: press `t o` to toggle collapse/expand
  > 
- [6d551e7b6] (Tienson Qin) [2025-12-29]: dark theme
  > 
- [db55b2d5c] (Tienson Qin) [2025-12-29]: style tweaks
  > 
- [a3d717812] (Tienson Qin) [2025-12-29]: multiple size variants for images
  > 
- [e77690b45] (Tienson Qin) [2025-12-28]: feat: password protected page
  > 
- [26c903f53] (Tienson Qin) [2025-12-28]: 404 page
  > 
- [0c668505c] (Tienson Qin) [2025-12-28]: friendly page for non published
  > 
- [326254c8f] (Tienson Qin) [2025-12-28]: style polish
  > 
- [12fabc63c] (Tienson Qin) [2025-12-28]: refactor: separate worker to multiple namespaces
  > 
- [55bf42887] (Tienson Qin) [2025-12-28]: code cleanup
  > 
- [4b2884abe] (Tienson Qin) [2025-12-28]: move inline style and script to publish.css and publish.js
  > 
- [b182a3736] (Tienson Qin) [2025-12-28]: fix: asset upload
  > 
- [cec45205d] (Tienson Qin) [2025-12-28]: wip: assets publish
  > 
- [aab7cfe2d] (Tienson Qin) [2025-12-28]: support math, code and quote blocks
  > 
- [80388f618] (Tienson Qin) [2025-12-28]: shorten url
  > 
- [0d38c982e] (Tienson Qin) [2025-12-28]: global ref
  > 
- [a682b9499] (Tienson Qin) [2025-12-28]: global tag
  > 
- [84cbb7931] (Tienson Qin) [2025-12-28]: tagged nodes
  > 
- [a4275838b] (Tienson Qin) [2025-12-28]: add graph page
  > 
- [08ab09a44] (Tienson Qin) [2025-12-28]: fix: property issues
  > 
- [7ad7e0b60] (Tienson Qin) [2025-12-28]: wip: properties publish
  > 
- [d4d8b1a7e] (Tienson Qin) [2025-12-28]: fix: add shadow-cljs.edn for publish worker
  > 
- [e9c2269e3] (Tienson Qin) [2025-12-28]: linked refs
  > 
- [36408416c] (Tienson Qin) [2025-12-27]: publish mvp
  > 
- [58cda5538] (Tienson Qin) [2025-12-27]: cljs worker
  > 
- [2a9624347] (Tienson Qin) [2025-12-27]: add publish worker
  > 
- [704312b40] (Tienson Qin) [2025-12-27]: Add publish dep
  > 
- [a2a5bfc09] (Tienson Qin) [2025-12-26]: remove temp workflow
  > 
- [c548542b4] (Tienson Qin) [2025-12-26]: add ci tool
  > 
- [7c7fe5e46] (Tienson Qin) [2025-12-26]: bump version to 2
  > 
- [8fe3cddba] (Tienson Qin) [2025-12-26]: enhance: limit access to db graphs only
  > 
- [8b312a403] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [678cb7dda] (Tienson Qin) [2025-12-26]: fix: init
  > 
- [910cd9514] (Tienson Qin) [2025-12-26]: fix: tests
  > 
- [c5c66aadc] (Tienson Qin) [2025-12-26]: update shadow-cljs.edn to remove tldraw and excalidraw
  > 
- [3d78209a6] (charlie) [2025-12-26]: chore(libs):bump new version
  > 
- [ec3ace5bd] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [cb9ec1d49] (Tienson Qin) [2025-12-26]: add sanitize-filename
  > 
- [c9a92f92b] (Tienson Qin) [2025-12-26]: Merge branch 'master' into refactor/separate-og-and-db-version
  > 
- [c084a6b9d] (Tienson Qin) [2025-12-26]: fix dicts
  > 
- [ec7a325bc] (Tienson Qin) [2025-12-26]: more cleanup
  > 
- [f1de1ea16] (Tienson Qin) [2025-12-26]: remove git, chokidar, rsapi
  > 
- [467c5200d] (Tienson Qin) [2025-12-26]: fix: lint
  > 
- [7d719623a] (Tienson Qin) [2025-12-26]: refactor: prefer conn than repo for outliner core and rtc update
  > 
- [f6188b6cc] (Tienson Qin) [2025-12-25]: cleanup more config/db-based-graph? usage
  > 
- [eceaa7396] (Tienson Qin) [2025-12-25]: more cleanup
  > 
- [f419fbea8] (Tienson Qin) [2025-12-25]: cleanup
  > 
- [d8d6e8b4b] (Tienson Qin) [2025-12-25]: remove file based model
  > 
- [acbb03856] (Tienson Qin) [2025-12-25]: remove diff-merge
  > 
- [9f927137a] (Tienson Qin) [2025-12-25]: remove file based handlers
  > 
- [32cbac5a5] (Tienson Qin) [2025-12-25]: remove file based page handler
  > 
- [4aab5daca] (Tienson Qin) [2025-12-25]: add simplified datepicker
  > 
- [bbe6fa861] (Tienson Qin) [2025-12-25]: remove file based commands
  > 
- [8d1fafb4e] (Tienson Qin) [2025-12-25]: remove outdated datepicker component
  > 
- [eaa5a5733] (Tienson Qin) [2025-12-25]: remove file based api
  > 
- [5490824cf] (Tienson Qin) [2025-12-25]: remove old srs implementation
  > 
- [b3813ade7] (Tienson Qin) [2025-12-25]: remove file sync, tldraw, excalidraw and zotero
  > 
- [da06bc2db] (Tienson Qin) [2025-12-25]: wip: separate og and new version apps
  > 
- [fbe6b998b] (Charlie) [2025-12-25]: enhance(plugins): apis related improvements (#12266)
  > 
- [33e34ab1b] (A. S.) [2025-12-25]: Fix vertical line collapse behaviour (#12248)
  > 
- [9df740c79] (Tienson Qin) [2025-12-25]: fix: multi-tabs e2e tests
  > 
- [cf80caebf] (Tienson Qin) [2025-12-25]: feat: db auto backup on desktop (#12275)
  > * feat: db auto backup on desktop
* fix: press delete closes right sidebar

fix https://github.com/logseq/db-test/issues/670

* disable git backup for db graphs since it's confusing

Users can still use external Git for version control.

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
- [afd421090] (Tienson Qin) [2025-12-24]: fix: auto backup
  > 
- [b2497c0cc] (Tienson Qin) [2025-12-24]: fix: pages starting with "@" fail to import from markdown
  > related to https://github.com/logseq/db-test/issues/680

- [a97fec165] (Tienson Qin) [2025-12-24]: fix: can't copy PDF highlights refs
  > fix https://github.com/logseq/db-test/issues/673
fix https://github.com/logseq/db-test/issues/650

- [e29cb6d5a] (Tienson Qin) [2025-12-24]: fix: page reference doesn't recoginize pasted text
  > fix https://github.com/logseq/db-test/issues/671

- [c167b6954] (Tienson Qin) [2025-12-24]: enhance: select match
  > 
- [82a1932e9] (Tienson Qin) [2025-12-24]: fix: copy and pasting blocks with dates does not work correctly
  > related to https://github.com/logseq/db-test/issues/645

- [43d483d35] (Tienson Qin) [2025-12-24]: fix: rtc e2e tests
  > 
- [23d81c48a] (Tienson Qin) [2025-12-24]: fix: e2e tests
  > 
- [77c3ddf49] (Tienson Qin) [2025-12-24]: fix: property value in table view doesn't render for new objects
  > fixes https://github.com/logseq/db-test/issues/675

- [34858d43e] (Tienson Qin) [2025-12-24]: fix(mobile): don't set background color when hovering favorite item
  > 
- [6dacc20fa] (Tienson Qin) [2025-12-24]: fix(mobile): number property cannot be edited if the value is empty
  > fixes https://github.com/logseq/db-test/issues/678

- [8ee2bc0ee] (Tienson Qin) [2025-12-24]: fix: new option should be the first choice
  > 
- [e5982b678] (Tienson Qin) [2025-12-24]: enhance: run db validation before graph EDN export
  > 
- [0b406c487] (Tienson Qin) [2025-12-24]: fix: invalid data
  > 
- [f927ae8b7] (Tienson Qin) [2025-12-24]: fix: e2ee password confirm inconsistent
  > fixes https://github.com/logseq/db-test/issues/681

- [5132eddd9] (Tienson Qin) [2025-12-24]: enhance: enable db backup on desktop
  > 
- [1ae9e2c49] (Tienson Qin) [2025-12-24]: fix: extends `tags` query and support eid|title|db-ident
  > Previously `tags` doesn't return blocks that have children tag(s).

Also, `tags` support both db/ident and block/title to make it easy
to use:
(tags ?b #{144})
(tags ?b #{"Task"})
(tags ?b #{:logseq.class/Task})

- [be26d4278] (Tienson Qin) [2025-12-20]: enhance(ux): prompt user to input encryption password when creating
  > the first sync graph

- [a630835ce] (Gabriel Horner) [2025-12-19]: chore: update libs links so they are compatible with DB
  > 
- [8c2733111] (Gabriel Horner) [2025-12-19]: fix: rename Encryption tab to match description
  > 
- [e0f136a36] (Gabriel Horner) [2025-12-19]: fix: improve wording to describe external asset workflow
  > 
- [aea9f09fd] (Tienson Qin) [2025-12-19]: Simple query performance enhancements (#12262)
  > * perf: separate scalar from ref property query

* fix: query bindings

* perf: separate default value query from others

* fix: import properties first and then other datoms
- [045cd5f53] (Gabriel Horner) [2025-12-18]: enhance: import block tags that are
  > specified via tags property. Fixes
https://github.com/logseq/db-test/issues/434

- [b947f8760] (rcmerci) [2025-12-19]: fix(rtc): clean cached local-t and remote-t while download-graph
  > 
- [8d7ce3989] (rcmerci) [2025-12-19]: test(e2e): add asset-blocks-validate-after-init-downloaded-test
  > 
- [38e3b9980] (Tienson Qin) [2025-12-18]: fix: lint
  > 
- [60f6c7613] (Tienson Qin) [2025-12-18]: perf: linked references with filters
  > 
- [5918429db] (rcmerci) [2025-12-18]: test(rtc,e2e): add paste-multiple-blocks-test
  > 
- [cec20a543] (rcmerci) [2025-12-18]: fix(rtc,asset): :logseq.property.asset/remote-metadata shouldn't be ignored when init-download
  > 
- [020f65b00] (albanobattistella) [2025-12-17]: Update it.edn (#11881)
  > 
- [8da7e7604] (Piotr Narel) [2025-12-17]: Fix: typos in PL translation (#12190)
  > 
- [84db92bc1] (Gabriel Horner) [2025-12-17]: fix: translations from #12258
  > 
- [f88b0c63d] (Laer-tty) [2025-12-17]: Update pt-br translation. (#12258)
  > 
- [87dff14f4] (Gabriel Horner) [2025-12-17]: chore(cli): bump to 0.4.2
  > 
- [ee97b8cda] (Gabriel Horner) [2025-12-17]: enhance(cli): Add -h/--help to commands
  > 
- [3a9b00f21] (Gabriel Horner) [2025-12-17]: chore(cli): changelog for 0.4.2 and tweak option descriptions
  > 
- [8541b7c79] (Tienson Qin) [2025-12-17]: perf: linked refs without filters
  > 
- [260f6511f] (Tienson Qin) [2025-12-17]: fix: recursive alias redirect
  > 
- [954b3b41f] (Tienson Qin) [2025-12-17]: fix: d/entity print
  > 
- [4a2d9c130] (Tienson Qin) [2025-12-17]: fix: e2e tests
  > 
- [584dd3e36] (Gabriel Horner) [2025-12-16]: chore: remove rest of fix-broken-graph
  > Removed in 30068f882eb05ed144480972cf4b00b748361dce for
https://github.com/logseq/db-test/issues/655

- [bc6ab3867] (Tienson Qin) [2025-12-16]: fix: delete page result in invalid data
  > When children blocks have references of their parent page

- [9d49ba6d4] (Gabriel Horner) [2025-12-16]: fix(cli): import-edn and mcp-server commands
  > don't transact refs for local graphs

- [dce3b4111] (Gabriel Horner) [2025-12-16]: fix: refs not built for imported EDN
  > from UI form or when using cli in-app graph

- [f7207d2d2] (Tienson Qin) [2025-12-16]: fix(mobile): cold start capture first doesn't render other journals
  > 
- [7b063be30] (Tienson Qin) [2025-12-16]: fix: don't hide right positioned properties
  > 
- [efc9f36b6] (Tienson Qin) [2025-12-16]: fix(mobile): favorite icon doesn't update status
  > 
- [b9928a150] (Tienson Qin) [2025-12-16]: fix(mobile): press home button in the root tab doesn't scroll to top
  > 
- [6020df880] (Tienson Qin) [2025-12-16]: fix: pages backlink count resets to zero
  > fixes https://github.com/logseq/db-test/issues/634

- [ef8f5caa2] (Tienson Qin) [2025-12-16]: chore: remove debug
  > 
- [fd52fb50c] (Tienson Qin) [2025-12-16]: fix: search highlights doesn't work for pages
  > fix https://github.com/logseq/db-test/issues/633

- [bef3939f3] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (2)
  > add version to encrypted-data vector

- [85211e95d] (rcmerci) [2025-12-16]: enhance(e2ee): update :iterations for en/decrypt fns (1)
  > 
- [60b8e9298] (Tienson Qin) [2025-12-16]: fix(mobile): foldable control
  > 
- [1caefaef6] (Tienson Qin) [2025-12-16]: enhance(mobile): add create new graph link to graph switcher
  > 
- [6eea79313] (Tienson Qin) [2025-12-16]: fix(ios): set keyboard resize to none
  > 
- [50963ed05] (Tienson Qin) [2025-12-16]: fix(mobile): pop to root shouldn't be running on ios
  > 
- [9cfb7f0b2] (Tienson Qin) [2025-12-16]: fix(mobile): clicking icon and properties shouldn't redirect page
  > 
- [135878a78] (Tienson Qin) [2025-12-16]: fix(mobile): swipe on node link opens the node
  > 
- [52dae0fee] (Tienson Qin) [2025-12-16]: fix(iOS): failed to share screenshot
  > related to https://github.com/logseq/db-test/issues/594

- [2801130bb] (Tienson Qin) [2025-12-16]: fix(mobile): duplicate assets created when sharing to mobile
  > 
- [666c50f65] (Tienson Qin) [2025-12-16]: fix: sentry-cli releases
  > 
- [30068f882] (Tienson Qin) [2025-12-16]: chore: remove command fix broken graph
  > 
- [fe55fdf81] (Gabriel Horner) [2025-12-15]: chore: add dev cli task for easier cli testing
  > 
- [b4f3e364e] (Gabriel Horner) [2025-12-15]: fix: duplication of exporter config leads to bugs
  > in script and test. Also fix <export-file not being waited on and new docs
after #12260

- [bcad78227] (rcmerci) [2025-12-16]: fix(rtc): lost first block's rtc-ops when paste multiple blocks (2)
  > - add more tests
- consider more cases

- [33db791ac] (Tienson Qin) [2025-12-15]: fix: import Zotero pdf blocks (#12260)
  > fix: support zotero files when importing pdf annotations
- [270f13f11] (Tienson Qin) [2025-12-15]: fix: support external pdf files when importing from file graphs
  > 
- [208e3df66] (rcmerci) [2025-12-15]: fix(rtc): lost first block's rtc-ops when paste multiple blocks
  > 
- [c9f1920e1] (charlie) [2025-12-15]: enhance(css): improve layout for block title wrap in PDF styling
  > 
- [b10753502] (charlie) [2025-12-15]: fix(pdf): add extern hints for pdfjs apis
  > 
- [98fad292e] (Tienson Qin) [2025-12-15]: fix(regression): pdf annotations in file graphs
  > 
- [bab068ac3] (Tienson Qin) [2025-12-15]: fix: add warning on excalidraw
  > 
- [cf7e51a8c] (Tienson Qin) [2025-12-15]: fix: lint
  > 
- [f77a4ba96] (Tienson Qin) [2025-12-15]: fix: shouldn't retract db/ident when validating and fixing db
  > 
- [1413ab97d] (charlie) [2025-12-15]: enhance(mobile): improve modal handling on back navigation
  > 
- [401fce3a7] (Tienson Qin) [2025-12-15]: fix: show mcp server toggle only on electron
  > fixes https://github.com/logseq/db-test/issues/663

- [81cbc1e70] (Tienson Qin) [2025-12-15]: fix: pages don't have #Page tag when importing from markdown files
  > related to https://github.com/logseq/db-test/issues/641

- [c75322f55] (charlie) [2025-12-13]: enhance(mobile): polish UI spacing and background styling in various components
  > 
- [ff7ecc4ba] (rcmerci) [2025-12-13]: fix(rtc): carefully compare t when add-ops to client-ops
  > 
- [b7cd27a92] (Tienson Qin) [2025-12-13]: fix(android): search crashes
  > 
- [56593a092] (Tienson Qin) [2025-12-13]: fix: keyboard toolbar position on Android (release)
  > 
- [a4d3bd4d9] (Gabriel Horner) [2025-12-12]: fix(cli): CLI can't run in CI envs
  > For now just downgrade desktop check to warning. Decided
against CI flag since that requires more setup for all

- [f910fcfea] (Tienson Qin) [2025-12-13]: Android native UX (#12254)
  > feat: android native UX
- [068dbf266] (Gabriel Horner) [2025-12-12]: fix(dev): add js dev dep to db
  > so that CLI commands can be run from deps/db.
This is especially useful for developing and debugging deps/db based CLI
commands

- [ef96a8d03] (Gabriel Horner) [2025-12-12]: chore: remove db scripts made redundant by CLI
  > 
- [61daa0aff] (rcmerci) [2025-12-12]: ci: separate build and parallel test jobs for rtc-extra-test
  > 
- [fa7c9f102] (rcmerci) [2025-12-12]: ci: add rtc-extra-part2-test to github ci and run in parallel
  > 
- [707ef76ad] (rcmerci) [2025-12-12]: fix(rtc): temp fix custom-decode for :block/name
  > 
- [73ee1bbc1] (rcmerci) [2025-12-12]: test(rtc): add logseq.e2e.rtc-extra-part2-test (1)
  > 
- [529be0496] (Gabriel Horner) [2025-12-11]: fix: build and export of property values
  > for a :many property that has additional attributes
e.g. :build/tags
Another possible fix for https://github.com/logseq/db-test/issues/636

- [75a131238] (rcmerci) [2025-12-11]: enhance(rtc): add more context when apply-ops return :not-found-target-ops
  > 
- [cd36c97f7] (Tienson Qin) [2025-12-11]: remove confusing message
  > 
- [390bf94ca] (Tienson Qin) [2025-12-11]: fix: allow properties to use rename-db-idents
  > 
- [016e63c4a] (Tienson Qin) [2025-12-11]: fix: ensure using :rename-db-idents to rename properties in migrate
  > 
- [2b80c5a04] (Tienson Qin) [2025-12-11]: fix: able to make "Quick add" page invalid
  > fixes https://github.com/logseq/db-test/issues/603

- [eda73175f] (Tienson Qin) [2025-12-11]: fix: ensure :block/title, :block/name are correct for built-in pages
  > 
- [86023329c] (Gabriel Horner) [2025-12-10]: fix: export of property values for :many property
  > Caught by failing properties test :)

- [b696a84ca] (Gabriel Horner) [2025-12-10]: fix: export of property value blocks doesn't
  > keep its uuid when it's referenced as a property value.
Noticed this with a graph from Tienson where a property value had a
view

- [1effbdfad] (Gabriel Horner) [2025-12-10]: fix: referenced property value blocks not exported
  > with their uuid. Possibly affecting
https://github.com/logseq/db-test/issues/636

- [69f0df24f] (rcmerci) [2025-12-11]: fix(rtc): some update-page-ops do not need update-block-attrs
  > 
- [9f6cef29a] (rcmerci) [2025-12-11]: enhance: add :error/path to malli-schema
  > 
- [efdebeaac] (rcmerci) [2025-12-11]: fix(rtc): fix update-page local-op->remote-op
  > 
- [7aea01d77] (charlie) [2025-12-10]: fix(plugins): incorrect plugins setup for mobile web platform
  > 
- [3c79dff81] (Tienson Qin) [2025-12-10]: fix: capture can't open block to edit
  > 
- [ffa695626] (Tienson Qin) [2025-12-10]: fix(mobile): editor toolbar moved webview to top
  > 
- [fae6547fc] (Tienson Qin) [2025-12-10]: fix: remove header title on ipad
  > 
- [3eb0846bb] (Tienson Qin) [2025-12-10]: fix(ios): ensure fallback system image
  > 
- [84df4ad93] (Tienson Qin) [2025-12-10]: fix(ci): remove space
  > 
- [9fc2b2c73] (Tienson Qin) [2025-12-10]: fix: if: must be single-line
  > 
- [636bf9283] (Tienson Qin) [2025-12-10]: fix(mobile): navigation history pop
  > 
- [57a012bcc] (Tienson Qin) [2025-12-10]: rename external-src-to-external-url
  > also fixed the issue and external audio not rendered

- [ad109277b] (charlie) [2025-12-10]: enhance(ci): add push trigger and conditional execution for build-docs workflow
  > 
- [dc4c71208] (charlie) [2025-12-10]: enhance(apis): add note for setBlockIcon method
  > 
- [e8d06f6fa] (charlie) [2025-12-10]: chore(ci): update dependencies and add GitHub Actions workflow for syncing plugin documentation
  > 
- [dcf754329] (Tienson Qin) [2025-12-10]: fix: ui js path
  > 
- [73f8dfc42] (Gabriel Horner) [2025-12-09]: chore: mv db tests to correct project
  > Also tweak test so that errors aren't printed confusingly when tests
pass

- [effcce3a6] (Gabriel Horner) [2025-12-09]: fix: lockfile not updated for latest nbb-logseq
  > 
- [8f4a6d962] (Tienson Qin) [2025-12-09]: chore: remove silkhq components
  > 
- [f594e2034] (Tienson Qin) [2025-12-09]: fix(rtc): batch store and validate db (#12249)
  > * fix(rtc): batch store and validate db

* fix: logseq.db/transact! shouldn't distinct tx-data

since move-op include 2 steps:
1. insert-block
2. update-attrs

This results in db invalid after step 1.

* refactor: add transact-with-temp-conn!

* bump nbb-logseq and add tests for ldb/transact* fns

---------

Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>
Co-authored-by: rcmerci <rcmerci@gmail.com>
- [61d28c307] (charlie) [2025-12-09]: chore(libs): bump version to 0.2.9
  > 
- [e5903e1c2] (charlie) [2025-12-09]: enhance(apis): add tag related apis
  > 
- [5593b9b0b] (Tienson Qin) [2025-12-09]: fix: lint
  > 
- [ddcfe0209] (Tienson Qin) [2025-12-09]: enhance(mobile): show page above block in search results
  > 
- [73a1722bb] (Tienson Qin) [2025-12-09]: add api to support icon and tag extends
  > 
- [4c1b0b5d3] (Tienson Qin) [2025-12-09]: fix: cljs navigation stack
  > 
- [803a37363] (Tienson Qin) [2025-12-09]: enhance(mobile): native search view
  > 
- [74fa40103] (charlie) [2025-12-09]: enhance(property): improve property label rendering with plugin identification
  > 
- [ef211a528] (Charlie) [2025-12-09]: enhance(pdf): assets related improvements (#12244)
  > 
- [a5875420d] (Gabriel Horner) [2025-12-08]: fix: property value blocks with uuid
  > don't generate their uuids or their tags correctly.
This should've been handled in ad64632c1ba739640b4befa56eba6a245cd6f268.
Related to https://github.com/logseq/db-test/issues/636

- [1fe486542] (Gabriel Horner) [2025-12-08]: fix: some validation errors fail to pretty print
  > with error 'RangeError: Maximum call stack size exceeded'.
As reported by Danzu for https://github.com/logseq/db-test/issues/636

- [4209fb4cb] (Tienson Qin) [2025-12-08]: enhance: add errors
  > 
- [1aff68e4b] (rcmerci) [2025-12-08]: script: update create-graph-with-clojuredocs, add author property
  > 
- [05f21dd8b] (Tienson Qin) [2025-12-08]: fix: capture error support extra data
  > 
- [af22f0974] (rcmerci) [2025-12-07]: script: update create-graph-with-clojuredocs
  > 
- [07cc46663] (Tienson Qin) [2025-12-07]: fix(mobile): disable main content horizontal scrollable
  > 
- [bd8ba941e] (Tienson Qin) [2025-12-07]: fix: lint
  > 
- [e90191f63] (rcmerci) [2025-12-07]: script: add create-graph-with-clojuredocs
  > 
- [71866e50e] (Tienson Qin) [2025-12-07]: enhance(mobile): login uses bottom sheet instead of dialog
  > 
- [edb9f914c] (Tienson Qin) [2025-12-07]: enhance: save record audio to quick add when capture
  > 
- [489f7afed] (Tienson Qin) [2025-12-07]: fix(mobile): widget shortcuts
  > 
- [f1f8f22de] (Tienson Qin) [2025-12-07]: fix: slow rendering after searching and back to home
  > 
- [57fcd5996] (Tienson Qin) [2025-12-07]: fix: reset search stack
  > 
- [cbb755617] (Tienson Qin) [2025-12-07]: enhance: mobile multi navigation stacks
  > 
- [4b1d51ae4] (Tienson Qin) [2025-12-07]: multi nav stacks impl
  > 
- [39d834d03] (Tienson Qin) [2025-12-07]: single nav stack impl
  > 

### Branch: upstream/version/file
- [6efedb755] (charlie) [2025-12-09]: enhance(pdf): https://github.com/logseq/logseq/issues/12247
  > 
