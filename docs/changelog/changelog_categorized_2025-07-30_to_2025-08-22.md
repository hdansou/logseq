## New Feature
### Search
- Semantic search (embeddings + keyword hybrid) introduced ([a2eee34d8](https://github.com/logseq/logseq/commit/a2eee34d8))
### Editor & Navigation
- Cmd+K block move command; also enables converting between page and block via #Page tag toggle in same change ([d84d3f965](https://github.com/logseq/logseq/commit/d84d3f965))

## Enhancement
### UX & UI
- Warn users if WebGPU is not supported in the browser ([1cccba390](https://github.com/logseq/logseq/commit/1cccba390))
- Show edit button instead of 00:00 placeholder for datetime property values ([a918e5b6f](https://github.com/logseq/logseq/commit/a918e5b6f))
### Developer Tooling
- Add more workers and DB namespaces to linters for improved static analysis coverage ([f7e32108f](https://github.com/logseq/logseq/commit/f7e32108f))
### Storage / Persistence
- Switch to official @sqlite.org/sqlite-wasm distribution instead of fork ([11eb0f38a](https://github.com/logseq/logseq/commit/11eb0f38a))

## Bug Fix
### Properties & Scheduling
- Add journal day reference only to scheduled & deadline properties (avoid adding to all :datetime properties) ([da73a6c48](https://github.com/logseq/logseq/commit/da73a6c48))
- Application crash when populating node property resolved ([c9e446fd8](https://github.com/logseq/logseq/commit/c9e446fd8))
### Privacy & Networking
- Do not send cookies to YouTube when running in Electron ([e2d8fe308](https://github.com/logseq/logseq/commit/e2d8fe308))
### Build & Tooling
- nbb-logseq bundling & Datascript require issues fixed (#12032) ([707f49bc4](https://github.com/logseq/logseq/commit/707f49bc4))
- Electron release pipeline adjustments ([ab0786090](https://github.com/logseq/logseq/commit/ab0786090))
- App and other builds no longer need deprecated shadow-cljs config hack ([a407035d8](https://github.com/logseq/logseq/commit/a407035d8))
- yarn watch script reliability restored ([cf4153b1c](https://github.com/logseq/logseq/commit/cf4153b1c))
### Editor & Blocks
- Hidden blocks are no longer inferred unexpectedly ([9d594637c](https://github.com/logseq/logseq/commit/9d594637c))
- Prevent invalid page/block conversions and related validation errors (part of multi-change commit) ([d84d3f965](https://github.com/logseq/logseq/commit/d84d3f965))
### Analytics
- Ensure PostHog is defined in release mode ([78a054eec](https://github.com/logseq/logseq/commit/78a054eec))
### UI Polish
- Align checkbox property value to center ([3025c94d2](https://github.com/logseq/logseq/commit/3025c94d2))
### Test / Website Assets
- Remove mobile static from test website build output ([11514a0c3](https://github.com/logseq/logseq/commit/11514a0c3))
### Publishing
- Publishing no longer needs obsolete shadow-cljs config hack ([74fa24cb1](https://github.com/logseq/logseq/commit/74fa24cb1))
- Fix publishing app in production mode & restore watch command ([182a51a85](https://github.com/logseq/logseq/commit/182a51a85))

## Chore
### Platform & Dependencies
- Bump Electron to 37 ([1d9fa6929](https://github.com/logseq/logseq/commit/1d9fa6929))
- Bump Node version to match new Electron runtime ([3700f4bc8](https://github.com/logseq/logseq/commit/3700f4bc8))
### Testing
- Add test for PDF re-upload functionality plus manual test plan ([9c52cf3ff](https://github.com/logseq/logseq/commit/9c52cf3ff))
### Meta / Planning
- Initial plan scaffolding commit ([3e4b8bb72](https://github.com/logseq/logseq/commit/3e4b8bb72))
### Publishing
- Internal *publishing? flag adjustment ([be6929234](https://github.com/logseq/logseq/commit/be6929234))

---

Notes:
- Merge commits without user-facing changes omitted (e.g. sync merges).
- Multi-purpose commit [d84d3f965] spans a new feature plus fixes; listed under New Feature with related fix noted separately.
