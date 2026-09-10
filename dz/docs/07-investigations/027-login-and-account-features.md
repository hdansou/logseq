# Login and Account Features Investigation

> **Last revised:** 2026-09-10
> **Verified against:** `master` @ `be800f1` (2026-09-08). Every claim re-read in source on 2026-09-10; nothing was exercised at runtime. The Open Collective arrangement was supplied on 2026-09-10 and is marked where used.
> **Context:** a user asked why Logseq has a login when graphs are stored locally, and what logging in gets them.

## Question

Logseq keeps graphs on the device. Why does it have a login, and what does a logged-in user get that a logged-out user doesn't?

## Answer

Logging in is optional. The only `logged-in?` checks in the app are in sync, collaboration, encryption, account and header code, so editing, queries, plugins, themes and export work without an account. The account exists for services that run on Logseq's servers, which need an identity to track ownership, access, encryption keys and billing. The app's own summary: *"With a Logseq account, you can access cloud-based services like Logseq Sync and alpha/beta features."* (`src/resources/dicts/en.edn:17`)

| Feature | What it needs | Evidence |
|---|---|---|
| Logseq Sync | Logged in **and** `rtc-group?`; today that means an Open Collective backer or sponsor ([below](#who-is-in-rtc_2025_07_10-today)) | `components/repo.cljs:40-47`, `:248-257` |
| Collaboration | Logged in; the server lets only the graph's manager invite or remove members | `components/settings.cljs:1487-1488`; `deps/db-sync/.../handler/index.cljs:186-191` |
| Encryption settings and keys | Logged in | `components/settings.cljs:1490-1491`; `handler/events/ui.cljs:510-512` |
| Publish pages to logseq.io | Server rejects requests without a valid token | `deps/publish/src/logseq/publish/routes.cljs:64-73` |
| Early access features | `alpha-tester` or `beta-tester` group | `handler/user.cljs:364-376` |

Logging in uploads no graph data. A graph reaches the server only when the user picks "Use Logseq Sync" for it. Logging in does set up encryption keys, which can prompt for a password (see [Encryption](#encryption)).

## Findings

### Login is AWS Cognito; tokens live on the device

- The `:user/login` event opens a login modal on desktop and web, and redirects to the `:user-login` route on native mobile (`handler/events/ui.cljs:421-424`). The modal uses AWS Amplify (`components/user/login.cljs:24`) and hands the session to `login-callback` (`login.cljs:335`, `handler/user.cljs:265-272`). Electron also accepts a `loginCallback` from its main process (`src/main/electron/listener.cljs:86-88`).
- `login-with-username-password-e2e` (`user.cljs:282-306`) is an exported helper for end-to-end tests, not the user login path.
- Tokens are refreshed through `config/OAUTH-DOMAIN` (`user.cljs:184-190`).
- `logged-in?` only checks that a non-blank refresh token exists (`user.cljs:87-90`).
- Tokens are stored in `localStorage` (`user.cljs:92-101`). Desktop also writes them to `~/logseq/auth.json` (`user.cljs:103-125`), which the CLI shares (see [026](026-default-paths-by-platform.md)).
- Login is offered from the header menu when logged out (`components/header.cljs:242-245`), from Settings → Features on desktop and mobile but not web (`settings.cljs:1163-1178`), and from mobile settings (`src/main/mobile/components/settings.cljs:42-43`).

### What happens right after login

`:user/fetch-info-and-graphs` (`handler/events/ui.cljs:500-522`):

1. Fetches user info from `https://api.logseq.com/file-sync/user_info` (`events/ui.cljs:503`; `user.cljs:422-430`, `:509-513`). This is the current user-info endpoint, not a leftover.
2. Tags Sentry error reports with the user UUID (`events/ui.cljs:511`).
3. Enables the sync worker config and ensures the user's RSA keys, for **every** logged-in user, not only sync users (`events/ui.cljs:485-498`, `:512`).
4. For alpha/beta or `rtc-group?` users only: fetches remote graphs, refreshes the graph list, and starts sync if the current graph is remote (`events/ui.cljs:513-522`).

### Sync needs more than a login

`rtc-group?` (`user.cljs:357-362`) is true only in a dev build, when a custom sync server URL is set, or when the user's groups include `team` or `rtc_2025_07_10`. The sync entry points check both conditions:

- the "Use Logseq Sync (Beta testing)" graph menu item (`repo.cljs:248-257`; string `en.edn:785`)
- the remote graphs list (`repo.cljs:329-330`, `:350-352`)
- the header sync indicator, which also requires a remote graph (`header.cljs:98-109`)

A logged-in user outside those groups sees no way to start syncing.

### Who is in `rtc_2025_07_10` today

Until the Pro plan officially launches, Pro access is granted through [Open Collective](https://opencollective.com/logseq). **Backers** pay $5 and **sponsors** pay $15, and both are added to the `rtc_2025_07_10` group.

*Source: supplied to this investigation on 2026-09-10; not found in source. Adding users to the group happens server-side, outside this repo.*

What the repo does show:

- The only code reference to the group is `rtc-group?` (`user.cljs:362`).
- Logged-out users on desktop and mobile see this under the Login button in Settings → Features: *"To access new features before anyone else you must be an Open Collective Sponsor or Backer of Logseq and therefore log in first."* (`settings.cljs:1178`; `en.edn:1730`)
- The README asks for support on Open Collective and thanks sponsors and backers (`README.md:140`, `:195-204`).

### Collaboration

- The Collaboration settings tab appears only when logged in (`settings.cljs:1487-1488`).
- The client shows the invite box to every member of the graph (`settings.cljs:1266-1273`) and shows "Remove access" only to managers, on members (`settings.cljs:1203`, `:1233`).
- The sync server is the real gate. Adding a member, changing a role and granting encryption access all return 403 unless the caller is the graph's manager (`deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs:186-191`, `:218-220`, `:348-350`). A manager can remove any non-manager, and a member can remove themselves (`handler/index.cljs:233-239`).
- Invites go by email to an existing account; an unknown email returns "user not found" (`handler/index.cljs:187-193`).
- Inviting to an encrypted graph also shares the graph key with the invitee (`handler/db_based/sync.cljs:113-118`, `:378`).
- The user who uploads a graph becomes its manager (`handler/index.cljs:134`).

### Encryption

- **Keys are set up at login.** If no valid RSA key pair exists locally or on the server, the worker generates one and, in the app, asks the user for an encryption password (`worker/sync/crypt.cljs:378-398`, `:401-421`). A plain login can therefore produce a password prompt even if the user never syncs.
- **Sync graphs are always encrypted from the app.** The upload dialog is a plain confirm with no encryption choice (`repo.cljs:78-89`). A missing setting counts as encrypted in the UI (`repo.cljs:59-70`), the worker (`worker/sync/upload.cljs:218-222`, `:363`) and the server (`handler/index.cljs:122`). No code in `src/main` sets it to false.
- The server refuses to create an encrypted graph for a user without an uploaded key pair (`handler/index.cljs:131-132`).
- [ADR 0003](../../../docs/adr/0003-optional-sync-graph-encryption.md) proposes letting users turn encryption off at creation (status: Proposed). The worker and server already carry a `graph-e2ee?` flag, but no UI exposes it.
- The Encryption settings tab appears only when logged in (`settings.cljs:1490-1491`). It can create the key pair, and read or change the encryption password (`settings.cljs:1340-1389`).
- Deeper analysis: [RTC E2EE security investigation](rtc-e2ee-security-investigation.md).

### Publishing

- The page menu's Publish item has no login check (`header.cljs:201-203`). The client attaches a `Bearer` token only if one exists (`handler/publish.cljs:325-327`) and sends the owner's `owner_sub` and `owner_username` (`publish.cljs:348-349`).
- The server refuses the upload without a verified JWT (`routes.cljs:64-73`). Deleting a page requires the token's `sub` to match the page's `owner_sub`, otherwise it returns 403 (`routes.cljs:451-482`). Deleting a graph also requires a token (`routes.cljs:484-492`).
- So a logged-out user can open the publish dialog, but the publish fails at the server.

### Plans exist as strings and client-side numbers, not enforcement

- Free: "1 synced graph (up to 50MB, notes only)" and "No asset syncing" (`en.edn:37`, `:42`).
- Pro: "10 synced graphs (up to 5GB each)" and "Sync assets up to 100MB per file" (`en.edn:52`, `:61`).
- The usage panel hard-codes a graph limit of 1 or 10 (`settings.cljs:940`) and a default storage limit of 0.05 or **10** GB (`settings.cljs:951`). The 10 GB contradicts the "5GB each" string.
- `pro-account?` is a Lemon Squeezy status of `active`, `on_trial` or `cancelled`, or a `pro` user group (`settings.cljs:991-993`). "Upgrade" opens a Lemon Squeezy checkout (`user.cljs:316-322`).
- `pro-account?` does not check `rtc_2025_07_10`, so Open Collective backers and sponsors would not count as Pro accounts in this code if the Account tab were switched on.
- None of this is reachable. The usage panel renders only inside the Account tab (`settings.cljs:1021`, `:1524`), which is switched off: `ENABLE-SETTINGS-ACCOUNT-TAB false` (`config.cljs:23`).
- The client shows notifications for `storage-limit` and `graph-count-exceed-limit` 403s (`user.cljs:453-477`; `handler/events/rtc.cljs:61-65`), but only on the `/file-sync/` client, whose one remaining call is `user_info` (`user.cljs:482-513`).
- The DB sync server has no plan concept. `deps/db-sync/src` contains no `plan`, `tier`, `quota`, `UserGroups`, `graph-count` or `storage-limit` references. Graph creation checks only the token, the request body, a duplicate name, and the key pair for encrypted graphs (`handler/index.cljs:114-134`). This agrees with [025](025-rtc-limits-and-quotas.md).

### What logout does

`logout` (`user.cljs:308-314`) clears the encryption password in the worker, clears all tokens (including Cognito's `localStorage` keys, and rewrites `auth.json` on desktop), clears all of `localStorage` and the user info, then publishes `:user/logout`.

## Not verified

- No login was performed and no feature was exercised at runtime.
- The Open Collective arrangement (backers $5, sponsors $15, added to `rtc_2025_07_10` until Pro launches) was supplied, not read from source. The step that adds users to the group is not in this repo, and whether the amounts are monthly was not stated.
- What `https://api.logseq.com/file-sync/user_info` returns, and whether it ever returns the limit 403s, is not in this repo.

## Related

- [025 — RTC Limits and Quotas](025-rtc-limits-and-quotas.md)
- [026 — Default Paths by Platform](026-default-paths-by-platform.md)
- [Sync limits reference](../ddocs/reference/features/sync-limits.md)
- [RTC E2EE security investigation](rtc-e2ee-security-investigation.md)
- [ADR 0003 — Optional encryption when creating a sync graph](../../../docs/adr/0003-optional-sync-graph-encryption.md)
