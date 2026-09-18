---
title: 'User Profiles'
status: 'Active Development'
quadrant: 'Reference'
platform: 'Desktop (macOS/Windows/Linux)'
graphType: [db]
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
related-docs:
  - '../../tutorials/collaboration/real-time-sync.md'
  - '../../explanation/security/e2ee-encryption.md'
---

> **Last revised:** 2026-09-11

# User Profiles Reference

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

## Overview

Logseq has **no user-profile feature** in the usual sense. There is no profile settings screen, no display-name or bio field, and no avatar upload control. What exists at `be800f1711` is:

1. **User pages in the graph.** When you edit a DB graph while signed in, Logseq creates a page entity for your account holding `:logseq.property.user/name` and `:logseq.property.user/email`. Blocks you create point at that page through `:logseq.property/created-by-ref`.
2. **Presence identity in synced graphs.** Online users and block-editing indicators use the name and email from each person's account token. This data is not read from the graph.
3. **The sync member list.** Names, emails and roles come from the sync server's `/graphs/:graph-id/members` endpoint.

The CLI's `logseq.cli.profile` namespace is unrelated. It is a "Stage timing profiler for logseq CLI runtime" ([profile.cljs:1-2](../../../../../src/main/logseq/cli/profile.cljs#L1-L2)).

**Primary Use Cases**:

- Attribute blocks to the account that created them (`:logseq.property/created-by-ref`)
- Show who is online and who is editing which block in a synced graph
- Show author names on published pages and in the recycle bin

## Built-in Properties

Three built-in properties live in the `logseq.property.user` namespace. All three are defined with the same schema ([property.cljs:620-634](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L620-L634)).

### `:logseq.property.user/name`

**Purpose**: The account's display name as stored in the graph

**Schema**:

```clojure
{:title "User Name"
 :schema {:type :string
          :hide? false
          :public? true}}
```

**Details**:

- **Type**: String
- **Set by**: the created-by hook, from the account token's `cognito:username` ([pipeline.cljs:396-409](../../../../../src/main/frontend/worker/pipeline.cljs#L396-L409))
- **User-editable**: No UI sets it

**Code Reference**: [property.cljs:620-624](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L620-L624)

---

### `:logseq.property.user/email`

**Purpose**: The account's email address as stored in the graph

**Schema**:

```clojure
{:title "User Email"
 :schema {:type :string
          :hide? false
          :public? true}}
```

**Details**:

- **Type**: String
- **Set by**: the created-by hook, from the account token's `email` claim ([pipeline.cljs:396-409](../../../../../src/main/frontend/worker/pipeline.cljs#L396-L409))
- **Validation**: None. The value is copied from the token.
- **Also used as a marker**: the initial-data loader finds user entities through this attribute ([initial_data.cljs:396-401](../../../../../deps/db/src/logseq/db/common/initial_data.cljs#L396-L401))

**Privacy Note**: The email is stored in the graph itself, so every member of a synced graph receives it. See [Privacy Considerations](#privacy-considerations).

**Code Reference**: [property.cljs:625-629](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L625-L629)

---

### `:logseq.property.user/avatar`

**Purpose**: An image URL for the user

**Schema**:

```clojure
{:title "User Avatar"
 :schema {:type :string
          :hide? false
          :public? true}}
```

**Details**:

- **Type**: String (image URL)
- **Set by**: nothing in the app. The created-by hook does not set it ([pipeline.cljs:396-409](../../../../../src/main/frontend/worker/pipeline.cljs#L396-L409)).
- **Read by**: comment rows ([comments_model.cljs:127-132](../../../../../src/main/frontend/components/block/comments_model.cljs#L127-L132)) and the recycle bin's "deleted by" avatar ([recycle.cljs:26-33](../../../../../src/main/frontend/components/recycle.cljs#L26-L33)). Both pass it to `user-avatar` as `avatar-src`.

**Avatar Upload**: `<upload-user-avatar` requests a presigned URL from `/logseq/get_presigned_user_avatar_put_url` on the account API and PUTs the image there ([user.cljs:392-405](../../../../../src/main/frontend/handler/user.cljs#L392-L405)). It does not write `:logseq.property.user/avatar`. Its only caller is a test button in the RTC debug UI ([debug_ui.cljs:74](../../../../../src/main/frontend/db/rtc/debug_ui.cljs#L74)), so end users have no avatar upload control.

**Code Reference**: [property.cljs:630-634](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L630-L634)

---

### `:logseq.property/created-by-ref`

**Purpose**: Links a block or page to the user entity that created it

```clojure
{:title "Node created by"
 :schema {:type :entity
          :hide? true}
 :queryable? true}
```

**Code Reference**: [property.cljs:653-656](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L653-L656)

---

## How User Pages Are Created

No settings UI configures a profile. The worker pipeline's `add-created-by-ref-hook` creates and links user pages automatically ([pipeline.cljs:411-444](../../../../../src/main/frontend/worker/pipeline.cljs#L411-L444)). It is called for every local transaction ([pipeline.cljs:530](../../../../../src/main/frontend/worker/pipeline.cljs#L530)):

1. If the transaction is not undo, redo, or an incoming sync/download transaction, and a signed-in ID token exists, the hook decodes the token ([pipeline.cljs:413-415](../../../../../src/main/frontend/worker/pipeline.cljs#L413-L415))
2. It looks up the entity whose `:block/uuid` is the token's `sub` ([pipeline.cljs:416](../../../../../src/main/frontend/worker/pipeline.cljs#L416)). If none exists, it adds one ([pipeline.cljs:396-409](../../../../../src/main/frontend/worker/pipeline.cljs#L396-L409)):
   - `:block/uuid` = token `sub`
   - `:block/title` and `:block/name` = token `cognito:username`
   - `:block/tags` = `:logseq.class/Page`
   - `:logseq.property.user/name` = token `cognito:username`
   - `:logseq.property.user/email` = token `email`
3. It sets `:logseq.property/created-by-ref` on newly created entities that don't have one yet ([pipeline.cljs:427-432](../../../../../src/main/frontend/worker/pipeline.cljs#L427-L432)). It also sets it on blocks whose title changes from blank to non-blank ([pipeline.cljs:434-441](../../../../../src/main/frontend/worker/pipeline.cljs#L434-L441)).

The hook does **not** check whether the graph is synced. Any DB graph edited while you are signed in gets your user page and `created-by-ref` links. The page's title is your account username, and nothing later updates the name or email if your account changes.

---

## How Identity Appears in Collaboration

### Online Users in the Header

In a synced graph, the `rtc-collaborators` component shows a user-plus button (opens the Members dialog) and one avatar per online user ([header.cljs:121-150](../../../../../src/main/frontend/components/header.cljs#L121-L150)).

- **Where the data comes from**: the sync server builds each online user from JWT claims: `sub`, `email`, `preferred_username` / `cognito:username` / `username`, and `name` ([presence.cljs:6-19](../../../../../deps/db-sync/src/logseq/db_sync/worker/presence.cljs#L6-L19)). The client turns them into `:user/uuid`, `:user/name` (username, else name, else user id) and `:user/email` ([presence.cljs:61-71](../../../../../src/main/frontend/worker/sync/presence.cljs#L61-L71)). Graph properties are not consulted.
- **Rendering**: `avatar/user-avatar` at `w-5 h-5`, with no image source, so always initials ([header.cljs:144-149](../../../../../src/main/frontend/components/header.cljs#L144-L149))
- **Tooltip**: the user's email through `display-email`, which is masked by default ([header.cljs:147](../../../../../src/main/frontend/components/header.cljs#L147))

### Block Editing Indicators

While another user edits a block, their initials avatar (`w-4 h-4`) appears in that block's control area ([block.cljs:2251-2261](../../../../../src/main/frontend/components/block.cljs#L2251-L2261)). Your own editing is never shown, and only the first editor per block is kept ([events.cljs:425-434](../../../../../src/main/frontend/handler/events.cljs#L425-L434)).

### Members List

**Settings → Collaboration**, or the header's user-plus dialog, lists members with name, email and role ([settings.cljs:1209-1285](../../../../../src/main/frontend/components/settings.cljs#L1209-L1285)). That data comes from the server's members endpoint. The name shown is the username, else the email, else the user id ([sync.cljs:251-282](../../../../../src/main/frontend/handler/db_based/sync.cljs#L251-L282)).

### Avatar Appearance

- **Initials**: `initials` takes two graphemes when the name starts with a Latin letter or a digit, otherwise one, upper-cased ([avatar.cljs:30-40](../../../../../src/main/frontend/components/avatar.cljs#L30-L40))
- **Colour**: `uuid-color` runs `uniqolor` on the user UUID with saturation `[55, 70]`, lightness `70`, `differencePoint` `60` ([util.cljs:46-52](../../../../../deps/shui/src/logseq/shui/util.cljs#L46-L52))
- **Background**: the colour with hex alpha `50` appended, about 31% opacity ([avatar.cljs:45-49](../../../../../src/main/frontend/components/avatar.cljs#L45-L49))
- **Image**: rendered only when a non-empty `avatar-src` is passed ([avatar.cljs:58-59](../../../../../src/main/frontend/components/avatar.cljs#L58-L59)), which the header and block indicators never do

### Email Masking

Emails in the header tooltip and the members list are masked by default. `:ui/mask-email?` defaults to `true` ([state.cljs:464](../../../../../src/main/frontend/state.cljs#L464)), and `mask-email?` treats anything except `false` as "mask" ([util/email.cljs:28-30](../../../../../src/main/frontend/util/email.cljs#L28-L30)). In the members list, an eye button with the tooltip **Show email address** / **Hide email address** reveals one address ([components/email.cljs:36-56](../../../../../src/main/frontend/components/email.cljs#L36-L56); [en.edn:38](../../../../../src/resources/dicts/en.edn#L38), [:57](../../../../../src/resources/dicts/en.edn#L57)). The header tooltip has no reveal.

> **Known defect:** setting `:ui/mask-email? false` in `config.edn` does not unmask emails. Both call sites pass the whole `[:config]` subscription ([components/email.cljs:25-26](../../../../../src/main/frontend/components/email.cljs#L25-L26), [header.cljs:125](../../../../../src/main/frontend/components/header.cljs#L125)). That state is keyed by repo ([state.cljs:491](../../../../../src/main/frontend/state.cljs#L491)), so `(:ui/mask-email? config)` finds no value and masking stays on.

---

## Where the Properties Are Used or Excluded

| Consumer | Behaviour | Source |
| -------- | --------- | ------ |
| Comments | Reads `:logseq.property.user/avatar` of the comment's `created-by-ref` | [comments_model.cljs:127-132](../../../../../src/main/frontend/components/block/comments_model.cljs#L127-L132) |
| Recycle bin | "Deleted by" avatar uses `user/name` (else `:block/title`) and `user/avatar` | [recycle.cljs:26-33](../../../../../src/main/frontend/components/recycle.cljs#L26-L33) |
| Published pages | "By:" lists the titles (usernames) of the `created-by-ref` users of the page and its blocks | [render.cljs:1083-1097](../../../../../deps/publish/src/logseq/publish/render.cljs#L1083-L1097), [:1244](../../../../../deps/publish/src/logseq/publish/render.cljs#L1244) |
| Publish payload | Drops `:logseq.property.user/email` datoms | [worker/publish.cljs:227](../../../../../src/main/frontend/worker/publish.cljs#L227) |
| Initial data load | Includes every datom of every entity with `user/email` | [initial_data.cljs:396-401](../../../../../deps/db/src/logseq/db/common/initial_data.cljs#L396-L401), [:432](../../../../../deps/db/src/logseq/db/common/initial_data.cljs#L432) |
| SQLite graph datom export | Excludes the three user properties and `created-by-ref` | [export.cljs:899-906](../../../../../deps/db/src/logseq/db/sqlite/export.cljs#L899-L906) |
| Markdown mirror | Skips pages that have `user/email` | [markdown_mirror.cljs:552-557](../../../../../src/main/frontend/worker/markdown_mirror.cljs#L552-L557) |
| Class property UI | Excludes the user properties | [handler/property.cljs:40-49](../../../../../src/main/frontend/handler/property.cljs#L40-L49) |

> **Unverified:** whether generic plugin property APIs return `logseq.property.user/*` values. `IAppProxy.getUserInfo` is declared ([LSPlugin.ts:411](../../../../../libs/src/LSPlugin.ts#L411)), but no host implementation was found under `src/main/logseq`.

---

## Privacy Considerations

### What Is Stored in the Graph

A user page stores **only**:

- Your account username (`:block/title`, `:block/name`, `:logseq.property.user/name`)
- Your account email (`:logseq.property.user/email`)

In a synced graph these datoms replicate like any other data. Every member can see and query them, and they sit in each member's local copy.

**End-to-end encryption does not cover them.** E2EE encrypts only `:block/title` and `:block/name` values ([const.cljs:25-27](../../../../../src/main/frontend/worker/sync/const.cljs#L25-L27)). The username in the title is encrypted, but `:logseq.property.user/name` and `:logseq.property.user/email` are not in that set.

### Outside the Graph

- The sync server knows each connected user's token claims and broadcasts name, username and email to online members ([presence.cljs:6-19](../../../../../deps/db-sync/src/logseq/db_sync/worker/presence.cljs#L6-L19), [:38-40](../../../../../deps/db-sync/src/logseq/db_sync/worker/presence.cljs#L38-L40))
- Published pages show author **usernames** but strip emails (table above)

### Limiting Exposure

The stored email is your Logseq account email. If you don't want collaborators to see a particular address, sign in with an account that uses an address you are willing to share.

---

## Advanced Usage

### Datalog Query

The hook stores `user/name` and `user/email` as plain strings ([pipeline.cljs:408-409](../../../../../src/main/frontend/worker/pipeline.cljs#L408-L409)), so they can be matched directly:

```clojure
;; All user pages in the graph
[:find ?name ?email
 :where
 [?u :logseq.property.user/name ?name]
 [?u :logseq.property.user/email ?email]]
```

```clojure
;; Blocks created by a given username
[:find (pull ?b [:block/uuid :block/title])
 :where
 [?u :logseq.property.user/name "alice"]
 [?b :logseq.property/created-by-ref ?u]]
```

Because nothing sets `user/avatar`, adding it as a required clause will usually return nothing.

---

## Database Schema Details

### Property Definitions

**Source**: [deps/db/src/logseq/db/frontend/property.cljs:620-634](../../../../../deps/db/src/logseq/db/frontend/property.cljs#L620-L634)

All three user properties are entries in the `built-in-properties` map, with the same schema: string type, not hidden, public.

### DataScript Schema

The user properties have no entries in `deps/db/src/logseq/db/frontend/schema.cljs`, and `:logseq.property.user/email` carries no uniqueness constraint. User entities are unique because the hook looks them up by `[:block/uuid <token sub>]` ([pipeline.cljs:416](../../../../../src/main/frontend/worker/pipeline.cljs#L416)), and `:block/uuid` is `:db.unique/identity` ([schema.cljs:61](../../../../../deps/db/src/logseq/db/frontend/schema.cljs#L61)).

**Key Points**:

- A user entity is identified by `:block/uuid`, which equals the account's token `sub`
- All user properties hold single string values
- No validation is applied to the values

---

## Related Documentation

**Tutorials**:

- [Getting Started with Real-Time Collaboration](../../tutorials/collaboration/real-time-sync.md): presence, members, roles

**Explanations**:

- [E2EE Encryption](../../explanation/security/e2ee-encryption.md): what is and isn't encrypted
- [RTC Architecture](../../../01-onboarding/rtc-onboarding.md): developer onboarding for sync

---

**Last Updated**: 2026-09-11
**Status**: Active Development
**Feedback**: Report issues on [GitHub](https://github.com/logseq/logseq/issues)
