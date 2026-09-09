---
title: 'User Profiles'
status: 'Active Development'
quadrant: 'Reference'
platform: 'Desktop (macOS/Windows/Linux)'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
related-docs:
  - '../../tutorials/collaboration/real-time-sync.md'
  - '../../explanation/security/e2ee-encryption.md'
  - '../../how-to/collaboration/sharing-graphs.md'
---

> **Last revised:** 2026-09-09

# User Profiles Reference

## Overview

User Profiles in Logseq store identity information for collaboration features, particularly Real-Time Collaboration (RTC). Each user has a profile with their name and email (and optionally an avatar URL), which appear when editing shared graphs.

User profile entities are created automatically when a user first makes an edit in a synced graph. The profile is derived from the user's Logseq account (Cognito) credentials and stored as a page entity tagged with `:logseq.class/Page`.

**Primary Use Cases**:

- Display user identity in collaborative editing sessions
- Show user presence indicators (who's online)
- Attribute edits to specific users via `:logseq.property/created-by-ref` references

## Built-in Properties

Logseq provides three built-in properties for user profiles, defined in the `logseq.property.user/*` namespace:

### `:logseq.property.user/name`

**Purpose**: Display name shown to collaborators

**Schema**:

```clojure
{:title "User Name"
 :schema {:type :string
          :hide? false
          :public? true}}
```

**Details**:

- **Type**: String (any text)
- **Public**: Visible to all collaborators in shared graphs
- **Required**: No (defaults to Cognito username from account token)

**Example Values**:

- `"Alice Johnson"`
- `"Bob Smith"`

**Code Reference**: [property.cljs:620-624](../../../deps/db/src/logseq/db/frontend/property.cljs#L620-L624)

---

### `:logseq.property.user/email`

**Purpose**: Email address for identification

**Schema**:

```clojure
{:title "User Email"
 :schema {:type :string
          :hide? false
          :public? true}}
```

**Details**:

- **Type**: String
- **Public**: Visible to collaborators
- **Required**: Yes (automatically set from Logseq account token)
- **Validation**: No client-side email format validation is performed in the codebase; the value comes directly from the account token

**Example Values**:

- `"alice@example.com"`
- `"bob.smith@company.org"`

**Privacy Note**: Email is shared with collaborators in remote graphs. If privacy is a concern, use a dedicated collaboration email.

**Code Reference**: [property.cljs:625-629](../../../deps/db/src/logseq/db/frontend/property.cljs#L625-L629)

---

### `:logseq.property.user/avatar`

**Purpose**: Profile picture URL for visual identification

**Schema**:

```clojure
{:title "User Avatar"
 :schema {:type :string
          :hide? false
          :public? true}}
```

**Details**:

- **Type**: String (URL to image)
- **Public**: Visible to all collaborators
- **Required**: No (defaults to initials-based avatar)

**Default Behavior**: If no avatar is set, Logseq generates an initials-based avatar using the user's name (e.g., "AJ" for "Alice Johnson") with a dynamically generated background color based on the user's UUID (via the `uniqolor` library in `uuid-color`, `deps/shui/src/logseq/shui/util.cljs:46-52`). Initials are computed grapheme-aware -- two characters for Latin/number names, one otherwise (`initials`, `src/main/frontend/components/avatar.cljs:30-40`).

**Avatar Upload**: Logseq provides a presigned-URL mechanism for uploading avatars to its cloud storage: `<upload-user-avatar` requests a URL from `/logseq/get_presigned_user_avatar_put_url` and PUTs the image to it (`src/main/frontend/handler/user.cljs:392-405`). As of `be800f1711` its only caller is the RTC debug UI (`src/main/frontend/db/rtc/debug_ui.cljs:74`) -- there is no end-user avatar upload control.

**Code Reference**: [property.cljs:630-634](../../../deps/db/src/logseq/db/frontend/property.cljs#L630-L634)

---

## How Profiles Are Created

User profiles are **not** manually configured through a dedicated settings UI. Instead, they are created automatically by the `add-created-by-ref-hook` pipeline in `src/main/frontend/worker/pipeline.cljs:411`:

1. When a user makes an edit in a synced graph, the system reads the user's JWT ID token
2. If no profile entity exists for that user UUID, one is created as a page entity (`:block/tags :logseq.class/Page`, `:block/uuid` = the token's `sub`) with `:logseq.property.user/name` (set to the token's `cognito:username`) and `:logseq.property.user/email` (`gen-created-by-block`, `src/main/frontend/worker/pipeline.cljs:396-408`)
3. The new or existing profile entity is then referenced via `:logseq.property/created-by-ref` on edited blocks (`src/main/frontend/worker/pipeline.cljs:421-434`)

The hook is skipped for undo/redo transactions and for RTC/download-graph transactions (`pipeline.cljs:412-414`).

Note: The avatar property is defined in the schema but is **not** set by `gen-created-by-block` during profile creation.

---

## How Profiles Appear in Collaboration

### User Presence Indicator

In shared graphs, online users appear in the header area. The `rtc-collaborators` component (`src/main/frontend/components/header.cljs:120-149`) displays an avatar for each online user, plus a user-plus button that opens the collaboration members dialog.

**Avatar Display**:

- Initials on a colored background (color derived from user UUID via `uniqolor`)
- The avatar is rendered by `frontend.components.avatar/user-avatar` (`src/main/frontend/components/avatar.cljs:42-62`), which wraps `shui/avatar`, at small sizes (`w-5 h-5` = 20x20px in the header, `header.cljs:144`)
- If `avatar-src` is supplied, an `<img>` is rendered; otherwise the initials fallback is used (`avatar.cljs:58-62`)

**Online User Data**: The presence system provides `:user/email`, `:user/name`, `:user/uuid`, and `:user/editing-block-uuid` for each online user.

### Editing Indicators

When collaborators edit blocks, their avatar appears next to the block being edited. The `editing-user-avatar` function (`src/main/frontend/components/block.cljs:2234-2244`) renders the editing user's initials at `w-4 h-4` with their unique color.

**Cursor Colors**: Each user gets a unique color dynamically generated from their UUID using the `uniqolor` library (not a fixed palette). The color is computed with saturation range [55, 70], lightness 70 and `differencePoint` 60 (`deps/shui/src/logseq/shui/util.cljs:46-52`); it is applied to the fallback background at 50/255 alpha (`avatar.cljs:47-49`).

### Block Attribution

Edits are attributed to users via the `:logseq.property/created-by-ref` property ("Node created by", `deps/db/src/logseq/db/frontend/property.cljs:653`), which references the user's profile entity.

---

## Privacy Considerations

### What Gets Shared

When you join a remote graph, these profile properties are **automatically shared** with all collaborators:

- Name (`:logseq.property.user/name`)
- Email (`:logseq.property.user/email`)
- Avatar URL (`:logseq.property.user/avatar`) if set

**Not shared**:

- Account password (never leaves your device)
- E2EE password (local only)
- Other graphs you own
- Local-only graphs

### Data Visibility

**Who can see your profile**:

- All collaborators in **shared remote graphs** (if you join)
- Logseq servers (for sync coordination)
- Not visible to: Public, search engines, non-collaborators

**Where it's stored**:

- Logseq cloud database (for sync)
- Collaborators' local databases (cached as part of the graph)
- Your local database (original)

### Using Private Email

If you want to collaborate without exposing your personal email:

**Option 1: Create Alias**

- Gmail: Use `yourname+logseq@gmail.com` (delivered to `yourname@gmail.com`)
- ProtonMail, Fastmail: Create dedicated alias

**Option 2: Separate Logseq Account**

- Create new account with a work-specific email
- Use only for collaboration graphs

---

## Advanced Usage

### DataScript Query

You can query user profiles in a graph using DataScript:

```clojure
;; Get all user profiles in graph
[:find ?name ?email
 :where
 [?b :logseq.property.user/name ?name]
 [?b :logseq.property.user/email ?email]]
```

Note: Not all user entities will have an avatar set, so querying for all three fields with an inner join may return fewer results than expected.

### Where Profile Data Is Consumed

Besides the header presence list and block editing indicators, profile properties are read by:

- Comments (`:logseq.property.user/avatar`, `src/main/frontend/components/block/comments_model.cljs:130`)
- The recycle bin / trash view, which shows who deleted a node (`src/main/frontend/components/recycle.cljs:30-33`)

> **Not implemented:** there is no profile settings screen, no `logseq profile ...` CLI command, and no plugin API for reading or writing `logseq.property.user/*`. Profiles are created and maintained entirely by the pipeline hook described above.

### Initial Data Export

User profile datoms are included in the initial data export via `get-all-user-datoms` (`deps/db/src/logseq/db/common/initial_data.cljs:396`, called at `:432`). This function finds all entities with `:logseq.property.user/email` and returns their complete datom sets.

---

## Database Schema Details

### Property Definitions

**Source**: [deps/db/src/logseq/db/frontend/property.cljs:620-634](../../../deps/db/src/logseq/db/frontend/property.cljs#L620-L634)

The user profile properties are defined as part of the `built-in-properties` map. All three share the same schema structure: string type, not hidden, and public.

### DataScript Schema

The user profile properties do **not** have explicit entries in the DataScript schema file (`deps/db/src/logseq/db/frontend/schema.cljs`). They are dynamically registered as part of the built-in properties system. There is no `:db.unique/identity` constraint on `:logseq.property.user/email` in the schema. Instead, uniqueness of user profiles is enforced at the application level by looking up entities by their block UUID (which corresponds to the user's Cognito sub/UUID).

**Key Points**:

- User profile entities are identified by `:block/uuid` (the user's Cognito UUID), which does have `:db.unique/identity`
- All user properties store single string values
- No database-level validation is performed on property values

---

## Related Documentation

**Tutorials**:

- [Getting Started with RTC](../../tutorials/collaboration/real-time-sync.md) -- Learn collaborative editing with profiles

**How-To Guides**:

- [Sharing Graphs](../../how-to/collaboration/sharing-graphs.md) -- Invite collaborators and manage access

**Explanations**:

- [E2EE Encryption](../../explanation/security/e2ee-encryption.md) -- How profiles are encrypted in shared graphs
- [RTC Architecture](../../../dz/docs/01-onboarding/rtc-onboarding.md) -- Technical deep dive into collaboration

**References**:

- [Built-in Properties](../schema/built-in-properties.md) -- All built-in properties including user properties

---

**Last Updated**: September 2026
**Status**: Active Development (Profile features expanding for RTC)
**Feedback**: Report profile issues on [GitHub](https://github.com/logseq/logseq/issues)
