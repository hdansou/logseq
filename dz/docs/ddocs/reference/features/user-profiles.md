---
title: 'User Profiles'
status: 'Active Development'
quadrant: 'Reference'
platform: 'Desktop (macOS/Windows/Linux)'
related-docs:
  - '../../tutorials/collaboration/real-time-sync.md'
  - '../../explanation/security/e2ee-encryption.md'
  - '../../how-to/collaboration/sharing-graphs.md'
---

> **Last revised:** 2026-03-10

# User Profiles Reference

## Overview

User Profiles in Logseq store identity information for collaboration features, particularly Real-Time Collaboration (RTC). Each user has a profile with their name and email (and optionally an avatar URL), which appear when editing shared graphs.

User profile entities are created automatically when a user first makes an edit in a synced graph. The profile is derived from the user's Logseq account (Cognito) credentials and stored as a page entity tagged with `:logseq.class/Page`.

**Primary Use Cases**:

- Display user identity in collaborative editing sessions
- Show user presence indicators (who's online)
- Attribute edits to specific users via `:logseq.property/created-by` references

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

**Code Reference**: [property.cljs:580-584](../../../deps/db/src/logseq/db/frontend/property.cljs#L580-L584)

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

**Code Reference**: [property.cljs:585-589](../../../deps/db/src/logseq/db/frontend/property.cljs#L585-L589)

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

**Default Behavior**: If no avatar is set, Logseq generates an initials-based avatar using the user's name (e.g., "AJ" for "Alice Johnson") with a dynamically generated background color based on the user's UUID (via the `uniqolor` library in `shui-util/uuid-color`).

**Avatar Upload**: Logseq provides a presigned URL mechanism for uploading avatars to its cloud storage. See `new-task--upload-user-avatar` in `src/main/frontend/handler/user.cljs`.

**Code Reference**: [property.cljs:590-594](../../../deps/db/src/logseq/db/frontend/property.cljs#L590-L594)

---

## How Profiles Are Created

User profiles are **not** manually configured through a dedicated settings UI. Instead, they are created automatically by the `add-created-by-ref-hook` pipeline in `src/main/frontend/worker/pipeline.cljs`:

1. When a user makes an edit in a synced graph, the system reads the user's JWT ID token
2. If no profile entity exists for that user UUID, one is created as a page entity with `:logseq.property.user/name` (set to the Cognito username) and `:logseq.property.user/email` (from the token)
3. The new or existing profile entity is then referenced via `:logseq.property/created-by` on edited blocks

Note: The avatar property is defined in the schema but is **not** automatically set during profile creation.

---

## How Profiles Appear in Collaboration

### User Presence Indicator

In shared graphs, online users appear in the header area. The `rtc-collaborators` component in `src/main/frontend/components/header.cljs` displays avatars for each online user.

**Avatar Display**:

- Initials on a colored background (color derived from user UUID via `uniqolor`)
- The avatar is rendered using the `shui/avatar` component at small sizes (e.g., `w-5 h-5` = 20x20px in the header)

**Online User Data**: The presence system provides `:user/email`, `:user/name`, `:user/uuid`, and `:user/editing-block-uuid` for each online user.

### Editing Indicators

When collaborators edit blocks, their avatar appears next to the block being edited. The `editing-user-avatar` function in `src/main/frontend/components/block.cljs` renders the editing user's initials with their unique color.

**Cursor Colors**: Each user gets a unique color dynamically generated from their UUID using the `uniqolor` library (not a fixed palette). The color is computed with saturation range [55, 70] and lightness 70.

### Block Attribution

Edits are attributed to users via the `:logseq.property/created-by` property, which references the user's profile entity.

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

### Initial Data Export

User profile datoms are included in the initial data export via `get-all-user-datoms` in `deps/db/src/logseq/db/common/initial_data.cljs`. This function finds all entities with `:logseq.property.user/email` and returns their complete datom sets.

---

## Database Schema Details

### Property Definitions

**Source**: [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs#L580-L594)

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

**Status**: Active Development (Profile features expanding for RTC)
**Feedback**: Report profile issues on [GitHub](https://github.com/logseq/logseq/issues)
