---
title: 'User Profiles'
status: '🟡 Active Development'
version: 'Since v65.12'
quadrant: 'Reference'
platform: 'Desktop (macOS/Windows/Linux)'
related-docs:
  - '../../tutorials/collaboration/real-time-sync.md'
  - '../../explanation/security/e2ee-encryption.md'
  - '../../how-to/collaboration/sharing-graphs.md'
---

# User Profiles Reference

## Overview

User Profiles in Logseq store identity information for collaboration features, particularly Real-Time Collaboration (RTC). Each user has a profile with their name, email, and avatar, which appear when editing shared graphs.

**Primary Use Cases**:

- Display user identity in collaborative editing sessions
- Show user presence indicators (who's online)
- Attribute edits to specific users
- Enable @mentions in shared graphs

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
- **Required**: No (defaults to account email if not set)
- **Length**: 1-100 characters (recommended < 50 for UI)

**Example Values**:

- `"Alice Johnson"`
- `"Bob Smith"`
- `"李明"` (Unicode supported)

**Code Reference**: [property.cljs:574-578](../../../deps/db/src/logseq/db/frontend/property.cljs#L574-L578)

---

### `:logseq.property.user/email`

**Purpose**: Email address for contact and identification

**Schema**:

```clojure
{:title "User Email"
 :schema {:type :string
          :hide? false
          :public? true}}
```

**Details**:

- **Type**: String (email format expected but not enforced)
- **Public**: Visible to collaborators
- **Required**: Yes (automatically set from Logseq account)
- **Validation**: Client-side email format validation (regex: `/^[^@]+@[^@]+\.[^@]+$/`)

**Example Values**:

- `"alice@example.com"`
- `"bob.smith@company.org"`

**Privacy Note**: Email is shared with collaborators in remote graphs. If privacy is a concern, use a dedicated collaboration email.

**Code Reference**: [property.cljs:579-583](../../../deps/db/src/logseq/db/frontend/property.cljs#L579-L583)

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
- **Format**: HTTP/HTTPS URL or data URI
- **Recommended Size**: 128x128px (displayed at 32x32px, 48x48px, 64x64px)
- **Supported Formats**: PNG, JPG, GIF, SVG, WebP

**Example Values**:

- `"https://avatars.githubusercontent.com/u/12345?v=4"`
- `"https://gravatar.com/avatar/abc123"`
- `"data:image/svg+xml,<svg>...</svg>"` (embedded SVG)

**Default Behavior**: If no avatar set, Logseq generates initials-based avatar (e.g., "AJ" for "Alice Johnson" on colored background).

**Code Reference**: [property.cljs:584-588](../../../deps/db/src/logseq/db/frontend/property.cljs#L584-L588)

---

## Setting Up Your Profile

### Via Settings UI

1. Open **Settings** (⚙️ icon)
2. Navigate to **Account** tab
3. Edit profile fields:

   ```txt
   ┌────────────────────────────────────────────┐
   │  Your Profile                              │
   ├────────────────────────────────────────────┤
   │                                            │
   │  Name:    [Alice Johnson              ]   │
   │           (Shown to collaborators)         │
   │                                            │
   │  Email:   [alice@example.com          ]   │
   │           (Read-only from account)         │
   │                                            │
   │  Avatar:  [https://...                ]   │
   │           or [Choose File]                 │
   │                                            │
   │           [Preview: 👤 AJ]                 │
   │                                            │
   │  [Save Changes]                            │
   └────────────────────────────────────────────┘
   ```

4. Click **Save Changes**

**Changes Apply**:

- Immediately in new collaboration sessions
- May take 5-10 seconds to update in active sessions (WebSocket propagation)

### Via Properties Panel

You can also edit your profile directly in the graph (advanced):

1. Go to page: `[[logseq.property.user/profile]]`
2. Edit properties:

   ```markdown
   - logseq.property.user/name:: Alice Johnson
   - logseq.property.user/email:: alice@example.com
   - logseq.property.user/avatar:: https://avatars.github.com/u/12345
   ```

3. Changes sync automatically

⚠️ **Note**: Editing via properties panel is not recommended for beginners. Use Settings UI instead.

### Uploading Custom Avatar

**Option 1: Use Existing Image URL**

If you have an image hosted online:

1. Copy image URL (right-click image → "Copy image address")
2. Paste into Avatar field
3. Preview appears automatically

**Option 2: Upload New Image**

1. Click **Choose File** button
2. Select image from your computer (PNG, JPG, GIF, WebP, SVG)
3. Image uploads to Logseq asset storage
4. URL auto-filled in Avatar field

**Option 3: Use Gravatar**

If you have a Gravatar account:

1. Get your Gravatar URL: `https://gravatar.com/avatar/YOUR_HASH`
2. Paste into Avatar field
3. Your Gravatar syncs across all Logseq graphs

**Option 4: Embed SVG (Advanced)**

For programmatically generated avatars:

```markdown
logseq.property.user/avatar:: data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128"><circle cx="64" cy="64" r="64" fill="%234F46E5"/><text x="64" y="64" font-size="48" fill="white" text-anchor="middle" dominant-baseline="central">AJ</text></svg>
```

**Image Guidelines**:

- **Size**: 128x128px (will be scaled down)
- **File size**: < 500 KB (larger images slow down sync)
- **Format**: PNG with transparency recommended
- **Aspect ratio**: Square (1:1) for best display

---

## How Profiles Appear in Collaboration

### User Presence Indicator

In shared graphs, you'll see online users at the top-right:

```txt
┌──────────────────────────────────────────────────────────┐
│  🟢 👤 You    🟢 🧑 Alice Johnson    🟢 👨 Bob Smith    │
└──────────────────────────────────────────────────────────┘
```

**Avatar Display**:

- Custom avatar (if set)
- Initials on colored background (if no avatar)
- Green dot 🟢 = online
- Gray dot ⚪ = idle (5+ min inactive)
- No indicator = offline

### Cursor Labels

When collaborators edit blocks, you see their cursor:

```txt
┌──────────────────────────────────────────────────┐
│  - This is a block [🟦 Alice is typing...]      │
└──────────────────────────────────────────────────┘
```

**Cursor Colors**: Each user gets a unique color:

- User 1: 🟦 Blue (`#3B82F6`)
- User 2: 🟩 Green (`#10B981`)
- User 3: 🟪 Purple (`#8B5CF6`)
- User 4: 🟧 Orange (`#F59E0B`)
- User 5: 🟥 Red (`#EF4444`)
- User 6+: Colors cycle

**Hover for Details**:

```txt
┌────────────────────────────────┐
│  👤 Alice Johnson              │
│  📧 alice@example.com          │
│  🕐 Last active: 2 seconds ago │
└────────────────────────────────┘
```

### Block Attribution

In graph history (if enabled), edits are attributed to users:

```markdown
- [[2024-01-20]] Meeting notes
  - Edited by: [[logseq.property.user/alice@example.com]]
  - Changes: Added action items
```

**Future Feature** (🟡 In Development): Full edit history with per-user diffs.

---

## Privacy Considerations

### What Gets Shared

When you join a remote graph, these profile properties are **automatically shared** with all collaborators:

- ✅ Name (`:logseq.property.user/name`)
- ✅ Email (`:logseq.property.user/email`)
- ✅ Avatar URL (`:logseq.property.user/avatar`)

**Not shared**:

- ❌ Account password (never leaves your device)
- ❌ E2EE password (local only)
- ❌ Other graphs you own
- ❌ Local-only graphs

### Data Visibility

**Who can see your profile**:

- All collaborators in **shared remote graphs** (if you join)
- Logseq servers (for sync coordination, but E2EE encrypts content)
- Not visible to: Public, search engines, non-collaborators

**Where it's stored**:

- Logseq cloud database (for sync)
- Collaborators' local databases (cached)
- Your local database (original)

### Using Private Email

If you want to collaborate without exposing your personal email:

**Option 1: Create Alias**

- Gmail: Use `yourname+logseq@gmail.com` (delivered to `yourname@gmail.com`)
- ProtonMail, Fastmail: Create dedicated alias

**Option 2: Separate Logseq Account**

- Create new account: `yourname-work@example.com`
- Use only for collaboration graphs

**Option 3: Request Privacy Feature** (🟡 Planned)

- Future: Option to hide email from collaborators (show only name/avatar)

---

## Comparison with Other Platforms

| Feature          | Logseq                    | Notion          | Obsidian Publish | Confluence      |
| ---------------- | ------------------------- | --------------- | ---------------- | --------------- |
| Display Name     | ✅ Yes                    | ✅ Yes          | ❌ No            | ✅ Yes          |
| Email Shown      | ✅ Yes (to collaborators) | ✅ Yes          | ❌ N/A           | ✅ Yes          |
| Custom Avatar    | ✅ Yes (URL)              | ✅ Yes (upload) | ❌ No            | ✅ Yes (upload) |
| Gravatar Support | ✅ Yes (manual)           | ✅ Yes (auto)   | ❌ No            | ✅ Yes (auto)   |
| Privacy Controls | ⚠️ Limited                | ✅ Granular     | ✅ Anonymous     | ✅ Granular     |
| Cursor Colors    | ✅ Yes                    | ✅ Yes          | ❌ No            | ✅ Yes          |

**Logseq's Strengths**:

- Simple profile setup (3 fields only)
- E2EE protects profile data in transit
- Flexible avatar sources (URL, upload, Gravatar, SVG)

**Logseq's Limitations**:

- No granular privacy controls yet (can't hide email from collaborators)
- No automatic Gravatar detection (must paste URL manually)
- No bio/status/role fields (planned for future)

---

## Advanced Usage

### Programmatic Profile Updates

For developers automating Logseq workflows:

**Via Plugin API** (JavaScript):

```javascript
// Update user profile
await logseq.Editor.upsertBlockProperty(
  'logseq.property.user/profile',
  'logseq.property.user/name',
  'Alice Johnson (Bot)'
)

// Get current user profile
const profile = await logseq.App.getCurrentGraph()
console.log(profile.user)
// {
//   name: "Alice Johnson",
//   email: "alice@example.com",
//   avatar: "https://..."
// }
```

**Via CLI** (for CI/CD automation):

```bash
# Set profile name via Logseq CLI
logseq profile set-name "CI Bot"
logseq profile set-avatar "https://cdn.example.com/bot-avatar.png"
```

**Via DataScript Query**:

```clojure
;; Get all user profiles in graph
[:find ?name ?email ?avatar
 :where
 [?b :logseq.property.user/name ?name]
 [?b :logseq.property.user/email ?email]
 [?b :logseq.property.user/avatar ?avatar]]
```

### Multi-Device Sync

User profiles sync automatically across devices:

1. Update profile on Device A (e.g., desktop)
2. Logseq syncs profile to cloud (< 5 seconds)
3. Device B (e.g., laptop) receives update via WebSocket
4. Profile appears updated on Device B immediately

**Storage Locations**:

- **Cloud**: Logseq servers (encrypted if E2EE enabled)
- **Local**: `~/.logseq/graphs/<graph-uuid>/user-profile.transit`
- **Cache**: IndexedDB in browser/Electron

### Team Avatars (Organizations)

For teams using Logseq:

**Option 1: Unified Avatar Style**

- Use same avatar generator (e.g., DiceBear) with team color scheme
- Example: All avatars use blue background with white initials

**Option 2: Company Gravatar**

- Set up company Gravatar account
- All team members use `@company.com` email
- Avatars automatically match company branding

**Option 3: Bot Accounts**

- For automated workflows (CI/CD, bots)
- Use distinctive robot avatar
- Name clearly indicates non-human: "GitHub Bot", "CI/CD Runner"

---

## Troubleshooting

### Avatar Not Displaying

**Problem**: Avatar URL set but shows initials instead

**Possible Causes**:

1. **URL unreachable**: Check if image loads in browser
2. **CORS blocked**: Avatar host doesn't allow cross-origin requests
3. **Invalid format**: Logseq supports PNG, JPG, GIF, SVG, WebP only
4. **File too large**: Images > 2 MB may fail to load

**Solutions**:

- Test URL in browser: Open in new tab
- Use Logseq asset storage: Upload via **Choose File** button
- Use data URI for small images: `data:image/png;base64,...`
- Check browser console for CORS errors: Help → Toggle Developer Tools → Console

### Name Not Updating for Collaborators

**Problem**: Changed name but collaborators still see old name

**Causes**:

1. Collaborators haven't refreshed (cached profile)
2. Network delay (WebSocket propagation takes 5-10 seconds)
3. E2EE key not updated (if graph is encrypted)

**Solutions**:

1. Wait 30 seconds for propagation
2. Ask collaborators to refresh page (Cmd+R / Ctrl+R)
3. Force sync: Settings → Sync → **Force Full Sync**
4. Check network status: Settings → Sync → **Sync Status**

### Email Cannot Be Changed

**Problem**: Email field grayed out in Settings

**Explanation**: Email is tied to your Logseq account and cannot be changed independently. To use a different email:

**Option 1: Update Logseq Account Email**

1. Go to [logseq.com/account](https://logseq.com/account)
2. Update email address
3. Verify new email
4. Profile email updates automatically

**Option 2: Create New Account**

- For separation of personal/work identities
- Create new Logseq account with desired email
- Re-invite to shared graphs

---

## Future Enhancements

### Planned Features (🟡 Active Development)

**User Status**:

```markdown
logseq.property.user/status:: "🏖️ On vacation until Jan 25"
```

- Display status in user presence indicator
- Auto-clear after expiry date

**User Roles**:

```markdown
logseq.property.user/role:: "Editor" | "Viewer" | "Admin"
```

- Permission-based access control
- Restrict editing to specific roles

**User Bio**:

```markdown
logseq.property.user/bio:: "Product Manager | Coffee enthusiast"
```

- Display in hover tooltip
- Useful for larger teams

**Automatic Gravatar**:

- Detect email domain
- Auto-populate avatar from Gravatar if available
- Fallback to initials if not found

### Research Areas

- **Profile Encryption**: Option to hide profile from server (E2EE for metadata)
- **Verified Identities**: Badge for verified accounts (email confirmation)
- **Custom Fields**: User-defined profile properties
- **Profile Templates**: Organizational templates for consistent profiles

---

## Related Documentation

**Tutorials**:

- [Getting Started with RTC](../../tutorials/collaboration/real-time-sync.md) — Learn collaborative editing with profiles

**How-To Guides**:

- [Sharing Graphs](../../how-to/collaboration/sharing-graphs.md) — Invite collaborators and manage access
- [Managing Team Workflows](../../how-to/collaboration/team-workflows.md) — Best practices for teams

**Explanations**:

- [E2EE Encryption](../../explanation/security/e2ee-encryption.md) — How profiles are encrypted in shared graphs
- [RTC Architecture](../../../dz/docs/01-onboarding/rtc-onboarding.md) — Technical deep dive into collaboration

**References**:

- [Built-in Properties](../schema/built-in-properties.md) — All built-in properties including user properties
- [Schema Guide](../schema/overview.md) — Understanding Logseq's database schema

---

## API Reference

### Property Definitions

**Source**: [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs#L574-L588)

```clojure
(def built-in-properties
  {;; ... other properties ...

   :logseq.property.user/name
   {:title "User Name"
    :schema {:type :string
             :hide? false
             :public? true}}

   :logseq.property.user/email
   {:title "User Email"
    :schema {:type :string
             :hide? false
             :public? true}}

   :logseq.property.user/avatar
   {:title "User Avatar"
    :schema {:type :string
             :hide? false
             :public? true}}})
```

### DataScript Schema

```clojure
{:logseq.property.user/name {:db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}
 :logseq.property.user/email {:db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one
                               :db/unique :db.unique/identity}
 :logseq.property.user/avatar {:db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one}}
```

**Key Points**:

- Email is `:db.unique/identity` — ensures one profile per email
- All properties are `:db.cardinality/one` — single value only
- String type allows any text (validation done client-side)

---

**Last Updated**: January 2026  
**Status**: 🟡 Active Development (Profile features expanding for RTC)  
**Feedback**: Report profile issues on [GitHub](https://github.com/logseq/logseq/issues) with `user-profile` label
