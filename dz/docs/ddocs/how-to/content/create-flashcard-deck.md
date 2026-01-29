---
title: 'Create a Flashcard Deck'
description: 'How to organize flashcards into focused study decks'
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [file, db]
relatedDocs:
  tutorials:
    - '[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)'
  reference:
    - '[Flashcard System](../../reference/features/flashcards.md)'
---

# Create a Flashcard Deck

> How to organize flashcards into focused study decks for efficient learning.

## Page-Based Decks

### Create a Deck Page

All cards on a page automatically form a deck:

```markdown
# Spanish Vocabulary

## Greetings

Hola #card
Hello

Buenos días #card  
Good morning

## Numbers

Uno #card
One

Dos #card
Two
```

### Study the Deck

1. Navigate to the page
2. Use `{{cards [[Spanish Vocabulary]]}}`
3. Or click the flashcard icon in sidebar

---

## Tag-Based Decks

### Use Tags for Categorization

```markdown
What is H2O? #card #chemistry
Water

What is NaCl? #card #chemistry
Sodium Chloride (Salt)

When was World War II? #card #history
1939-1945
```

### Study by Tag

Create a query for the tag:

```
{{cards [[chemistry]]}}
```

---

## Property-Based Decks

### Add Properties to Cards

```markdown
What is photosynthesis? #card
topic:: biology
difficulty:: medium
source:: [[Biology Textbook]]
The process by which plants convert light to energy
```

### Query by Property

```clojure
#+BEGIN_QUERY
{:title "Medium Difficulty Cards"
 :query [:find (pull ?b [*])
         :where [?b :block/refs ?ref]
                [?ref :block/name "card"]
                [?b :block/properties ?props]
                [(get ?props :difficulty) ?d]
                [(= ?d "medium")]]}
#+END_QUERY
```

---

## Multi-Level Organization

### Namespace Structure

```
Languages/Spanish/Vocabulary  ← Deck page
Languages/Spanish/Grammar     ← Another deck
Languages/French/Vocabulary   ← French deck
```

### Parent-Level Queries

Study all Spanish cards:

```
{{cards [[Languages/Spanish]]}}
```

---

## Dynamic Decks

### Due Today

```clojure
#+BEGIN_QUERY
{:title "Due Today"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where [?b :block/refs ?ref]
                [?ref :block/name "card"]
                [?b :block/properties ?props]
                [(get ?props :card-next-schedule) ?date]
                [(<= ?date ?today)]]
 :inputs [:today]}
#+END_QUERY
```

### Recent Cards

Cards created in the last week:

```clojure
{{query (and (property :card) (between [[created-at]] [[7d-ago]] [[now]]))}}
```

---

## Deck Templates

### Create a Deck Template

```markdown
# Deck: {Name}

Tags: #deck #{topic}

## To Study

(Add cards here)

## Mastered

(Move completed cards here)

## Card Query

{{cards [[Deck: {Name}]]}}

## Stats

Total cards:
Due today:
```

### Use the Template

1. Create new page
2. Apply template
3. Start adding cards

---

## Import Cards

### From Markdown

Create cards from existing notes:

1. Find key concepts
2. Add `#card` tag
3. Ensure answer is indented child

### Bulk Creation

Use search and replace:

- Find: `Key concept:`
- Replace with: `Key concept: #card`

---

## Export Decks

### Export to Markdown

1. Navigate to deck page
2. Use `Cmd/Ctrl+Shift+E`
3. Select Markdown format
4. Save file

### Share Decks

1. Export deck page
2. Include all linked pages
3. Share as `.md` files
4. Others can import to their graph

---

## Deck Maintenance

### Review Deck Health

Check for:

- Orphan cards (no deck)
- Duplicate cards
- Outdated content
- Too-easy cards to archive

### Archive Mastered Cards

Move very easy cards to archive:

```markdown
## Active Cards

(Current study cards)

## Archived

(Well-known cards, rarely review)
```

### Split Large Decks

If a deck exceeds 200 cards:

1. Create sub-decks by topic
2. Use namespace pages
3. Rotate focus between sub-decks

---

## Deck Strategies

### The 20-Card Rule

For new decks:

- Start with 20 cards
- Master before adding more
- Prevents overwhelm

### Topic Rotation

For multiple decks:

- Day 1: Spanish
- Day 2: History
- Day 3: Science
- Repeat

### Focused Sessions

- 15 minutes maximum
- One deck at a time
- Stop when attention wanders

---

## Troubleshooting

### No Cards in Deck

1. Check `#card` tags present
2. Verify page/tag references correct
3. Check query syntax

### Wrong Cards Appearing

1. Review query filters
2. Check for stray tags
3. Verify property values

### Cards Not Scheduled

1. Complete initial review
2. Check algorithm settings
3. Verify due dates

---

## See Also

- **[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)**
- **[Flashcard System](../../reference/features/flashcards.md)**

---

_Last updated: 2026-01-29_
