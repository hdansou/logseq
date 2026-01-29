---
title: 'Flashcards Basics'
description: "Learn to create and study flashcards with Logseq's spaced repetition system"
type: tutorial
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [file, db]
difficulty: beginner
timeToComplete: '15 minutes'
prerequisites: ['basic-editing']
relatedDocs:
  reference:
    - '[Flashcard System](../../reference/features/flashcards.md)'
  howTo:
    - '[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)'
---

# Flashcards Basics

> Learn to create flashcards and use spaced repetition for effective learning.

## What You'll Learn

- 🎴 Creating flashcards
- 🔄 The spaced repetition system (FSRS)
- 📚 Studying your cards
- 📊 Tracking progress
- ✨ Best practices

**Time:** ~15 minutes

---

## What is Spaced Repetition?

Spaced repetition is a learning technique that shows you information right before you forget it. Logseq uses **FSRS** (Free Spaced Repetition Scheduler), a modern algorithm that:

- Learns how you remember
- Schedules reviews optimally
- Adapts to your performance

---

## Step 1: Create Your First Flashcard

### Basic Card (Front/Back)

Type on any block, then use the slash command:

```
What is the capital of France?
#card
Paris
```

Or use the keyboard shortcut: `Cmd/Ctrl+Shift+C`

The text before `#card` becomes the **front** (question).
Child blocks become the **back** (answer).

### Try It Now

1. Create a new block
2. Type: `What is 2 + 2?`
3. Add `#card` tag
4. Press Enter and indent
5. Type: `4`

---

## Step 2: Card Types

### Standard Card

```
Question here
#card
Answer here
```

### Cloze Deletion

Hide part of the text:

```
The {{cloze capital}} of France is Paris.
#card
```

When studying, you'll see: "The **\_** of France is Paris."

### Multiple Clozes

```
{{c1 Photosynthesis}} converts {{c2 light energy}} into {{c3 chemical energy}}.
#card
```

Each cloze becomes a separate card.

### Reversed Card

Creates both directions automatically:

```
Bonjour
#card #reversed
Hello
```

Generates:

- Card 1: Bonjour → Hello
- Card 2: Hello → Bonjour

---

## Step 3: Study Your Cards

### Access Flashcards

**Method 1: Sidebar**

- Click the flashcard icon in the left sidebar

**Method 2: Keyboard**

- Press `t` then `f` (toggle flashcards)

**Method 3: Slash Command**

- Type `/Flashcards` on any page

### Study Session

1. A card appears showing the front (question)
2. Think of the answer
3. Click "Show Answer"
4. Rate how well you remembered:
   - **Again** - Forgot completely
   - **Hard** - Struggled but got it
   - **Good** - Remembered correctly
   - **Easy** - Knew it instantly

### Review Schedule

Based on your rating, the card appears again:

- **Again**: Within minutes
- **Hard**: Next day
- **Good**: Several days
- **Easy**: Many days/weeks

---

## Step 4: Organize Into Decks

### Page-Based Decks

All cards on a page form a deck:

```
# Spanish Vocabulary
## Greetings
Hola #card
Hello

Buenos días #card
Good morning

## Food
Manzana #card
Apple
```

### Tag-Based Decks

Use specific tags to group cards:

```
What is H2O? #card #chemistry
Water

When was WWII? #card #history
1939-1945
```

### Study by Tag

Filter cards when studying:

1. Open flashcards
2. Click "Filter"
3. Select tag or page

---

## Step 5: Track Progress

### Card Status

Each card shows:

- **New**: Never studied
- **Learning**: Recently introduced
- **Review**: In regular rotation
- **Due**: Needs review today

### Statistics

View your learning stats:

- Cards due today
- Review forecast
- Retention rate
- Streak

---

## Step 6: Write Better Cards

### Good Card Principles

✅ **One idea per card**

```
What is the atomic number of Carbon?
#card
6
```

❌ **Too much info**

```
Describe Carbon including atomic number, mass, and properties
#card
(Long answer that's hard to review)
```

### Use Context

✅ **Specific question**

```
In JavaScript, what keyword declares a constant?
#card
const
```

❌ **Vague question**

```
What is const?
#card
(Could mean many things)
```

### Add Examples

```
What is a metaphor?
#card
A figure of speech comparing two unlike things.
Example: "Time is money"
```

---

## Practice Exercise

Create a mini study deck:

1. Create page "My First Deck"
2. Add these cards:

```
What is the largest planet in our solar system?
#card
Jupiter

The Great Wall of China is in {{cloze Asia}}.
#card

Agua #card #reversed
Water

What does CPU stand for?
#card
Central Processing Unit
```

3. Study your cards
4. Review again tomorrow

---

## Tips

### Daily Review Habit

Review due cards every day, even just 5 minutes.

### Make Cards From Notes

Turn important concepts into cards as you learn:

```
Notes about photosynthesis...
- Key point: uses sunlight
- Produces: oxygen and glucose

Photosynthesis uses {{cloze sunlight}} to produce oxygen and glucose.
#card
```

### Use Images

```
What bird is this?
#card
![robin](image.jpg)
Robin
```

### Review Hard Cards More

If a card keeps failing, rewrite it simpler or break into smaller cards.

---

## Settings

Customize in Settings → Flashcards:

| Setting       | Description                     |
| ------------- | ------------------------------- |
| New cards/day | How many new cards to introduce |
| Reviews/day   | Maximum daily reviews           |
| Card order    | Random or by due date           |

---

## What's Next?

- **[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)** - Advanced deck organization
- **[Flashcard System Reference](../../reference/features/flashcards.md)** - All settings and options
- **[Semantic Search](../getting-started/semantic-search.md)** - Find related content

---

## Summary

✅ Created basic flashcards with `#card`  
✅ Used cloze deletions  
✅ Studied and rated cards  
✅ Understood the FSRS algorithm  
✅ Organized cards into decks

---

_Last updated: 2026-01-29_
