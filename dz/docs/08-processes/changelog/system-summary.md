# Changelog Generation System - Implementation Summary

> **Last revised:** 2026-05-30

## Overview

This changelog system combines repository scripts, Copilot skill instructions, and editorial standards to produce release-ready changelog artifacts.

It now supports both first-pass generation and rolling extension of an existing changelog window.

## Current Components

### 1. Copilot Skill Command: @workspace /new-changelog

Location:
- .github/copilot-instructions.md
- .github/skills/new-changelog.md

Primary behavior:
- Generates changelog-style or release-note-style output from commit history
- Supports date-window usage and format selection
- Uses the changelog prompt contract as guidance

### 2. Raw Commit Log Script: generate-changelog-log.sh

Location:
- dz/scripts/generate-changelog-log.sh

Current capabilities:
- Date format validation
- Optional output path
- Optional branch filter
- Commit counting and branch grouping
- Produces a markdown commit log window for AI/manual processing

### 3. Changelog Prompt Contract

Location:
- .github/prompts/changelog-generation.prompt.md

Current role:
- Defines changelog output structure and editorial expectations
- Provides examples and categorization guidance
- Documents the expected analysis workflow for generated commit logs

### 4. Process Documentation (Current Home)

Location:
- dz/docs/08-processes/changelog/readme.md
- dz/docs/08-processes/changelog/workflow.md
- dz/docs/08-processes/changelog/quick-reference.md
- dz/docs/08-processes/changelog/system-summary.md

## Findings: New Capabilities

### Rolling window extension tooling

The workflow now includes changelog window extension support through:
- dz/scripts/changelog-extend.sh
- dz/scripts/authors.txt
- dz/scripts/changelog-exclude.txt
- dz/changelog/STYLE.md

This extension flow adds capabilities that were not reflected in the prior summary:
- Extend an existing changelog date window to a new end date
- Optional no-merge and dry-run operation modes
- Character-budget checking during the extension workflow
- Auto-appended pre-categorized TRIAGE section for human review
- Contributor normalization and refresh using an author mapping file
- Noise filtering via maintained exclusion patterns

### Editorial taxonomy shift

The current style guide centers on:
- Spotlight
- Currently Available
- Upcoming
- Contributors

This is now the active editorial contract for categorized release artifacts.

### Source-backed functional area expansion

Compared with the older taxonomy snapshot, current source roots (src/, deps/, packages/) clearly surface active areas including:
- Publishing flows
- Properties-heavy data/model work
- PDF/media-related flows
- RTC/sync infrastructure
- Desktop window and shell integration paths

## Removed or Stale Claims (Corrected)

> **Removed:** docs/CHANGELOG_WORKFLOW.md as the canonical workflow doc location. Current workflow documentation lives in dz/docs/08-processes/changelog/.

> **Removed:** docs/CHANGELOG_QUICK_REFERENCE.md as the canonical quick reference location. Current quick reference lives in dz/docs/08-processes/changelog/quick-reference.md.

> **Removed:** prompts/changelog-generation.md as the canonical prompt path. Current prompt path is .github/prompts/changelog-generation.prompt.md.

> **Removed:** The statement that a deprecated New-LogseqChangeLog shell function currently points users to the new system. The function appears as an example in the prompt contract, but no active wrapper implementation is verified in source roots.

> **Removed:** PDF annotations as a standalone changelog functional-area bucket in this summary. Current categorization in process docs favors broader PDF/media and property-oriented flows.

## Current End-to-End Workflow

1. Generate the raw date-window commit log with dz/scripts/generate-changelog-log.sh.
2. Generate or refine release text via @workspace /new-changelog using the prompt contract.
3. If extending an existing release window, run dz/scripts/changelog-extend.sh and triage appended commits.
4. Finalize output against dz/changelog/STYLE.md and current process docs.

## Maintenance Notes

When updating this system, keep these files aligned together:
- .github/copilot-instructions.md
- .github/skills/new-changelog.md
- .github/prompts/changelog-generation.prompt.md
- dz/scripts/generate-changelog-log.sh
- dz/scripts/changelog-extend.sh
- dz/changelog/STYLE.md
- dz/docs/08-processes/changelog/*.md

Inconsistency risk to watch:
- Script help text and next-step messages in dz/scripts/generate-changelog-log.sh still reference prompts/changelog-generation.md in places, while canonical docs now point to .github/prompts/changelog-generation.prompt.md.

## Summary

The changelog system has moved from a single-pass generation model to a more complete release-operations pipeline: initial log generation, AI-assisted drafting, extension/triage tooling, and style-enforced publishing outputs. This summary now reflects the current process-document locations and active capability set as of 2026-05-30.
