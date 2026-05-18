#!/usr/bin/env bash
# changelog-extend.sh — roll the current changelog window forward to a new end date.
#
# Workflow:
#   1. Fetches upstream/master.
#   2. (Optionally) merges upstream up through the last commit dated <= NEW_END_DATE.
#   3. Renames changelog_*_to_OLD.md  -> changelog_*_to_NEW.md
#      Renames whats-new-OLD.md       -> whats-new-NEW.md
#   4. Updates the header dates in both files.
#   5. Generates the delta commits (OLD..NEW) pre-categorized by conventional-commit
#      prefix and scope, appended to the changelog as a "## TRIAGE" block for review.
#   6. Refreshes the Contributors line (normalized via authors.txt).
#   7. Reports char counts against the forum budget.
#
# Usage:
#   dz/scripts/changelog-extend.sh NEW_END_DATE [--no-merge] [--max-chars N] [--dry-run]
#
# Reads:
#   dz/scripts/authors.txt           author dedupe
#   dz/scripts/changelog-exclude.txt commit-message skip patterns
#
# See: dz/changelog/STYLE.md for the editorial rules this script supports.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CHANGELOG_DIR="$ROOT_DIR/dz/changelog"
AUTHORS_MAP="$SCRIPT_DIR/authors.txt"
EXCLUDE_FILE="$SCRIPT_DIR/changelog-exclude.txt"

# Colors
RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'; BLUE=$'\033[0;34m'; NC=$'\033[0m'

usage() {
  cat <<EOF
Usage: $0 NEW_END_DATE [OPTIONS]

Roll the current changelog window forward to NEW_END_DATE (YYYY-MM-DD).

Options:
  --no-merge          Don't merge upstream/master. Use HEAD as-is.
  --max-chars N       Char budget for the categorized changelog (default 32000).
  --dry-run           Show planned actions; don't modify files or run git merge.
  -h, --help          Show this help.

Examples:
  $0 2026-05-20
  $0 2026-05-20 --no-merge
  $0 2026-05-20 --max-chars 32000 --dry-run

Notes:
  - The script auto-detects the most recent changelog_*_to_*.md in dz/changelog/.
  - On --dry-run it still fetches upstream (read-only) and prints what it would do.
EOF
  exit 1
}

# -------- arg parsing --------
[[ $# -lt 1 ]] && usage
NEW_END=""
NO_MERGE=0
DRY_RUN=0
MAX_CHARS=32000

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    --no-merge) NO_MERGE=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --max-chars) MAX_CHARS="$2"; shift 2 ;;
    *)
      if [[ -z "$NEW_END" ]]; then NEW_END="$1"; shift
      else echo "${RED}Unknown argument: $1${NC}" >&2; usage
      fi ;;
  esac
done

[[ "$NEW_END" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "${RED}NEW_END_DATE must be YYYY-MM-DD${NC}" >&2; exit 1; }

# -------- locate current changelog file --------
cd "$ROOT_DIR"
CURRENT_CL=$(ls -1t "$CHANGELOG_DIR"/changelog_*_to_*.md 2>/dev/null | head -n1 || true)
[[ -n "$CURRENT_CL" ]] || { echo "${RED}No changelog_*_to_*.md found in $CHANGELOG_DIR${NC}" >&2; exit 1; }

base=$(basename "$CURRENT_CL")
if [[ ! "$base" =~ ^changelog_([0-9]{4}-[0-9]{2}-[0-9]{2})_to_([0-9]{4}-[0-9]{2}-[0-9]{2})\.md$ ]]; then
  echo "${RED}Cannot parse window from $base${NC}" >&2; exit 1
fi
START_DATE="${BASH_REMATCH[1]}"
OLD_END="${BASH_REMATCH[2]}"

if [[ "$NEW_END" < "$OLD_END" || "$NEW_END" == "$OLD_END" ]]; then
  echo "${RED}NEW_END ($NEW_END) must be > current end date ($OLD_END)${NC}" >&2; exit 1
fi

# Refuse to extend if a TRIAGE block from a previous run is still in the file —
# means the human hasn't finished triaging it yet.
if grep -q '^## TRIAGE' "$CURRENT_CL"; then
  echo "${RED}Existing TRIAGE block found in $(basename "$CURRENT_CL").${NC}" >&2
  echo "${RED}Finish triaging (move bullets, delete the block) before extending.${NC}" >&2
  exit 1
fi

CURRENT_WN="$CHANGELOG_DIR/whats-new-$OLD_END.md"
NEW_CL="$CHANGELOG_DIR/changelog_${START_DATE}_to_${NEW_END}.md"
NEW_WN="$CHANGELOG_DIR/whats-new-${NEW_END}.md"

echo "${BLUE}Window:${NC} $START_DATE → $OLD_END  ⇒  $START_DATE → $NEW_END"
echo "${BLUE}Files:${NC}"
echo "  $CURRENT_CL → $NEW_CL"
[[ -f "$CURRENT_WN" ]] && echo "  $CURRENT_WN → $NEW_WN" || echo "  (no whats-new-$OLD_END.md found; will skip)"

# -------- fetch upstream --------
echo "${YELLOW}Fetching upstream/master...${NC}"
git fetch upstream master --quiet

# -------- pick cutoff commit (last commit whose short committer-date is <= NEW_END) --------
# Walk upstream/master most-recent-first; pick first commit whose %cd --date=short ≤ NEW_END.
# Avoids git --until quirks across committer timezones.
# Subshell disables pipefail so awk's `exit` (causing git's SIGPIPE) doesn't kill us.
CUTOFF=$(set +o pipefail; git log upstream/master --pretty=format:"%H %cd" --date=short \
         | awk -v end="$NEW_END" '$2 <= end { print $1; exit }')
[[ -n "$CUTOFF" ]] || { echo "${RED}No upstream commits at or before $NEW_END${NC}" >&2; exit 1; }
echo "${BLUE}Upstream cutoff:${NC} $(git log -1 --pretty=format:'%h | %cd | %s' --date=short "$CUTOFF")"

# -------- merge --------
if [[ $NO_MERGE -eq 1 ]]; then
  echo "${YELLOW}--no-merge: skipping git merge.${NC}"
elif [[ $DRY_RUN -eq 1 ]]; then
  echo "${YELLOW}--dry-run: would 'git merge $CUTOFF --no-edit'${NC}"
else
  # Skip if HEAD already contains the cutoff commit.
  if git merge-base --is-ancestor "$CUTOFF" HEAD; then
    echo "${GREEN}HEAD already contains cutoff $CUTOFF; no merge needed.${NC}"
  else
    echo "${YELLOW}Merging upstream up to $CUTOFF...${NC}"
    git merge "$CUTOFF" --no-edit
  fi
fi

# -------- rename + update headers --------
rename_and_rehead() {
  local src="$1" dst="$2" header_pattern="$3" new_header="$4"
  if [[ ! -f "$src" ]]; then return 0; fi
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "${YELLOW}--dry-run: would rename $src → $dst and update header to: $new_header${NC}"
    return 0
  fi
  git mv "$src" "$dst" 2>/dev/null || mv "$src" "$dst"
  # Replace only the first occurrence of the matching header line.
  awk -v pat="$header_pattern" -v repl="$new_header" '
    BEGIN { done=0 }
    !done && $0 ~ pat { print repl; done=1; next }
    { print }
  ' "$dst" > "$dst.tmp" && mv "$dst.tmp" "$dst"
}

rename_and_rehead "$CURRENT_CL" "$NEW_CL" \
  "^# Changelog \(${START_DATE} to ${OLD_END}\)$" \
  "# Changelog (${START_DATE} to ${NEW_END})"

rename_and_rehead "$CURRENT_WN" "$NEW_WN" \
  "^# What's New with Logseq — ${OLD_END}$" \
  "# What's New with Logseq — ${NEW_END}"

# Also update the intro window prose in whats-new (best-effort; safe to leave if not matched).
if [[ -f "$NEW_WN" && $DRY_RUN -eq 0 ]]; then
  sed -i.bak "s|and ${OLD_END}|and ${NEW_END}|g" "$NEW_WN" && rm -f "${NEW_WN}.bak"
fi

# -------- generate delta commits with categorization --------
DELTA_TMP=$(mktemp)
trap 'rm -f "$DELTA_TMP" "$DELTA_TMP.tmp"' EXIT

# Build a single ERE union for exclude patterns.
EXCLUDE_ERE=""
if [[ -f "$EXCLUDE_FILE" ]]; then
  EXCLUDE_ERE=$(grep -v '^\s*#' "$EXCLUDE_FILE" | grep -v '^\s*$' | paste -sd'|' -)
fi

# Pull non-merge commits in (OLD_END, NEW_END] window from upstream/master, filtered by
# short committer-date to avoid the --until timezone quirks.
git log upstream/master --no-merges --pretty=format:"%h|%cd|%s" --date=short \
  | awk -F'|' -v lo="$OLD_END" -v hi="$NEW_END" '$2 > lo && $2 <= hi { print $1"|"$3 }' \
  > "$DELTA_TMP.raw"

TOTAL_RAW=$(grep -c '.' "$DELTA_TMP.raw" 2>/dev/null || echo 0)
if [[ -n "$EXCLUDE_ERE" ]]; then
  awk -F'|' -v re="$EXCLUDE_ERE" '$2 !~ re' "$DELTA_TMP.raw" > "$DELTA_TMP"
else
  cp "$DELTA_TMP.raw" "$DELTA_TMP"
fi
TOTAL_KEPT=$(grep -c '.' "$DELTA_TMP" 2>/dev/null || echo 0)
echo "${BLUE}Delta commits:${NC} $TOTAL_KEPT kept (of $TOTAL_RAW raw; $((TOTAL_RAW-TOTAL_KEPT)) excluded by patterns)"

# Categorize: prefix → section, scope → subsection.
# Outputs a tab-separated stream: SECTION \t SUBSECTION \t HASH \t SUBJECT
# Implemented in pure bash for portability (macOS awk lacks 3-arg match()).
categorize() {
  local hash subj prefix scope rest section subsec
  while IFS='|' read -r hash subj; do
    [[ -z "$hash" ]] && continue
    prefix=""; scope=""; rest="$subj"
    if [[ "$subj" =~ ^([a-zA-Z]+)(\(([^\)]+)\))?:[[:space:]]*(.*) ]]; then
      prefix="${BASH_REMATCH[1],,}"
      scope="${BASH_REMATCH[3],,}"
      rest="${BASH_REMATCH[4]}"
    fi

    # prefix → section
    case "$prefix" in
      feat) section="New Feature" ;;
      enhance) section="Enhancement" ;;
      fix) section="Bug Fix" ;;
      perf) section="Performance" ;;
      refactor) section="Refactoring" ;;
      chore|dev|ci|test|docs|style) section="Chore" ;;
      *) section="Bug Fix" ;;
    esac

    # scope → subsection
    case "$scope" in
      cli|dev|ci|e2e) subsec="Developer tools" ;;
      ui|ux|editor|header|fsrs|pdf|breadcrumb|i18n) subsec="UI/UX" ;;
      graph-view) subsec="Graph visualization" ;;
      sync|asset|assets) subsec="Sync and storage" ;;
      plugin|plugins) subsec="Plugin system" ;;
      publish) subsec="Publishing" ;;
      mobile|ios|android) subsec="Mobile" ;;
      search|query) subsec="Search and queries" ;;
      db|database) subsec="Database operations" ;;
      exporter|export|import) subsec="Import and export" ;;
      property|properties) subsec="Properties" ;;
      *) subsec="Uncategorized" ;;
    esac

    # Fallback subsection per section.
    if [[ "$subsec" == "Uncategorized" ]]; then
      case "$section" in
        Chore) subsec="Developer tools" ;;
        Performance) subsec="Search and queries" ;;
        Refactoring) subsec="Sync and storage" ;;
        *) subsec="UI/UX" ;;  # least-bad bucket; human will reassign
      esac
    fi

    printf '%s\t%s\t%s\t%s\n' "$section" "$subsec" "$hash" "$rest"
  done < "$DELTA_TMP"
}

# Render the triage block as markdown.
render_triage() {
  local order_sections="New Feature|Enhancement|Bug Fix|Performance|Refactoring|Chore"
  echo
  echo "---"
  echo
  echo "## TRIAGE — commits added in this extension ($OLD_END → $NEW_END)"
  echo
  echo "_Pre-categorized by conventional-commit prefix/scope. Move bullets into the appropriate **Currently Available** section above (consolidating with semicolons where sensible per STYLE.md), then delete this block. Char budget: $MAX_CHARS._"
  echo

  local section_iter
  IFS='|' read -ra _sections <<< "$order_sections"
  for section_iter in "${_sections[@]}"; do
    local section_bullets
    section_bullets=$(categorize | awk -F'\t' -v s="$section_iter" '$1==s')
    [[ -z "$section_bullets" ]] && continue
    echo "### $section_iter"
    echo
    # Group by subsection
    echo "$section_bullets" | awk -F'\t' '{print $2}' | sort -u | while read -r subsec; do
      echo "#### $subsec"
      echo
      echo "$section_bullets" | awk -F'\t' -v ss="$subsec" '$2==ss {
        printf "- %s ([%s](https://github.com/logseq/logseq/commit/%s)).\n", $4, $3, $3
      }'
      echo
    done
  done
}

if [[ $DRY_RUN -eq 1 ]]; then
  echo "${YELLOW}--dry-run: would append TRIAGE block (${TOTAL_KEPT} bullets) to $NEW_CL${NC}"
else
  render_triage >> "$NEW_CL"
  echo "${GREEN}Appended TRIAGE block with $TOTAL_KEPT bullets to $NEW_CL${NC}"
fi

# -------- refresh contributors --------
refresh_contributors() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  [[ $DRY_RUN -eq 1 ]] && { echo "${YELLOW}--dry-run: would refresh Contributors line${NC}"; return 0; }

  # Collect authors over the whole window from upstream/master.
  local authors_raw
  authors_raw=$(git log upstream/master \
    --since="$START_DATE 00:00:00" --until="$NEW_END 23:59:59" \
    --no-merges --pretty=format:"%an")

  # Apply authors.txt normalization; drop bot accounts; sort case-insensitive unique.
  local authors_norm
  authors_norm=$(echo "$authors_raw" \
    | awk -v map="$AUTHORS_MAP" '
        BEGIN {
          while ((getline line < map) > 0) {
            sub(/[[:space:]]*#.*$/, "", line)
            if (line ~ /^[[:space:]]*$/) continue
            split(line, kv, "=")
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", kv[1])
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", kv[2])
            m[kv[1]] = kv[2]
          }
        }
        { if ($0 in m) print m[$0]; else print $0 }
      ' \
    | grep -viE '\[bot\]|copilot-swe-agent' \
    | sort -fu \
    | tr '\n' '|' | sed 's/|$//; s/|/, /g')

  local n_master n_branch n_pr
  n_master=$(git log upstream/master --since="$START_DATE 00:00:00" --until="$NEW_END 23:59:59" --no-merges --oneline | wc -l | tr -d ' ')
  n_branch=$(grep -c '_(branch:' "$file" 2>/dev/null || echo 0)
  n_pr=$(git log upstream/master --since="$START_DATE 00:00:00" --until="$NEW_END 23:59:59" --merges --grep='Merge pull request' --oneline | wc -l | tr -d ' ')

  # Replace the two bullets at the end of the Contributors section.
  awk -v auth="- $authors_norm" -v stats="- Total analyzed: $n_master non-merge commits on master, $n_branch on feature branches, $n_pr merged pull requests" '
    BEGIN { in_contrib=0; replaced_auth=0; replaced_stats=0 }
    /^## Contributors$/ { in_contrib=1; print; next }
    in_contrib && /^- [A-Za-z]/ && !replaced_auth { print auth; replaced_auth=1; next }
    in_contrib && /^- Total analyzed:/ && !replaced_stats { print stats; replaced_stats=1; next }
    { print }
  ' "$file" > "$file.tmp" && mv "$file.tmp" "$file"
  echo "${GREEN}Refreshed Contributors line in $(basename "$file")${NC}"
}
refresh_contributors "$NEW_CL"

# -------- char budget check --------
echo
echo "${BLUE}== Char budget check ==${NC}"
budget_check() {
  local file="$1" budget="$2"
  [[ -f "$file" ]] || { echo "  (skip) $(basename "$file") not present"; return 0; }
  local chars
  chars=$(wc -c < "$file" | tr -d ' ')
  if [[ $chars -le $budget ]]; then
    printf "  ${GREEN}OK${NC}    %6d / %6d chars  %s\n" "$chars" "$budget" "$(basename "$file")"
  else
    local over=$((chars - budget))
    printf "  ${RED}OVER${NC}  %6d / %6d chars  (+%d over)  %s\n" "$chars" "$budget" "$over" "$(basename "$file")"
  fi
}
budget_check "$NEW_CL" "$MAX_CHARS"
budget_check "$NEW_WN" 24000   # advisory only; What's New has no forum hard cap
[[ -f "$CHANGELOG_DIR/discord-${OLD_END}.md" ]] && \
  budget_check "$CHANGELOG_DIR/discord-${OLD_END}.md" 2000

echo
echo "${GREEN}Done.${NC} Next steps:"
echo "  1. Open $NEW_CL, move TRIAGE bullets into the right sections (delete TRIAGE block when done)."
echo "  2. Edit $NEW_WN to add narrative for any new headline items."
echo "  3. Re-run the budget check: wc -c $NEW_CL"
echo "  4. See dz/changelog/STYLE.md for editorial rules."
