#!/bin/bash
# Logseq Changelog Generator
# Generates git commit logs for changelog creation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGSEQ_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Function to display usage
usage() {
    cat << EOF
Usage: $0 <start-date> <end-date> [options]

Generate git commit log for Logseq changelog creation.

Arguments:
    start-date      Start date in YYYY-MM-DD format
    end-date        End date in YYYY-MM-DD format

Options:
    -o, --output    Output file path (default: changelog_<dates>.md)
    -b, --branches  Branch pattern to include (default: all remote branches)
    -h, --help      Show this help message

Examples:
    $0 2025-01-05 2025-01-15
    $0 2025-01-01 2025-01-31 -o release-notes.md
    $0 2025-01-01 2025-01-31 -b "origin/master"

Output:
    Creates a markdown file with git commit log organized by branch.
    This file can then be processed by AI to generate a user-facing changelog.

See Also:
    prompts/changelog-generation.md - Full changelog generation guidelines
EOF
    exit 1
}

# Check if required parameters are provided
if [[ $# -lt 2 ]]; then
    usage
fi

# Parse arguments
start_date=$1
end_date=$2
shift 2

output_file=""
branch_pattern=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            output_file="$2"
            shift 2
            ;;
        -b|--branches)
            branch_pattern="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Error: Unknown option $1${NC}"
            usage
            ;;
    esac
done

# Validate date format
if ! [[ $start_date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo -e "${RED}Error: Invalid start date format. Use YYYY-MM-DD${NC}"
    exit 1
fi

if ! [[ $end_date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo -e "${RED}Error: Invalid end date format. Use YYYY-MM-DD${NC}"
    exit 1
fi

# Set default output file if not specified
if [[ -z "$output_file" ]]; then
    output_file="${LOGSEQ_DIR}/changelog_${start_date}_to_${end_date}.md"
fi

# Navigate to the Logseq directory
cd "$LOGSEQ_DIR" || {
    echo -e "${RED}Error: Cannot navigate to Logseq directory${NC}"
    exit 1
}

# Fetch latest updates
echo -e "${YELLOW}Fetching latest updates from remotes...${NC}"
git fetch --all --quiet

# Start changelog header
echo "# Git Commit Log (${start_date} to ${end_date})" > "$output_file"
echo "" >> "$output_file"
echo "Generated on: $(date '+%Y-%m-%d %H:%M:%S')" >> "$output_file"
echo "" >> "$output_file"
echo "---" >> "$output_file"

# Get branches to process
if [[ -n "$branch_pattern" ]]; then
    branches=$(git branch -r | grep -v '\->' | grep "$branch_pattern")
else
    branches=$(git branch -r | grep -v '\->')
fi

# Count total commits
total_commits=0

# Iterate over remote branches
echo -e "${YELLOW}Processing branches...${NC}"
for branch in $branches; do
    # Clean branch name
    branch=$(echo "$branch" | xargs)
    
    # Get log for the branch within the specified date range
    branch_log=$(git log "$branch" \
        --since="$start_date 00:00:00" \
        --until="$end_date 23:59:59" \
        --pretty=format:"- [%h] (%an) [%cd]: %s%n  > %b" \
        --date=short)
    
    # Only add branch if it has commits
    if [[ -n "$branch_log" ]]; then
        commit_count=$(echo "$branch_log" | grep -c '^\- \[' || true)
        total_commits=$((total_commits + commit_count))
        
        echo -e "${GREEN}✓ $branch${NC} (${commit_count} commits)"
        echo -e "\n## Branch: $branch" >> "$output_file"
        echo "" >> "$output_file"
        echo "$branch_log" >> "$output_file"
        echo "" >> "$output_file"
    fi
done

# Add summary footer
echo "---" >> "$output_file"
echo "" >> "$output_file"
echo "**Total commits:** $total_commits" >> "$output_file"
echo "**Branches processed:** $(echo "$branches" | wc -l | xargs)" >> "$output_file"

# Success message
echo ""
echo -e "${GREEN}✓ Changelog saved to: $output_file${NC}"
echo -e "${GREEN}✓ Total commits: $total_commits${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Review the raw commit log in: $output_file"
echo "2. Use GitHub Copilot with: @workspace /new-changelog"
echo "3. Or manually process using prompts/changelog-generation.md"
echo ""
