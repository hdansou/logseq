#!/bin/bash

# Collect system information
OS_NAME=$(sw_vers -productName)
OS_VERSION=$(sw_vers -productVersion)
OS_BUILD=$(sw_vers -buildVersion)

# Logseq App Version
APP_VERSION=$(defaults read /Applications/Logseq-DB.app/Contents/Info.plist CFBundleShortVersionString 2>/dev/null || echo "Not found")
BUILD_NUMBER=$(defaults read /Applications/Logseq-DB.app/Contents/Info.plist CFBundleVersion 2>/dev/null || echo "Not found")

# Logseq CLI Version
CLI_VERSION=$(logseq --version 2>/dev/null || echo "Not installed")

# Git Commit Information (test/db branch)
LOGSEQ_DIR="/Users/dzu/Projects/src/github.com/logseq"
if [ -d "$LOGSEQ_DIR/.git" ]; then
    cd "$LOGSEQ_DIR" || exit
    GIT_COMMIT=$(git log -1 test/db --oneline 2>/dev/null || echo "Not found")
    GIT_HASH=$(git log -1 test/db --format=%H 2>/dev/null || echo "")
else
    GIT_COMMIT="Git repository not found"
    GIT_HASH=""
fi

# Parse format argument (default: markdown)
FORMAT="${1:-markdown}"

case "$FORMAT" in
    json)
        cat <<EOF
{
  "platform": {
    "os": {
      "name": "$OS_NAME",
      "version": "$OS_VERSION",
      "build": "$OS_BUILD"
    },
    "logseq": {
      "app_version": "$APP_VERSION",
      "build_number": "$BUILD_NUMBER",
      "cli_version": "$CLI_VERSION"
    },
    "git": {
      "branch": "test/db",
      "last_commit": "$GIT_COMMIT",
      "commit_url": "https://github.com/logseq/logseq/commit/$GIT_HASH"
    }
  }
}
EOF
        ;;
    yaml)
        cat <<EOF
platform:
  os:
    name: $OS_NAME
    version: $OS_VERSION
    build: $OS_BUILD
  logseq:
    app_version: $APP_VERSION
    build_number: $BUILD_NUMBER
    cli_version: $CLI_VERSION
  git:
    branch: test/db
    last_commit: $GIT_COMMIT
    commit_url: https://github.com/logseq/logseq/commit/$GIT_HASH
EOF
        ;;
    markdown|*)
        if [ -n "$GIT_HASH" ] && [ "$GIT_HASH" != "Not found" ]; then
            GIT_LINK="[\`$GIT_COMMIT\`](https://github.com/logseq/logseq/commit/$GIT_HASH)"
        else
            GIT_LINK="\`$GIT_COMMIT\`"
        fi
        cat <<EOF
## Platform Information

- **Operating System:** $OS_NAME $OS_VERSION (Build $OS_BUILD)
- **Logseq Version:** $APP_VERSION (Build $BUILD_NUMBER)
- **Logseq CLI:** $CLI_VERSION
- **Last Commit (test/db):** $GIT_LINK
EOF
        ;;
esac