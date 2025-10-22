#!/bin/bash

echo "## Platform Information"

# OS Information
echo "**Operating System:** $(sw_vers -productName) $(sw_vers -productVersion) (Build $(sw_vers -buildVersion))"

# Logseq App Version
APP_VERSION=$(defaults read /Applications/Logseq-DB.app/Contents/Info.plist CFBundleShortVersionString 2>/dev/null)
BUILD_NUMBER=$(defaults read /Applications/Logseq-DB.app/Contents/Info.plist CFBundleVersion 2>/dev/null)

if [ -n "$APP_VERSION" ] && [ -n "$BUILD_NUMBER" ]; then
    echo "**Logseq Version:** $APP_VERSION (Build $BUILD_NUMBER)"
else
    echo "**Logseq Version:** Not found"
fi

# Logseq CLI Version
CLI_VERSION=$(logseq --version 2>/dev/null)
if [ -n "$CLI_VERSION" ]; then
    echo "**Logseq CLI:** $CLI_VERSION"
else
    echo "**Logseq CLI:** Not installed"
fi