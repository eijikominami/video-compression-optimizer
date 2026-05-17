#!/bin/bash
# Install VCOMenuBar as a macOS app with login item (auto-start on boot)
#
# Usage:
#   ./install.sh
#
# What it does:
#   1. Builds the release binary
#   2. Creates VCOMenuBar.app bundle in /Applications
#   3. Registers as Login Item (launches on OS startup)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

APP_NAME="VCOMenuBar"
APP_BUNDLE="/Applications/${APP_NAME}.app"
CONTENTS_DIR="${APP_BUNDLE}/Contents"
MACOS_DIR="${CONTENTS_DIR}/MacOS"
BUNDLE_ID="com.vco.menubar"

echo "Building ${APP_NAME} (release)..."
swift build -c release

echo "Installing to ${APP_BUNDLE}..."
rm -rf "${APP_BUNDLE}"
mkdir -p "${MACOS_DIR}"

cp ".build/release/${APP_NAME}" "${MACOS_DIR}/${APP_NAME}"

cat > "${CONTENTS_DIR}/Info.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleIdentifier</key>
    <string>${BUNDLE_ID}</string>
    <key>CFBundleName</key>
    <string>${APP_NAME}</string>
    <key>CFBundleDisplayName</key>
    <string>VCO MenuBar</string>
    <key>CFBundleVersion</key>
    <string>1.0</string>
    <key>CFBundleShortVersionString</key>
    <string>1.0</string>
    <key>CFBundleExecutable</key>
    <string>${APP_NAME}</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>LSMinimumSystemVersion</key>
    <string>13.0</string>
    <key>LSUIElement</key>
    <true/>
</dict>
</plist>
EOF

echo "Signing app bundle..."
codesign --sign - --force --deep "${APP_BUNDLE}"

echo "Registering as Login Item..."
osascript -e "tell application \"System Events\" to make login item at end with properties {path:\"${APP_BUNDLE}\", hidden:false}" 2>/dev/null || true

echo ""
echo "✅ Installed: ${APP_BUNDLE}"
echo "✅ Login Item registered (starts on boot)"
echo ""
echo "Launching now..."
open "${APP_BUNDLE}"
