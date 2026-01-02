#!/bin/bash
# Create a minimal .app bundle for vco-photos to enable Photos library access
#
# PhotoKit requires the executable to be part of an app bundle with proper
# Info.plist to trigger the authorization dialog.
#
# Usage:
#   ./create_app_bundle.sh
#
# Output:
#   vco-photos.app/ - Application bundle

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

APP_NAME="vco-photos"
APP_BUNDLE="${APP_NAME}.app"
CONTENTS_DIR="${APP_BUNDLE}/Contents"
MACOS_DIR="${CONTENTS_DIR}/MacOS"

echo "Creating app bundle for ${APP_NAME}..."

# Build the binary first
echo "Building binary..."
swift build -c debug

# Remove existing app bundle
rm -rf "${APP_BUNDLE}"

# Create directory structure
mkdir -p "${MACOS_DIR}"

# Copy binary
cp ".build/debug/${APP_NAME}" "${MACOS_DIR}/${APP_NAME}"

# Create Info.plist
cat > "${CONTENTS_DIR}/Info.plist" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleIdentifier</key>
    <string>com.vco.photos</string>
    <key>CFBundleName</key>
    <string>vco-photos</string>
    <key>CFBundleDisplayName</key>
    <string>VCO Photos</string>
    <key>CFBundleVersion</key>
    <string>1.0</string>
    <key>CFBundleShortVersionString</key>
    <string>1.0</string>
    <key>CFBundleExecutable</key>
    <string>vco-photos</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>LSMinimumSystemVersion</key>
    <string>13.0</string>
    <key>LSUIElement</key>
    <true/>
    <key>NSPhotoLibraryUsageDescription</key>
    <string>vco-photos needs access to your Photos library to scan, import, and manage videos.</string>
    <key>NSPhotoLibraryAddUsageDescription</key>
    <string>vco-photos needs permission to add videos to your Photos library.</string>
</dict>
</plist>
EOF

# Sign the app bundle
echo "Signing app bundle..."
codesign --sign - --force --deep "${APP_BUNDLE}"

echo ""
echo "App bundle created: ${APP_BUNDLE}"
echo ""
echo "To test Photos access:"
echo "  echo '{\"command\":\"scan\",\"args\":{}}' | ${APP_BUNDLE}/Contents/MacOS/${APP_NAME}"
echo ""
echo "First run will trigger the Photos authorization dialog."
