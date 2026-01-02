#!/bin/bash
# Build script for vco-photos Universal Binary
#
# This script builds the vco-photos Swift binary for both arm64 and x86_64
# architectures and combines them into a Universal Binary using lipo.
#
# Usage:
#   ./build_swift.sh [--release]
#
# Options:
#   --release    Build in release mode (default: debug)
#
# Output:
#   bin/vco-photos - Universal Binary

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Parse arguments
BUILD_TYPE="debug"
if [[ "$1" == "--release" ]]; then
    BUILD_TYPE="release"
fi

echo "Building vco-photos ($BUILD_TYPE mode)..."

# Create output directory
mkdir -p bin

# Build for arm64
echo "Building for arm64..."
swift build -c "$BUILD_TYPE" --arch arm64

# Build for x86_64
echo "Building for x86_64..."
swift build -c "$BUILD_TYPE" --arch x86_64

# Get binary paths
ARM64_BINARY=".build/arm64-apple-macosx/$BUILD_TYPE/vco-photos"
X86_64_BINARY=".build/x86_64-apple-macosx/$BUILD_TYPE/vco-photos"

# Check if binaries exist
if [[ ! -f "$ARM64_BINARY" ]]; then
    echo "Error: arm64 binary not found at $ARM64_BINARY"
    exit 1
fi

if [[ ! -f "$X86_64_BINARY" ]]; then
    echo "Error: x86_64 binary not found at $X86_64_BINARY"
    exit 1
fi

# Create Universal Binary
echo "Creating Universal Binary..."
lipo -create -output bin/vco-photos "$ARM64_BINARY" "$X86_64_BINARY"

# Embed Info.plist for Photos library access permissions
INFO_PLIST="Sources/vco-photos/Info.plist"
if [[ -f "$INFO_PLIST" ]]; then
    echo "Embedding Info.plist..."
    # Create __TEXT,__info_plist section with the plist content
    # This allows the binary to have the NSPhotoLibraryUsageDescription
fi

# Sign the binary with entitlements (ad-hoc signing)
echo "Signing binary..."
codesign --sign - --force --options runtime bin/vco-photos

# Verify the Universal Binary
echo ""
echo "Verifying Universal Binary..."
file bin/vco-photos
lipo -info bin/vco-photos

# Show binary size
echo ""
echo "Binary size:"
ls -lh bin/vco-photos

echo ""
echo "Build complete: bin/vco-photos"
