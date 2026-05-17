English / [**日本語**](README_JP.md)

# Video Compression Optimizer (VCO)

[![Build](https://github.com/eijikominami/video-compression-optimizer/actions/workflows/test.yml/badge.svg)](https://github.com/eijikominami/video-compression-optimizer/actions/workflows/test.yml)
[![Release](https://github.com/eijikominami/video-compression-optimizer/actions/workflows/release.yml/badge.svg)](https://github.com/eijikominami/video-compression-optimizer/actions/workflows/release.yml)
[![Release Version](https://img.shields.io/github/v/release/eijikominami/video-compression-optimizer)](https://github.com/eijikominami/video-compression-optimizer/releases)
[![codecov](https://codecov.io/gh/eijikominami/video-compression-optimizer/branch/main/graph/badge.svg)](https://codecov.io/gh/eijikominami/video-compression-optimizer)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)

A tool to convert videos in Apple Photos to H.265 format to save storage space.

## Features

- Automatic scanning of Apple Photos library videos
- Native Swift PhotoKit implementation for fast, reliable Photos access
- High-quality H.265 conversion using AWS MediaConvert
- SSIM and VMAF-based quality verification using MediaConvert per-frame metrics
- Metadata preservation (capture date, location, albums)
- iCloud video status detection
- Efficient conversion with Top-N selection

## Prerequisites

- macOS 10.15 (Catalina) or later
- Python 3.10 or later
- AWS account (MediaConvert, S3, Lambda)
- exiftool (for metadata embedding)

```bash
# Install exiftool
brew install exiftool
```

## Installation

### 1. Download Swift Binary

Download the pre-built Universal Binary from [GitHub Releases](https://github.com/eijikominami/video-compression-optimizer/releases):

```bash
# Create bin directory
mkdir -p bin

# Download latest release
curl -sL $(curl -s https://api.github.com/repos/eijikominami/video-compression-optimizer/releases/latest | grep browser_download_url | cut -d '"' -f 4) -o bin/vco-photos
chmod +x bin/vco-photos
```

Or build from source (see [Development](#development) section).

### 2. Install Python Package

```bash
pip install .
```

Development environment:

```bash
pip install -e ".[dev]"
```

## AWS Infrastructure Deployment

Deploy the SAM template:

```bash
cd sam-app
sam build
sam deploy --stack-name vco-infrastructure \
  --capabilities CAPABILITY_NAMED_IAM \
  --resolve-s3 \
  --profile <your-aws-profile> \
  --region ap-northeast-1
```

## Usage

### Scan

```bash
# Scan Apple Photos library
vco scan

# Specify date range
vco scan --from 2020-01 --to 2020-12

# Show top N by file size
vco scan --top-n 10

# Output in JSON format
vco scan --json
```

**Scan Summary Categories**:

| Category | Description |
|----------|-------------|
| Total videos | All videos in Photos library |
| Conversion candidates | Videos using inefficient codecs (H.264, MPEG-2, etc.) |
| Already optimized | Videos already using efficient codecs (H.265, AV1, VP9) |
| Professional format | ProRes, DNxHD, CineForm, RAW - skipped (manual review recommended) |
| Skipped | Videos excluded from conversion (see below) |

**Skipped Reasons**:
- Duration too short (< 1 second)
- Image-based codec (JPEG, PNG, GIF - not true video)
- Unsupported codec by MediaConvert
- File not accessible

### Convert

```bash
# Execute conversion (default: balanced)
vco convert

# Specify quality preset
vco convert --quality high

# Convert only top N by file size
vco convert --top-n 5

# Dry run (no actual conversion)
vco convert --dry-run

# Skip iCloud videos (process local only)
vco convert --skip-icloud

# Set download timeout for iCloud videos (default: 300 seconds, range: 30-3600)
vco convert --download-timeout 600

# Set parallel transfer concurrency (default: 3, range: 1-10)
vco convert --parallel 5

# Skip confirmation prompts
vco convert --yes
```

**Automatic iCloud Download**: When running `vco convert`, iCloud-only videos are automatically downloaded using Swift PhotoKit. Use `--skip-icloud` to skip them.

**Parallel Transfers**: Downloads and uploads run in parallel (default: 3 concurrent). Use `--parallel N` to adjust. Press Ctrl+C to cancel gracefully.

Conversions are processed asynchronously via AWS Step Functions. After submitting a conversion, you can check status and manage tasks:

```bash
# Check task status
vco status                    # List recent tasks (default: 10)
vco status -n 20              # List more tasks
vco status <task-id>          # Show task details

# Cancel a running task
vco cancel <task-id>

# Import completed files
vco import --list             # List all importable items
vco import --all              # Import all items
vco import <task-id:file-id>  # Import specific file
vco import --delete-original <task-id:file-id>  # Import and delete original
vco import --force <task-id:file-id>  # Import even if metadata verification fails
```

### Import

Import converted videos from AWS completed tasks:

```bash
# Show import queue
vco import --list

# Import specified video to Photos
vco import <item-id>          # Format: task-id:file-id

# Import and automatically delete original video
vco import --delete-original <item-id>

# Batch import all videos
vco import --all

# Skip confirmation prompts
vco import -y <item-id>
vco import -y --all

# Remove specified ID from queue (also deletes S3 files)
vco import --remove <item-id>

# Force import even if metadata verification fails
vco import --force <item-id>

# Clear all items from queue (also deletes S3 files)
vco import --clear
```

**Item ID formats**:
- AWS items: `task-uuid:file-uuid` (task:file format)

**Options**:
- `--delete-original`: Automatically delete original video from Photos after successful import (moves to trash)
- `--force`: Import even if metadata verification fails (capture date, GPS location mismatch)
- `-y, --yes`: Skip confirmation prompts

**Note**: 
- The `--remove` and `--clear` options delete both the queue entry and the corresponding S3 files.
- Without `--delete-original`, you need to manually delete original videos in Photos app after import.
- Metadata verification checks capture date (±1 second tolerance) and GPS location (±0.0001 degrees tolerance) before import. Use `--force` to bypass verification failures.
- If capture date is within ±1 hour of processing time, a warning is displayed but import continues.

### Configuration

```bash
# Show current configuration
vco config

# AWS settings
vco config set aws.s3_bucket <bucket>
vco config set aws.role_arn <arn>
vco config set aws.region ap-northeast-1

# Conversion settings
vco config set conversion.quality_preset balanced
vco config set conversion.max_concurrent 3
```

## Quality Presets

| Preset | QVBR | Use Case |
|--------|------|----------|
| `high` | 8-9 | When maintaining high quality |
| `balanced` | 6-7 | Balance of quality and size (recommended) |
| `balanced+` | 6-7 → 8-9 | Retry with high if balanced fails quality check (best-effort) |
| `compression` | 4-5 | Maximum compression |

### Quality Metrics

VCO uses MediaConvert's per-frame metrics to evaluate video quality:

| Metric | Range | Threshold | Description |
|--------|-------|-----------|-------------|
| SSIM | 0-1 | >= 0.95 | Structural Similarity Index |
| VMAF | 0-100 | >= 70 | Video Multi-Method Assessment Fusion |

Both metrics must meet their thresholds for quality verification to pass.

### balanced+ Preset (Adaptive)

`balanced+` is an adaptive preset with the following behavior:

1. First convert with `balanced` and check SSIM/VMAF scores
2. If SSIM >= 0.95 and VMAF >= 70, finish as success
3. If either threshold is not met, reconvert with `high`
4. If `high` also fails, **best-effort mode** applies, adopting the result

In best-effort mode, conversion is treated as successful even if quality thresholds are not met. CLI output shows when best-effort mode was used:

```
Best-effort mode used:
  - video.mp4: preset=balanced, SSIM=0.9132, VMAF=68.5
```

## Workflow

### Basic Usage

```bash
# 1. Scan
vco scan

# 2. AWS configuration (first time only)
vco config set aws.s3_bucket my-bucket
vco config set aws.role_arn arn:aws:iam::123456789012:role/vco-mediaconvert-role

# 3. Convert
vco convert

# 4. Import
vco import --list          # Check list
vco import --all           # Batch import

# 5. Delete original videos (manual)
# Select and delete original videos in Photos app
```

### Efficient Conversion (Top-N)

To maximize storage savings, convert videos with largest file sizes first:

```bash
# Scan top 10
vco scan --top-n 10

# Convert top 5
vco convert --top-n 5
```

## Language Support

VCO CLI supports **English** and **Japanese** help messages.

### Automatic Language Detection

The CLI automatically detects your system locale:
- **Japanese locale** (ja, ja_JP, etc.): Help messages in Japanese
- **Other locales**: Help messages in English

**Note**: Output messages (progress, results, errors) are always in English for consistency and searchability.

## Development

### Initial Setup

After cloning the repository, build the Swift binary:

```bash
cd swift
./build_swift.sh --release
cp bin/vco-photos ../bin/
cd ..
pip install -e ".[dev]"
```

### Building Swift Binary

For development, you can build the Swift binary locally:

```bash
cd swift

# Build Universal Binary (arm64 + x86_64) - recommended
./build_swift.sh --release

# Or build for current architecture only (faster)
swift build -c release
```

The built binary is placed in `swift/bin/vco-photos`. Copy it to the project root:

```bash
cp swift/bin/vco-photos ../bin/vco-photos
```

### Running Tests

```bash
# All Python tests
python3.11 -m pytest tests/ -v

# Swift tests
cd swift && swift test

# Property tests
python3.11 -m pytest tests/properties/ -v

# Coverage
python3.11 -m pytest tests/ --cov=src/vco --cov-report=term-missing
```

### Code Quality

```bash
# Format
ruff format src/ tests/

# Lint
ruff check src/ tests/

# Type check
mypy src/
```

## MenuBar App (macOS)

A native macOS menu bar app that automates the full `scan → convert → polling → import` pipeline with one click.

### Build & Run

```bash
cd VCOMenuBar
swift build
swift test
```

### Features

- One-click pipeline execution from the menu bar
- Real-time status display (per-file progress)
- Automatic polling for AWS conversion completion
- Error recovery (disk space retry, auth expiry notification)
- State persistence across app restarts
- macOS notifications for pipeline events

### Requirements

- macOS 13.0 (Ventura) or later
- `vco` CLI installed and in PATH
- `~/.config/vco/config.json` configured

## License

MIT License

## Contributing & Support

- **Bug Reports**: [GitHub Issues](https://github.com/eijikominami/video-compression-optimizer/issues)
- **Feature Requests**: [GitHub Issues](https://github.com/eijikominami/video-compression-optimizer/issues)
- **Changelog**: [CHANGELOG.md](CHANGELOG.md)
