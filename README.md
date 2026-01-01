English / [**日本語**](README_JP.md)

# Video Compression Optimizer (VCO)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)

A tool to convert videos in Apple Photos to H.265 format to save storage space.

## Features

- Automatic scanning of Apple Photos library videos
- High-quality H.265 conversion using AWS MediaConvert
- SSIM-based quality verification
- Metadata preservation (capture date, location, albums)
- iCloud video status detection
- Efficient conversion with Top-N selection

## Prerequisites

- macOS 10.15 (Catalina) or later
- Python 3.10 or later
- AWS account (MediaConvert, S3, Lambda)
- For iCloud videos, download originals in Photos app first

## Installation

```bash
pip install .
```

Development environment:

```bash
pip install -e ".[dev]"
```

## AWS Infrastructure Deployment

### 1. Create FFmpeg Lambda Layer

The quality check Lambda function requires FFmpeg. Create a Lambda Layer with the following script:

```bash
cd sam-app/scripts

# Create and deploy Layer
./create-ffmpeg-layer.sh \
  --bucket <your-s3-bucket> \
  --profile <your-aws-profile> \
  --region ap-northeast-1

# Dry-run mode (create ZIP only, no deployment)
./create-ffmpeg-layer.sh --dry-run
```

The script performs:
1. Download FFmpeg static build
2. Create ZIP for Lambda Layer
3. Upload to S3
4. Publish Lambda Layer

### 2. Deploy SAM Template

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
```

### Async Workflow

Conversions are processed asynchronously via AWS Step Functions. After submitting a conversion, you can check status and manage tasks:

```bash
# Check task status
vco status                    # List all active tasks
vco status <task-id>          # Show task details

# Cancel a running task
vco cancel <task-id>

# Import completed files (replaces vco download)
vco import --list             # List all importable items (local + AWS)
vco import --all              # Import all items
vco import <task-id:file-id>  # Import specific AWS file
```

#### Async Workflow Benefits

- **Background processing**: Submit tasks and check status later
- **Parallel conversion**: Multiple files processed concurrently
- **Unified import**: Import from both local queue and AWS with single command
- **Partial completion**: Import successful files even if some fail

### Import

Import converted videos from both local queue and AWS completed tasks:

```bash
# Show import queue (local + AWS)
vco import --list

# Import specified video to Photos
vco import <item-id>          # Local: review-id, AWS: task-id:file-id

# Batch import all videos (local + AWS)
vco import --all

# Remove specified ID from queue (also deletes files)
vco import --remove <item-id>

# Clear local review queue only (also deletes files)
vco import --clear
```

**Item ID formats**:
- Local items: `abc123` (review ID)
- AWS items: `task-uuid:file-uuid` (task:file format)

**Note**: 
- The `--remove` and `--clear` options delete both the queue entry and the corresponding converted video and metadata files.
- `--clear` only affects local queue; AWS items remain in S3.
- `vco download` is deprecated. Use `vco import` instead.

After import, manually delete original videos in Photos app.

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

### balanced+ Preset (Adaptive)

`balanced+` is an adaptive preset with the following behavior:

1. First convert with `balanced` and check SSIM score
2. If SSIM >= 0.95, finish as success
3. If SSIM < 0.95, reconvert with `high`
4. If `high` also has SSIM < 0.95, **best-effort mode** applies, adopting the result with higher SSIM score

In best-effort mode, conversion is treated as successful even if SSIM threshold is not met. CLI output shows when best-effort mode was used:

```
Best-effort mode used:
  - video.mp4: preset=balanced, SSIM=0.9132
```

## iCloud Video Processing

Videos stored only in iCloud (not downloaded locally) cannot be automatically downloaded. This is a limitation of the osxphotos library.

### Scan Behavior

When running `vco scan`, each video's iCloud status (Local/iCloud) is displayed:

```
⚠ 10 videos are in iCloud only and need to be downloaded first.
Open Photos app and download these videos before running 'vco convert':

  - IMG_1234.mov
  - IMG_5678.mov
  ...
```

### Convert Behavior

When running `vco convert`, only locally available videos are converted. iCloud-only videos are skipped.

### Manual Download Steps

1. Open Photos app
2. Select iCloud-only videos
3. Right-click → Select "Download Original"
4. After download completes, re-run `vco scan` to update file paths
5. Run `vco convert`

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

### Running Tests

```bash
# All tests
python3.11 -m pytest tests/ -v

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

## License

MIT License

## Contributing & Support

- **Bug Reports**: [GitHub Issues](https://github.com/eijikominami/video-compression-optimizer/issues)
- **Feature Requests**: [GitHub Issues](https://github.com/eijikominami/video-compression-optimizer/issues)
- **Changelog**: [CHANGELOG.md](CHANGELOG.md)
