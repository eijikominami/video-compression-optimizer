# Architecture

## Overview

Video Compression Optimizer (VCO) is a tool that converts videos in Apple Photos to H.265 format to save storage space. It uses AWS cloud services for high-quality video conversion with asynchronous processing via Step Functions.

## System Context

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              User Environment                            │
│  ┌──────────┐    ┌─────────────┐    ┌──────────────────────────────┐   │
│  │   User   │───▶│   VCO CLI   │───▶│      Apple Photos Library     │   │
│  └──────────┘    └──────┬──────┘    └──────────────────────────────┘   │
│                         │                                                │
└─────────────────────────┼────────────────────────────────────────────────┘
                          │ HTTPS/AWS SigV4
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              AWS Cloud                                   │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────┐ │
│  │ API Gateway │───▶│   Lambda    │───▶│      Step Functions         │ │
│  └─────────────┘    └─────────────┘    └──────────────┬──────────────┘ │
│                                                        │                 │
│  ┌─────────────┐    ┌─────────────┐    ┌──────────────▼──────────────┐ │
│  │  DynamoDB   │◀───│MediaConvert │◀───│    Workflow Lambda          │ │
│  └─────────────┘    └─────────────┘    └─────────────────────────────┘ │
│                           │                                              │
│                     ┌─────▼─────┐                                        │
│                     │    S3     │                                        │
│                     └───────────┘                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### VCO CLI

Python-based command-line interface for video operations.

| Command | Description |
|---------|-------------|
| `vco scan` | Scan Apple Photos library for videos |
| `vco convert` | Submit videos for H.265 conversion |
| `vco status` | Check conversion task status |
| `vco import` | Import converted videos to Photos |
| `vco cancel` | Cancel running conversion tasks |
| `vco config` | Manage configuration settings |

### Swift PhotoKit Binary

Native Swift implementation for Photos library access (`bin/vco-photos`).

- Universal Binary (arm64 + x86_64)
- iCloud video automatic download
- Video import to Photos library

### AWS Infrastructure

| Resource | Purpose |
|----------|---------|
| **API Gateway** | REST API endpoints for async operations |
| **Lambda Functions** | Task submission, status, cancel, workflow orchestration |
| **Step Functions** | Async workflow state machine |
| **MediaConvert** | H.265 video transcoding |
| **S3** | Video file storage (source, output, metadata) |
| **DynamoDB** | Task and file status tracking (90-day TTL) |

## Data Flow

### Conversion Workflow

```
1. User runs `vco convert`
2. CLI scans Photos library (osxphotos)
3. iCloud videos are automatically downloaded (Swift PhotoKit)
4. CLI uploads videos to S3 via presigned URLs
5. CLI starts Step Functions execution
6. Workflow Lambda orchestrates:
   a. MediaConvert job creation
   b. Quality verification (SSIM check)
   c. Status updates to DynamoDB
7. User checks status with `vco status`
8. User imports completed files with `vco import`:
   a. Download converted video from S3
   b. Embed metadata using exiftool (Keys:CreationDate with timezone)
   c. Verify metadata matches original
   d. Import to Photos library (Swift PhotoKit)
```

### File Status Transitions

```
PENDING → CONVERTING → COMPLETED → DOWNLOADED
                    ↘ FAILED     ↘ REMOVED
```

Note: Quality evaluation (SSIM/VMAF) is performed as part of the CONVERTING phase completion using MediaConvert per-frame metrics.

## Technology Stack

| Layer | Technology |
|-------|------------|
| CLI | Python 3.10+, Click, Rich |
| Photos Scanning | osxphotos |
| iCloud Download & Import | Swift PhotoKit |
| Metadata Embedding | exiftool |
| AWS SDK | boto3 |
| Infrastructure | SAM/CloudFormation |
| Video Processing | AWS MediaConvert |
| Quality Check | MediaConvert Per-Frame Metrics (SSIM, VMAF) |

## Design Decisions

### boto3 Lambda Layer (Workaround)

The AsyncWorkflowFunction uses a custom boto3 Layer to support MediaConvert's `PerFrameMetrics` parameter.

**Background:**
- Lambda's default boto3 (~1.28.x) doesn't support the `PerFrameMetrics` parameter
- `PerFrameMetrics` requires boto3 1.34.x or later
- This Layer provides boto3 1.35+ for the required API support

**Important:** Do not include boto3 in `async-workflow/requirements.txt`. If boto3 is included in the function code, it takes precedence over the Layer (`/var/task/` has higher priority than `/opt/python/`).

**When to Remove:**
This workaround can be removed when AWS updates Lambda's default boto3 to 1.34.x or later. At that point:
1. Remove `Boto3Layer` resource from `sam-app/template.yaml`
2. Remove `Layers` property from `AsyncWorkflowFunction`
3. Delete `sam-app/layers/boto3/` directory

**Check Lambda's boto3 version:**
```python
import boto3
print(boto3.__version__)
```

### Swift Native Implementation

Adopted Swift PhotoKit for iCloud download and Photos import:
- Native iCloud download support (unstable with osxphotos)
- Direct import to Photos library
- Scanning continues to use osxphotos (stability and detailed metadata)

### Async-Only Processing

Removed synchronous conversion mode:
- All conversions use AWS Step Functions
- Better handling of long-running jobs
- Parallel file processing
- Resumable downloads

### Quality Presets

| Preset | QVBR | Use Case |
|--------|------|----------|
| `high` | 8-9 | Quality priority |
| `balanced` | 6-7 | Recommended default |
| `balanced+` | 6-7 → 8-9 | Adaptive (retry with high if SSIM < 0.95) |
| `compression` | 4-5 | Maximum compression |

### MediaConvert Encoding Settings

H.265 encoding is optimized for high quality and efficient compression using AWS recommended settings:

| Setting | Value | Purpose |
|---------|-------|---------|
| `QualityTuningLevel` | `MULTI_PASS_HQ` | 2-pass encoding for optimal bitrate allocation |
| `DynamicSubGop` | `ADAPTIVE` | Dynamic B-frame adjustment based on content |
| `GopBReference` | `ENABLED` | B-frame reference for better compression |
| `AdaptiveQuantization` | `AUTO` | Automatic quantization optimization |
| `GopSizeUnits` | `AUTO` | MediaConvert auto-selects optimal GOP size |
| `FlickerAdaptiveQuantization` | `ENABLED` | Reduces flicker artifacts |
| `SpatialAdaptiveQuantization` | `ENABLED` | Optimizes spatial detail preservation |
| `TemporalAdaptiveQuantization` | `ENABLED` | Optimizes temporal consistency |
| `SampleAdaptiveOffsetFilterMode` | `ADAPTIVE` | Reduces banding artifacts |

These settings prioritize quality while achieving efficient compression. The `MULTI_PASS_HQ` mode analyzes the entire video first to allocate bitrate optimally across scenes.

### S3 Key Structure

```
tasks/{task_id}/source/{file_id}/{filename}        # Source files
output/{task_id}/{file_id}/{stem}_h265.mp4         # Converted files
tasks/{task_id}/metadata/{file_id}/{filename}.json # Metadata
```

## Security

- AWS Signature V4 authentication for all API calls
- IAM roles with least-privilege access
- S3 presigned URLs for secure uploads/downloads
- No credentials stored in CLI (uses AWS profiles)

## References

- [README.md](README.md) - Installation and usage
- [docs/api-specification.md](docs/api-specification.md) - REST API details
- [docs/data-models.md](docs/data-models.md) - Data model specifications
