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
8. User imports completed files with `vco import` (Swift PhotoKit)
```

### File Status Transitions

```
PENDING → CONVERTING → VERIFYING → COMPLETED → DOWNLOADED
                                 ↘ FAILED     ↘ REMOVED
```

## Technology Stack

| Layer | Technology |
|-------|------------|
| CLI | Python 3.10+, Click, Rich |
| Photos Scanning | osxphotos |
| iCloud Download & Import | Swift PhotoKit |
| AWS SDK | boto3 |
| Infrastructure | SAM/CloudFormation |
| Video Processing | AWS MediaConvert |
| Quality Check | FFmpeg (Lambda Layer), SSIM |

## Design Decisions

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
