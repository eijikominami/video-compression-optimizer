# Video Compression Optimizer API Specification

## Overview

Video Compression Optimizer (VCO) provides a REST API for asynchronous video conversion using AWS MediaConvert. This API enables users to submit video conversion tasks, monitor progress, and download results.

**Base URL**: `https://{api-gateway-id}.execute-api.ap-northeast-1.amazonaws.com/Prod`

**API Version**: 1.2

**Authentication**: AWS Signature Version 4

## OpenAPI Specification

```yaml
openapi: 3.0.3
info:
  title: Video Compression Optimizer API
  description: REST API for asynchronous video conversion
  version: 1.2.0
  contact:
    name: VCO Support
    url: https://github.com/eijikominami/video-compression-optimizer

servers:
  - url: https://{api-gateway-id}.execute-api.ap-northeast-1.amazonaws.com/Prod
    description: Production API Gateway

security:
  - AWSSignatureV4: []

paths:
  /tasks:
    post:
      summary: Submit conversion task
      description: Create a new video conversion task with presigned upload URLs
      operationId: submitTask
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/TaskSubmitRequest'
      responses:
        '201':
          description: Task created successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TaskSubmitResponse'
        '400':
          description: Invalid request parameters
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '500':
          description: Internal server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /tasks/{task_id}:
    get:
      summary: Get task status
      description: Retrieve detailed status of a specific task
      operationId: getTaskStatus
      parameters:
        - name: task_id
          in: path
          required: true
          schema:
            type: string
            format: uuid
          description: Task ID
      responses:
        '200':
          description: Task status retrieved successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TaskStatusResponse'
        '404':
          description: Task not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '500':
          description: Internal server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /tasks/{task_id}/cancel:
    post:
      summary: Cancel task
      description: Cancel a running conversion task
      operationId: cancelTask
      parameters:
        - name: task_id
          in: path
          required: true
          schema:
            type: string
            format: uuid
          description: Task ID
      responses:
        '200':
          description: Task cancelled successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TaskCancelResponse'
        '404':
          description: Task not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '409':
          description: Task cannot be cancelled (already completed/failed)
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '500':
          description: Internal server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

  /tasks/{task_id}/files/{file_id}/cleanup:
    post:
      summary: Cleanup file after import or removal
      description: |
        Atomically update file status and delete S3 file. Used after successful import
        (action=downloaded) or when removing/clearing AWS items (action=removed).
        
        Processing order:
        1. Update DynamoDB status (DOWNLOADED or REMOVED)
        2. Delete S3 file
        
        If status update fails, S3 deletion is skipped and error is returned.
        If S3 deletion fails, warning is logged but success is returned (status is authoritative).
      operationId: cleanupFile
      parameters:
        - name: task_id
          in: path
          required: true
          schema:
            type: string
            format: uuid
          description: Task ID
        - name: file_id
          in: path
          required: true
          schema:
            type: string
            pattern: '^f[0-9]+$'
          description: File ID
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CleanupRequest'
      responses:
        '200':
          description: File cleanup completed successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/CleanupResponse'
        '400':
          description: Invalid action parameter
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '404':
          description: Task or file not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'
        '500':
          description: Internal server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ErrorResponse'

components:
  securitySchemes:
    AWSSignatureV4:
      type: apiKey
      name: Authorization
      in: header
      description: AWS Signature Version 4 authentication

  schemas:
    TaskSubmitRequest:
      type: object
      required:
        - files
        - quality_preset
      properties:
        files:
          type: array
          items:
            $ref: '#/components/schemas/FileSubmitInfo'
          minItems: 1
          maxItems: 100
          description: List of files to convert
        quality_preset:
          type: string
          enum: [balanced, high, compression, balanced+, high+]
          description: Quality preset for conversion
      example:
        files:
          - filename: "video1.mp4"
            file_size: 1048576
          - filename: "video2.mov"
            file_size: 2097152
        quality_preset: "balanced"

    FileSubmitInfo:
      type: object
      required:
        - filename
        - file_size
      properties:
        filename:
          type: string
          maxLength: 255
          pattern: '^[^/\\:*?"<>|]+\.(mp4|mov|avi|mkv|m4v)$'
          description: Original filename with extension
        file_size:
          type: integer
          minimum: 1
          maximum: 5368709120
          description: File size in bytes (max 5GB)
      example:
        filename: "sample_video.mp4"
        file_size: 1048576

    TaskSubmitResponse:
      type: object
      required:
        - task_id
        - upload_urls
        - expires_at
      properties:
        task_id:
          type: string
          format: uuid
          description: Generated task ID
        upload_urls:
          type: array
          items:
            $ref: '#/components/schemas/UploadUrlInfo'
          description: Presigned URLs for file upload
        expires_at:
          type: string
          format: date-time
          description: Upload URL expiration time (ISO 8601)
      example:
        task_id: "123e4567-e89b-12d3-a456-426614174000"
        upload_urls:
          - file_id: "f1"
            filename: "video1.mp4"
            upload_url: "https://s3.amazonaws.com/bucket/key?signature=..."
        expires_at: "2024-01-01T12:00:00Z"

    UploadUrlInfo:
      type: object
      required:
        - file_id
        - filename
        - upload_url
      properties:
        file_id:
          type: string
          pattern: '^f[0-9]+$'
          description: Generated file ID (f1, f2, ...)
        filename:
          type: string
          description: Original filename
        upload_url:
          type: string
          format: uri
          description: Presigned S3 upload URL
      example:
        file_id: "f1"
        filename: "video1.mp4"
        upload_url: "https://s3.amazonaws.com/bucket/key?signature=..."

    TaskStatusResponse:
      type: object
      required:
        - task_id
        - status
        - quality_preset
        - progress_percentage
        - current_step
        - files
        - created_at
        - updated_at
      properties:
        task_id:
          type: string
          format: uuid
          description: Task ID
        status:
          type: string
          enum: [PENDING, UPLOADING, CONVERTING, VERIFYING, COMPLETED, PARTIALLY_COMPLETED, FAILED, CANCELLED]
          description: Overall task status
        quality_preset:
          type: string
          enum: [balanced, high, compression, balanced+, high+]
          description: Quality preset used
        progress_percentage:
          type: integer
          minimum: 0
          maximum: 100
          description: Overall progress (calculated from file statuses)
        current_step:
          type: string
          enum: [pending, converting, verifying, completed]
          description: Current processing step
        files:
          type: array
          items:
            $ref: '#/components/schemas/FileStatus'
          description: Status of individual files
        created_at:
          type: string
          format: date-time
          description: Task creation time (ISO 8601)
        updated_at:
          type: string
          format: date-time
          description: Last update time (ISO 8601)
      example:
        task_id: "123e4567-e89b-12d3-a456-426614174000"
        status: "CONVERTING"
        quality_preset: "balanced"
        progress_percentage: 50
        current_step: "converting"
        files:
          - file_id: "f1"
            filename: "video1.mp4"
            status: "COMPLETED"
            conversion_progress_percentage: 100
            output_s3_key: "output/task123/f1/video1_h265.mp4"
        created_at: "2024-01-01T10:00:00Z"
        updated_at: "2024-01-01T10:30:00Z"

    FileStatus:
      type: object
      required:
        - file_id
        - filename
        - status
        - conversion_progress_percentage
      properties:
        file_id:
          type: string
          pattern: '^f[0-9]+$'
          description: File ID
        filename:
          type: string
          description: Original filename
        status:
          type: string
          enum: [PENDING, CONVERTING, VERIFYING, COMPLETED, DOWNLOADED, REMOVED, FAILED]
          description: |
            File processing status:
            - PENDING: Waiting to start
            - CONVERTING: MediaConvert job running
            - VERIFYING: SSIM quality check running
            - COMPLETED: Successfully processed, available for download
            - DOWNLOADED: File has been downloaded by user
            - REMOVED: File has been removed by user
            - FAILED: Processing failed
        conversion_progress_percentage:
          type: integer
          minimum: 0
          maximum: 100
          description: Individual file conversion progress (0=PENDING, 0-30=CONVERTING, 65=VERIFYING, 100=COMPLETED/DOWNLOADED/REMOVED/FAILED)
        output_s3_key:
          type: string
          nullable: true
          description: S3 key for converted file (null if not completed)
        error_message:
          type: string
          nullable: true
          description: Error message if status is FAILED
      example:
        file_id: "f1"
        filename: "video1.mp4"
        status: "COMPLETED"
        conversion_progress_percentage: 100
        output_s3_key: "output/task123/f1/video1_h265.mp4"
        error_message: null

    TaskCancelResponse:
      type: object
      required:
        - task_id
        - status
        - message
      properties:
        task_id:
          type: string
          format: uuid
          description: Task ID
        status:
          type: string
          enum: [CANCELLED]
          description: Updated task status
        message:
          type: string
          description: Cancellation confirmation message
      example:
        task_id: "123e4567-e89b-12d3-a456-426614174000"
        status: "CANCELLED"
        message: "Task cancelled successfully"

    CleanupRequest:
      type: object
      required:
        - action
      properties:
        action:
          type: string
          enum: [downloaded, removed]
          description: |
            Cleanup action:
            - downloaded: File was successfully imported (status → DOWNLOADED)
            - removed: File was removed/cleared by user (status → REMOVED)
      example:
        action: "downloaded"

    CleanupResponse:
      type: object
      required:
        - file_id
        - status
        - s3_deleted
        - cleaned_at
      properties:
        file_id:
          type: string
          pattern: '^f[0-9]+$'
          description: Cleaned up file ID
        status:
          type: string
          enum: [DOWNLOADED, REMOVED]
          description: Updated file status
        s3_deleted:
          type: boolean
          description: Whether S3 file was successfully deleted
        cleaned_at:
          type: string
          format: date-time
          description: Cleanup completion time (ISO 8601)
      example:
        file_id: "f1"
        status: "DOWNLOADED"
        s3_deleted: true
        cleaned_at: "2024-01-01T12:00:00Z"

    ErrorResponse:
      type: object
      required:
        - error
        - message
      properties:
        error:
          type: string
          description: Error code
        message:
          type: string
          description: Human-readable error message
        details:
          type: object
          description: Additional error details
      example:
        error: "INVALID_QUALITY_PRESET"
        message: "Quality preset 'invalid' is not supported"
        details:
          valid_presets: ["balanced", "high", "compression", "balanced+", "high+"]
```

## Error Codes

| Error Code | HTTP Status | Description |
|------------|-------------|-------------|
| `INVALID_REQUEST` | 400 | Request body validation failed |
| `INVALID_QUALITY_PRESET` | 400 | Unsupported quality preset |
| `INVALID_FILE_COUNT` | 400 | Too many files (max 100) |
| `INVALID_FILE_SIZE` | 400 | File size exceeds limit (5GB) |
| `INVALID_FILENAME` | 400 | Invalid filename format |
| `INVALID_ACTION` | 400 | Invalid cleanup action |
| `TASK_NOT_FOUND` | 404 | Task ID does not exist |
| `FILE_NOT_FOUND` | 404 | File ID does not exist |
| `TASK_NOT_CANCELLABLE` | 409 | Task already completed/failed |
| `INTERNAL_ERROR` | 500 | Server-side error |

## Rate Limits

- **Task submission**: 10 requests per minute per user
- **Status queries**: 100 requests per minute per user
- **Cancel operations**: 5 requests per minute per user
- **Cleanup operations**: 50 requests per minute per user

## Data Retention

- **Task records**: 90 days (automatic cleanup)
- **S3 files**: Deleted after successful import/removal or task TTL
- **CloudWatch logs**: 30 days retention

## Usage Examples

### Submit Task

```bash
curl -X POST https://api.example.com/tasks \
  -H "Content-Type: application/json" \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -d '{
    "files": [
      {"filename": "video.mp4", "file_size": 1048576}
    ],
    "quality_preset": "balanced"
  }'
```

### Check Status

```bash
curl -X GET https://api.example.com/tasks/123e4567-e89b-12d3-a456-426614174000 \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

### Cancel Task

```bash
curl -X POST https://api.example.com/tasks/123e4567-e89b-12d3-a456-426614174000/cancel \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

### Cleanup File (after import)

```bash
curl -X POST https://api.example.com/tasks/123e4567-e89b-12d3-a456-426614174000/files/f1/cleanup \
  -H "Content-Type: application/json" \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -d '{
    "action": "downloaded"
  }'
```

### Cleanup File (on removal)

```bash
curl -X POST https://api.example.com/tasks/123e4567-e89b-12d3-a456-426614174000/files/f1/cleanup \
  -H "Content-Type: application/json" \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -d '{
    "action": "removed"
  }'
```

## Integration Notes

### CLI Integration

The VCO CLI automatically handles:
- AWS Signature V4 authentication
- Presigned URL uploads
- Progress polling
- Error handling and retries
- File cleanup after import/removal

### S3 Key Structure

```
tasks/{task_id}/source/{file_id}/{filename}     # Source files
output/{task_id}/{file_id}/{stem}_h265.mp4     # Converted files
tasks/{task_id}/metadata/{file_id}/{filename}.json  # Metadata
```

### DynamoDB Schema

Tasks are stored with the following structure:
- **Partition Key**: `task_id` (String)
- **Attributes**: `status`, `quality_preset`, `files`, `created_at`, `updated_at`, `ttl`

### File Status Transitions

```
PENDING → CONVERTING → VERIFYING → COMPLETED → DOWNLOADED
                                 ↘ FAILED     ↘ REMOVED
```

- `COMPLETED`: File is ready for download
- `DOWNLOADED`: File has been downloaded by user (excluded from `import --list`)
- `REMOVED`: File has been removed by user (excluded from `import --list`)

## Changelog

- **v1.2.0**: Replaced download-status with cleanup endpoint
  - Removed `/tasks/{task_id}/download-status` endpoint
  - Added `/tasks/{task_id}/files/{file_id}/cleanup` endpoint
  - Added `REMOVED` to FileStatus enum
  - Cleanup atomically updates status and deletes S3 file
  - Status update is authoritative (S3 deletion failure is non-fatal)
- **v1.1.0**: Refactored download status management
  - Added `DOWNLOADED` to FileStatus enum
  - Changed `DownloadStatusRequest` from `file_ids` (array) to `file_id` (single)
  - Changed `DownloadStatusResponse` to return `file_id`, `status`, `downloaded_at`
  - Removed `downloaded_at` and `download_available` from FileStatus schema
- **v1.0.0**: Initial API specification
