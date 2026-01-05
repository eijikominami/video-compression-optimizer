"""Quality Checker Lambda Function.

Validates video conversion quality by checking:
1. File size comparison (converted must be smaller than original)
2. Playback verification (FFmpeg probe)
3. SSIM calculation (must be >= 0.95)
4. Metadata extraction

The function downloads videos from S3, performs quality checks,
and saves results as JSON to S3.

Progress updates (verification_progress):
- 0: SSIM calculation started
- 30: Frame extraction complete
- 100: SSIM calculation complete
"""

import json
import logging
import os
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "")
SSIM_THRESHOLD = float(os.environ.get("SSIM_THRESHOLD", "0.95"))
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "")


@dataclass
class VideoMetadata:
    """Video metadata extracted from FFprobe."""

    codec: str
    resolution: tuple[int, int]
    bitrate: int
    duration: float
    frame_rate: float
    file_size: int


@dataclass
class QualityResult:
    """Quality check result."""

    job_id: str
    original_s3_key: str
    converted_s3_key: str
    status: str  # 'passed', 'failed'
    ssim_score: float | None
    original_size: int
    converted_size: int
    compression_ratio: float
    space_saved_bytes: int
    space_saved_percent: float
    playback_verified: bool
    failure_reason: str | None
    converted_metadata: dict | None
    metadata_embedded: bool
    metadata_embed_error: str | None
    timestamp: str


def get_s3_client():
    """Get S3 client."""
    return boto3.client("s3")


def get_dynamodb_table():
    """Get DynamoDB table resource."""
    if not DYNAMODB_TABLE:
        return None
    dynamodb = boto3.resource("dynamodb")
    return dynamodb.Table(DYNAMODB_TABLE)


def update_verification_progress(task_id: str, file_id: str, progress: int) -> bool:
    """Update verification progress for a file in DynamoDB.

    Args:
        task_id: Task ID
        file_id: File ID within the task
        progress: Progress value (0-100)
            - 0: SSIM calculation started
            - 30: Frame extraction complete
            - 100: SSIM calculation complete

    Returns:
        True if update succeeded, False otherwise
    """
    table = get_dynamodb_table()
    if not table:
        logger.warning("DynamoDB table not configured, skipping progress update")
        return False

    try:
        # Get current task to find file index
        response = table.get_item(Key={"task_id": task_id, "sk": "TASK"})
        if "Item" not in response:
            logger.warning(f"Task not found: {task_id}")
            return False

        task = response["Item"]
        files = task.get("files", [])

        # Find file index
        file_index = None
        for i, f in enumerate(files):
            if f.get("file_id") == file_id:
                file_index = i
                break

        if file_index is None:
            logger.warning(f"File not found: {file_id} in task {task_id}")
            return False

        # Update verification_progress for the specific file
        table.update_item(
            Key={"task_id": task_id, "sk": "TASK"},
            UpdateExpression=f"SET files[{file_index}].verification_progress = :progress, updated_at = :updated_at",
            ExpressionAttributeValues={
                ":progress": progress,
                ":updated_at": datetime.utcnow().isoformat() + "Z",
            },
        )
        logger.info(f"Updated verification_progress to {progress} for {task_id}:{file_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to update verification_progress: {e}")
        return False


def download_from_s3(s3_key: str, local_path: str) -> None:
    """Download file from S3 to local path."""
    s3 = get_s3_client()
    logger.info(f"Downloading s3://{S3_BUCKET}/{s3_key} to {local_path}")
    s3.download_file(S3_BUCKET, s3_key, local_path)


def upload_to_s3(local_path: str, s3_key: str) -> None:
    """Upload file from local path to S3."""
    s3 = get_s3_client()
    logger.info(f"Uploading {local_path} to s3://{S3_BUCKET}/{s3_key}")
    s3.upload_file(local_path, S3_BUCKET, s3_key)


def get_file_size_from_s3(s3_key: str) -> int:
    """Get file size from S3 without downloading."""
    s3 = get_s3_client()
    response = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
    return response["ContentLength"]


def run_ffprobe(video_path: str) -> dict:
    """Run FFprobe to get video metadata."""
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        video_path,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    if result.returncode != 0:
        raise RuntimeError(f"FFprobe failed: {result.stderr}")

    return json.loads(result.stdout)


def extract_metadata(video_path: str, file_size: int) -> VideoMetadata:
    """Extract video metadata using FFprobe."""
    probe_data = run_ffprobe(video_path)

    # Find video stream
    video_stream = None
    for stream in probe_data.get("streams", []):
        if stream.get("codec_type") == "video":
            video_stream = stream
            break

    if not video_stream:
        raise ValueError("No video stream found")

    # Extract metadata
    codec = video_stream.get("codec_name", "unknown")
    width = int(video_stream.get("width", 0))
    height = int(video_stream.get("height", 0))

    # Get bitrate from format or calculate
    format_info = probe_data.get("format", {})
    bitrate = int(format_info.get("bit_rate", 0))

    # Get duration
    duration = float(format_info.get("duration", 0))

    # Get frame rate
    frame_rate_str = video_stream.get("r_frame_rate", "0/1")
    if "/" in frame_rate_str:
        num, den = frame_rate_str.split("/")
        frame_rate = float(num) / float(den) if float(den) > 0 else 0.0
    else:
        frame_rate = float(frame_rate_str)

    return VideoMetadata(
        codec=codec,
        resolution=(width, height),
        bitrate=bitrate,
        duration=duration,
        frame_rate=frame_rate,
        file_size=file_size,
    )


def verify_playback(video_path: str) -> bool:
    """Verify video is playable using FFprobe."""
    try:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_type",
            "-of",
            "csv=p=0",
            video_path,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return result.returncode == 0 and "video" in result.stdout
    except Exception as e:
        logger.error(f"Playback verification failed: {e}")
        return False


def calculate_ssim(original_path: str, converted_path: str) -> float:
    """Calculate SSIM score between original and converted video.

    Uses FFmpeg's ssim filter to compare videos frame by frame.
    Returns the average SSIM score (0.0 to 1.0).

    Note: FFmpeg ssim filter expects the reference (original) as the second input.
    The first input is the distorted/converted video, second is the reference.
    """
    cmd = [
        "ffmpeg",
        "-i",
        converted_path,  # First input: distorted/converted video
        "-i",
        original_path,  # Second input: reference/original video
        "-lavfi",
        "ssim=stats_file=-",
        "-f",
        "null",
        "-",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

    # Parse SSIM output - look for "All:" line which contains average SSIM
    # Format: "SSIM Y:0.987654 (19.123456) U:0.987654 (19.123456) V:0.987654 (19.123456) All:0.987654 (19.123456)"
    ssim_score = 0.0

    for line in result.stderr.split("\n"):
        if "All:" in line:
            # Extract the All: value
            try:
                all_part = line.split("All:")[1].strip()
                ssim_str = all_part.split()[0]
                ssim_score = float(ssim_str)
            except (IndexError, ValueError) as e:
                logger.warning(f"Failed to parse SSIM from line: {line}, error: {e}")

    return ssim_score


def get_metadata_from_s3(metadata_s3_key: str) -> dict | None:
    """Get metadata JSON from S3.

    Args:
        metadata_s3_key: S3 key for metadata JSON file

    Returns:
        Metadata dict or None if not found/invalid
    """
    if not metadata_s3_key:
        return None

    try:
        s3 = get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=metadata_s3_key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        logger.warning(f"Failed to get metadata from S3: {e}")
        return None


def embed_metadata(video_path: str, metadata: dict, output_path: str) -> tuple[bool, str | None]:
    """Embed metadata into video file using FFmpeg.

    Embeds:
    - creation_time: capture date/time
    - location: GPS coordinates (if available)

    Args:
        video_path: Path to input video
        metadata: Metadata dict with capture_date, location, etc.
        output_path: Path for output video with embedded metadata

    Returns:
        Tuple of (success, error_message)
    """
    try:
        # Build FFmpeg metadata arguments
        metadata_args = []

        # Add creation_time from capture_date
        capture_date = metadata.get("capture_date")
        if capture_date:
            # FFmpeg expects format: YYYY-MM-DDTHH:MM:SS.000000Z
            metadata_args.extend(["-metadata", f"creation_time={capture_date}"])

        # Add location if available
        location = metadata.get("location")
        if location and len(location) == 2:
            lat, lon = location
            # FFmpeg location format: +/-DD.DDDD+/-DDD.DDDD/
            lat_sign = "+" if lat >= 0 else ""
            lon_sign = "+" if lon >= 0 else ""
            location_str = f"{lat_sign}{lat:.4f}{lon_sign}{lon:.4f}/"
            metadata_args.extend(["-metadata", f"location={location_str}"])

        if not metadata_args:
            logger.info("No metadata to embed")
            return False, "No metadata to embed"

        # Build FFmpeg command
        cmd = (
            [
                "ffmpeg",
                "-i",
                video_path,
                "-c",
                "copy",  # Copy streams without re-encoding
                "-map_metadata",
                "0",  # Copy existing metadata
            ]
            + metadata_args
            + [
                "-y",  # Overwrite output
                output_path,
            ]
        )

        logger.info(f"Embedding metadata with command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        if result.returncode != 0:
            error_msg = f"FFmpeg failed: {result.stderr}"
            logger.error(error_msg)
            return False, error_msg

        return True, None

    except subprocess.TimeoutExpired:
        return False, "Metadata embedding timed out"
    except Exception as e:
        error_msg = f"Metadata embedding failed: {str(e)}"
        logger.exception(error_msg)
        return False, error_msg


def check_quality(
    original_s3_key: str,
    converted_s3_key: str,
    job_id: str,
    metadata_s3_key: str | None = None,
    task_id: str | None = None,
    file_id: str | None = None,
) -> QualityResult:
    """Perform comprehensive quality check on converted video.

    Steps:
    1. Get file sizes from S3
    2. Download both videos and metadata
    3. Embed metadata into converted video (if provided)
    4. Verify converted video is playable
    5. Calculate SSIM score (with progress updates)
    6. Extract metadata from converted video
    7. Determine pass/fail status

    Args:
        original_s3_key: S3 key for original video
        converted_s3_key: S3 key for converted video
        job_id: Quality check job ID
        metadata_s3_key: S3 key for metadata JSON (optional)
        task_id: Async task ID for progress updates (optional)
        file_id: File ID within task for progress updates (optional)
    """
    timestamp = datetime.utcnow().isoformat() + "Z"
    metadata_embedded = False
    metadata_embed_error = None

    # Update verification progress: SSIM calculation started (0%)
    if task_id and file_id:
        update_verification_progress(task_id, file_id, 0)

    # Get file sizes
    original_size = get_file_size_from_s3(original_s3_key)
    converted_size = get_file_size_from_s3(converted_s3_key)

    # Calculate compression metrics
    compression_ratio = original_size / converted_size if converted_size > 0 else 0.0
    space_saved_bytes = original_size - converted_size
    space_saved_percent = (space_saved_bytes / original_size * 100) if original_size > 0 else 0.0

    # Check if file size is reduced
    if converted_size >= original_size:
        return QualityResult(
            job_id=job_id,
            original_s3_key=original_s3_key,
            converted_s3_key=converted_s3_key,
            status="failed",
            ssim_score=None,
            original_size=original_size,
            converted_size=converted_size,
            compression_ratio=compression_ratio,
            space_saved_bytes=space_saved_bytes,
            space_saved_percent=space_saved_percent,
            playback_verified=False,
            failure_reason="Converted file is not smaller than original",
            converted_metadata=None,
            metadata_embedded=False,
            metadata_embed_error=None,
            timestamp=timestamp,
        )

    # Get metadata from S3 if provided
    video_metadata = None
    if metadata_s3_key:
        video_metadata = get_metadata_from_s3(metadata_s3_key)
        logger.info(f"Retrieved metadata: {video_metadata}")

    # Download files for detailed analysis
    with tempfile.TemporaryDirectory() as tmpdir:
        original_path = os.path.join(tmpdir, "original.mp4")
        converted_path = os.path.join(tmpdir, "converted.mp4")

        download_from_s3(original_s3_key, original_path)
        download_from_s3(converted_s3_key, converted_path)

        # Update verification progress: Frame extraction complete (30%)
        if task_id and file_id:
            update_verification_progress(task_id, file_id, 30)

        # Embed metadata if provided
        if video_metadata:
            embedded_path = os.path.join(tmpdir, "converted_with_metadata.mp4")
            success, error = embed_metadata(converted_path, video_metadata, embedded_path)

            if success:
                # Replace converted file with metadata-embedded version
                os.replace(embedded_path, converted_path)
                metadata_embedded = True
                logger.info("Metadata embedded successfully")

                # Upload the metadata-embedded file back to S3
                upload_to_s3(converted_path, converted_s3_key)

                # Update converted size after metadata embedding
                converted_size = os.path.getsize(converted_path)
                compression_ratio = original_size / converted_size if converted_size > 0 else 0.0
                space_saved_bytes = original_size - converted_size
                space_saved_percent = (
                    (space_saved_bytes / original_size * 100) if original_size > 0 else 0.0
                )
            else:
                metadata_embed_error = error
                logger.warning(f"Metadata embedding failed: {error}")

        # Verify playback
        playback_ok = verify_playback(converted_path)
        if not playback_ok:
            return QualityResult(
                job_id=job_id,
                original_s3_key=original_s3_key,
                converted_s3_key=converted_s3_key,
                status="failed",
                ssim_score=None,
                original_size=original_size,
                converted_size=converted_size,
                compression_ratio=compression_ratio,
                space_saved_bytes=space_saved_bytes,
                space_saved_percent=space_saved_percent,
                playback_verified=False,
                failure_reason="Converted video is not playable",
                converted_metadata=None,
                metadata_embedded=metadata_embedded,
                metadata_embed_error=metadata_embed_error,
                timestamp=timestamp,
            )

        # Calculate SSIM
        ssim_score = calculate_ssim(original_path, converted_path)

        # Update verification progress: SSIM calculation complete (100%)
        if task_id and file_id:
            update_verification_progress(task_id, file_id, 100)

        if ssim_score < SSIM_THRESHOLD:
            return QualityResult(
                job_id=job_id,
                original_s3_key=original_s3_key,
                converted_s3_key=converted_s3_key,
                status="failed",
                ssim_score=ssim_score,
                original_size=original_size,
                converted_size=converted_size,
                compression_ratio=compression_ratio,
                space_saved_bytes=space_saved_bytes,
                space_saved_percent=space_saved_percent,
                playback_verified=True,
                failure_reason=f"SSIM score {ssim_score:.4f} is below threshold {SSIM_THRESHOLD}",
                converted_metadata=None,
                metadata_embedded=metadata_embedded,
                metadata_embed_error=metadata_embed_error,
                timestamp=timestamp,
            )

        # Extract metadata
        metadata = extract_metadata(converted_path, converted_size)
        converted_metadata = {
            "codec": metadata.codec,
            "resolution": list(metadata.resolution),
            "bitrate": metadata.bitrate,
            "duration": metadata.duration,
            "frame_rate": metadata.frame_rate,
        }

        # All checks passed
        return QualityResult(
            job_id=job_id,
            original_s3_key=original_s3_key,
            converted_s3_key=converted_s3_key,
            status="passed",
            ssim_score=ssim_score,
            original_size=original_size,
            converted_size=converted_size,
            compression_ratio=compression_ratio,
            space_saved_bytes=space_saved_bytes,
            space_saved_percent=space_saved_percent,
            playback_verified=True,
            failure_reason=None,
            converted_metadata=converted_metadata,
            metadata_embedded=metadata_embedded,
            metadata_embed_error=metadata_embed_error,
            timestamp=timestamp,
        )


def save_result_to_s3(result: QualityResult) -> str:
    """Save quality check result to S3 as JSON."""
    result_key = f"results/{result.job_id}.json"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(asdict(result), f, indent=2)
        temp_path = f.name

    try:
        upload_to_s3(temp_path, result_key)
    finally:
        os.unlink(temp_path)

    return result_key


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda handler for quality check.

    Expected event format:
    {
        "job_id": "quality_001",
        "original_s3_key": "input/video.mp4",
        "converted_s3_key": "output/video_h265.mp4",
        "metadata_s3_key": "input/uuid/metadata.json",  // optional
        "task_id": "async-task-uuid",  // optional, for progress updates
        "file_id": "file-uuid"  // optional, for progress updates
    }

    Returns:
    {
        "statusCode": 200,
        "body": {
            "job_id": "quality_001",
            "status": "passed",
            "result_s3_key": "results/quality_001.json",
            "metadata_embedded": true,
            ...
        }
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # Extract parameters
        job_id = event.get("job_id")
        original_s3_key = event.get("original_s3_key")
        converted_s3_key = event.get("converted_s3_key")
        metadata_s3_key = event.get("metadata_s3_key")  # Optional
        task_id = event.get("task_id")  # Optional, for progress updates
        file_id = event.get("file_id")  # Optional, for progress updates

        if not all([job_id, original_s3_key, converted_s3_key]):
            return {
                "statusCode": 400,
                "body": {
                    "error": "Missing required parameters: job_id, original_s3_key, converted_s3_key"
                },
            }

        # Perform quality check
        result = check_quality(
            original_s3_key, converted_s3_key, job_id, metadata_s3_key, task_id, file_id
        )

        # Save result to S3
        result_key = save_result_to_s3(result)

        # Return response
        response_body = asdict(result)
        response_body["result_s3_key"] = result_key

        return {"statusCode": 200, "body": response_body}

    except Exception as e:
        logger.exception(f"Quality check failed: {e}")
        return {"statusCode": 500, "body": {"error": str(e), "job_id": event.get("job_id")}}
