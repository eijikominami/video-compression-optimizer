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
import re
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import boto3

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


def calculate_ssim(original_path: str, converted_path: str, task_id: str = None, file_id: str = None) -> float:
    """Calculate SSIM score between original and converted video.

    Uses FFmpeg's ssim filter to compare videos frame by frame.
    Returns the average SSIM score (0.0 to 1.0).

    Note: FFmpeg ssim filter expects the reference (original) as the second input.
    The first input is the distorted/converted video, second is the reference.
    
    Args:
        original_path: Path to original video file
        converted_path: Path to converted video file
        task_id: Task ID for progress updates (optional)
        file_id: File ID for progress updates (optional)
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

    # Get total frames for progress calculation
    total_frames = None
    if task_id and file_id:
        try:
            total_frames = get_total_frames(original_path)
        except Exception as e:
            logger.warning(f"Failed to get total frames: {e}")

    # Start FFmpeg process
    process = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True)
    
    # Progress tracking
    last_update_time = 0
    last_progress = 30
    stderr_lines = []
    
    try:
        # Read FFmpeg output line by line
        for line in iter(process.stderr.readline, ''):
            stderr_lines.append(line)
            
            # Update progress if task_id and file_id provided
            if task_id and file_id and total_frames and line.startswith("n:"):
                try:
                    import time
                    import re
                    
                    # Extract current frame number from SSIM output (n:12345 format)
                    match = re.search(r'n:(\d+)', line)
                    if match:
                        current_frame = int(match.group(1))
                        
                        # Calculate progress (30% to 99%)
                        progress = 30 + int((current_frame / total_frames) * 69)
                        progress = min(progress, 99)  # Cap at 99%
                        
                        # Update every 30 seconds or 5% progress change
                        now = time.time()
                        if (now - last_update_time >= 30) or (progress - last_progress >= 5):
                            update_verification_progress(task_id, file_id, progress)
                            last_update_time = now
                            last_progress = progress
                            
                except Exception as e:
                    logger.warning(f"Failed to update progress: {e}")
        
        # Wait for process completion
        return_code = process.wait(timeout=1800)
        
        if return_code != 0:
            stderr_output = ''.join(stderr_lines)
            raise RuntimeError(f"FFmpeg failed with return code {return_code}: {stderr_output}")
            
    except subprocess.TimeoutExpired:
        process.kill()
        raise RuntimeError("FFmpeg SSIM calculation timed out after 30 minutes")

    # Parse SSIM output - look for "All:" line which contains average SSIM
    # Format: "SSIM Y:0.987654 (19.123456) U:0.987654 (19.123456) V:0.987654 (19.123456) All:0.987654 (19.123456)"
    ssim_score = 0.0
    stderr_output = ''.join(stderr_lines)

    for line in stderr_output.split("\n"):
        if "All:" in line:
            # Extract the All: value
            try:
                all_part = line.split("All:")[1].strip()
                ssim_str = all_part.split()[0]
                ssim_score = float(ssim_str)
                break
            except (IndexError, ValueError) as e:
                logger.warning(f"Failed to parse SSIM from line: {line}, error: {e}")

    return ssim_score


def get_total_frames(video_path: str) -> int:
    """Get total frame count of a video file from metadata.
    
    Estimates frame count from duration and fps (fast).
    
    Args:
        video_path: Path to video file
        
    Returns:
        Estimated total number of frames
    """
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-select_streams", "v:0",
        "-show_entries", "stream=duration,r_frame_rate",
        "-of", "csv=p=0",
        video_path
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    
    if result.returncode == 0:
        try:
            lines = result.stdout.strip().split('\n')
            # Format: "fps,duration" or "fps\nduration"
            parts = result.stdout.strip().replace('\n', ',').split(',')
            
            fps_str = None
            duration = None
            
            for part in parts:
                part = part.strip()
                if '/' in part:
                    # This is fps (e.g., "30000/1001")
                    num, den = part.split('/')
                    fps_str = float(num) / float(den)
                elif part and not fps_str:
                    # Try as fps first
                    try:
                        fps_str = float(part)
                    except ValueError:
                        pass
                elif part:
                    # This is duration
                    try:
                        duration = float(part)
                    except ValueError:
                        pass
            
            if fps_str and duration:
                return int(duration * fps_str)
        except (ValueError, ZeroDivisionError) as e:
            logger.warning(f"Failed to parse video metadata: {e}")
    
    # Fallback: use format duration
    cmd_format = [
        "ffprobe",
        "-v", "quiet",
        "-show_entries", "format=duration",
        "-of", "csv=p=0",
        video_path
    ]
    result = subprocess.run(cmd_format, capture_output=True, text=True, timeout=10)
    if result.returncode == 0:
        try:
            duration = float(result.stdout.strip())
            return int(duration * 30)  # Assume 30fps
        except ValueError:
            pass
    
    return 10000  # Final fallback


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
    2. Download both videos
    3. Verify converted video is playable
    4. Calculate SSIM score (with progress updates)
    5. Extract metadata from converted video
    6. Determine pass/fail status

    Note: Metadata embedding is now done by CLI using exiftool for correct
    timezone handling in Photos app.

    Args:
        original_s3_key: S3 key for original video
        converted_s3_key: S3 key for converted video
        job_id: Quality check job ID
        metadata_s3_key: S3 key for metadata JSON (kept for CLI to retrieve)
        task_id: Async task ID for progress updates (optional)
        file_id: File ID within task for progress updates (optional)
    """
    timestamp = datetime.utcnow().isoformat() + "Z"

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
            timestamp=timestamp,
        )

    # Download files for detailed analysis
    with tempfile.TemporaryDirectory() as tmpdir:
        original_path = os.path.join(tmpdir, "original.mp4")
        converted_path = os.path.join(tmpdir, "converted.mp4")

        download_from_s3(original_s3_key, original_path)
        download_from_s3(converted_s3_key, converted_path)

        # Update verification progress: Frame extraction complete (30%)
        if task_id and file_id:
            update_verification_progress(task_id, file_id, 30)

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
                timestamp=timestamp,
            )

        # Calculate SSIM with progress updates
        ssim_score = calculate_ssim(original_path, converted_path, task_id, file_id)

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
