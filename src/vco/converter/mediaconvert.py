"""AWS MediaConvert client for video conversion."""

import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


@dataclass
class QualityPreset:
    """Quality preset configuration."""

    name: str
    qvbr_quality_level: int
    qvbr_max_bitrate: int  # in bps
    description: str


# Quality presets as per design requirements
QUALITY_PRESETS = {
    "high": QualityPreset(
        name="high",
        qvbr_quality_level=9,
        qvbr_max_bitrate=50_000_000,  # 50 Mbps
        description="High quality (QVBR 8-9)",
    ),
    "balanced": QualityPreset(
        name="balanced",
        qvbr_quality_level=7,
        qvbr_max_bitrate=20_000_000,  # 20 Mbps
        description="Balanced (QVBR 6-7)",
    ),
    "compression": QualityPreset(
        name="compression",
        qvbr_quality_level=5,
        qvbr_max_bitrate=10_000_000,  # 10 Mbps
        description="High compression (QVBR 4-5)",
    ),
}

# Adaptive presets that retry with higher quality on failure
# Format: preset_name -> list of presets to try in order
ADAPTIVE_PRESETS = {
    "balanced+": ["balanced", "high"],
}


@dataclass
class ConversionJob:
    """Conversion job information."""

    job_id: str
    source_video_uuid: str
    source_s3_key: str
    output_s3_key: str
    status: str  # SUBMITTED, PROGRESSING, COMPLETE, ERROR, CANCELED
    progress_percent: int = 0
    error_message: str | None = None
    estimated_cost: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None


class MediaConvertClient:
    """AWS MediaConvert client for video conversion."""

    # MediaConvert pricing (approximate, per minute of output)
    # Basic tier, SD: $0.0075, HD: $0.015, 4K: $0.030
    PRICING_PER_MINUTE = {
        "sd": 0.0075,  # < 720p
        "hd": 0.015,  # 720p - 1080p
        "4k": 0.030,  # > 1080p
    }

    def __init__(self, region: str, s3_bucket: str, role_arn: str, profile_name: str | None = None):
        """Initialize MediaConvert client.

        Args:
            region: AWS region
            s3_bucket: S3 bucket for video files
            role_arn: IAM role ARN for MediaConvert
            profile_name: AWS profile name (optional)
        """
        self.region = region
        self.s3_bucket = s3_bucket
        self.role_arn = role_arn

        # Create boto3 session
        session_kwargs = {"region_name": region}
        if profile_name:
            session_kwargs["profile_name"] = profile_name
        self.session = boto3.Session(**session_kwargs)

        # Get MediaConvert endpoint
        self._endpoint_url = self._get_mediaconvert_endpoint()

        # Create clients
        self.mediaconvert = self.session.client("mediaconvert", endpoint_url=self._endpoint_url)
        self.s3 = self.session.client("s3")

    def _get_mediaconvert_endpoint(self) -> str:
        """Get the MediaConvert endpoint URL for the region."""
        client = self.session.client("mediaconvert", region_name=self.region)
        response = client.describe_endpoints()
        return response["Endpoints"][0]["Url"]

    def upload_to_s3(self, local_path: Path, s3_key: str) -> str:
        """Upload a file to S3.

        Args:
            local_path: Local file path
            s3_key: S3 object key

        Returns:
            S3 URI (s3://bucket/key)
        """
        self.s3.upload_file(str(local_path), self.s3_bucket, s3_key)
        return f"s3://{self.s3_bucket}/{s3_key}"

    def download_from_s3(self, s3_key: str, local_path: Path) -> Path:
        """Download a file from S3.

        Args:
            s3_key: S3 object key
            local_path: Local file path to save to

        Returns:
            Local file path
        """
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(self.s3_bucket, s3_key, str(local_path))
        return local_path

    def delete_from_s3(self, s3_key: str) -> bool:
        """Delete a file from S3.

        Args:
            s3_key: S3 object key

        Returns:
            True if successful
        """
        try:
            self.s3.delete_object(Bucket=self.s3_bucket, Key=s3_key)
            return True
        except ClientError:
            return False

    def submit_job(
        self,
        source_video_uuid: str,
        source_s3_key: str,
        output_s3_key: str,
        quality_preset: str = "balanced",
    ) -> ConversionJob:
        """Submit a video conversion job to MediaConvert.

        Args:
            source_video_uuid: UUID of the source video
            source_s3_key: S3 key of the source video
            output_s3_key: S3 key for the output video
            quality_preset: Quality preset name (high, balanced, compression)

        Returns:
            ConversionJob with job details
        """
        preset = QUALITY_PRESETS.get(quality_preset, QUALITY_PRESETS["balanced"])

        # Build MediaConvert job settings
        job_settings = self._build_job_settings(
            source_s3_key=source_s3_key, output_s3_key=output_s3_key, preset=preset
        )

        # Submit job
        response = self.mediaconvert.create_job(
            Role=self.role_arn,
            Settings=job_settings,
            UserMetadata={"source_video_uuid": source_video_uuid, "quality_preset": quality_preset},
        )

        job_id = response["Job"]["Id"]

        return ConversionJob(
            job_id=job_id,
            source_video_uuid=source_video_uuid,
            source_s3_key=source_s3_key,
            output_s3_key=output_s3_key,
            status="SUBMITTED",
            progress_percent=0,
        )

    def _build_job_settings(
        self, source_s3_key: str, output_s3_key: str, preset: QualityPreset
    ) -> dict:
        """Build MediaConvert job settings.

        Args:
            source_s3_key: S3 key of the source video
            output_s3_key: S3 key for the output video
            preset: Quality preset

        Returns:
            MediaConvert job settings dictionary
        """
        # Extract output directory and filename
        output_path = Path(output_s3_key)
        output_dir = str(output_path.parent)

        # NameModifier is appended to the input filename by MediaConvert
        # MediaConvert output: {input_filename}{NameModifier}.{extension}
        # We use '_h265' as the modifier to indicate H.265 encoding
        name_modifier = "_h265"

        return {
            "Inputs": [
                {
                    "FileInput": f"s3://{self.s3_bucket}/{source_s3_key}",
                    "AudioSelectors": {"Audio Selector 1": {"DefaultSelection": "DEFAULT"}},
                    "VideoSelector": {},
                    "TimecodeSource": "ZEROBASED",
                }
            ],
            "OutputGroups": [
                {
                    "Name": "File Group",
                    "OutputGroupSettings": {
                        "Type": "FILE_GROUP_SETTINGS",
                        "FileGroupSettings": {
                            "Destination": f"s3://{self.s3_bucket}/{output_dir}/"
                        },
                    },
                    "Outputs": [
                        {
                            "NameModifier": name_modifier,
                            "ContainerSettings": {
                                "Container": "MP4",
                                "Mp4Settings": {
                                    "CslgAtom": "INCLUDE",
                                    "FreeSpaceBox": "EXCLUDE",
                                    "MoovPlacement": "PROGRESSIVE_DOWNLOAD",
                                },
                            },
                            "VideoDescription": {
                                "CodecSettings": {
                                    "Codec": "H_265",
                                    "H265Settings": {
                                        "RateControlMode": "QVBR",
                                        "QvbrSettings": {
                                            "QvbrQualityLevel": preset.qvbr_quality_level,
                                            "QvbrQualityLevelFineTune": 0,
                                        },
                                        "MaxBitrate": preset.qvbr_max_bitrate,
                                        "GopSize": 90,
                                        "GopSizeUnits": "FRAMES",
                                        "ParNumerator": 1,
                                        "ParDenominator": 1,
                                        "ParControl": "SPECIFIED",
                                        "NumberBFramesBetweenReferenceFrames": 3,
                                        "NumberReferenceFrames": 3,
                                        "Slices": 1,
                                        "InterlaceMode": "PROGRESSIVE",
                                        "SceneChangeDetect": "ENABLED",
                                        "MinIInterval": 0,
                                        "AdaptiveQuantization": "HIGH",
                                        "FlickerAdaptiveQuantization": "ENABLED",
                                        "SpatialAdaptiveQuantization": "ENABLED",
                                        "TemporalAdaptiveQuantization": "ENABLED",
                                        "UnregisteredSeiTimecode": "DISABLED",
                                        "SampleAdaptiveOffsetFilterMode": "ADAPTIVE",
                                        "WriteMp4PackagingType": "HVC1",
                                        "AlternateTransferFunctionSei": "DISABLED",
                                    },
                                },
                                "ScalingBehavior": "DEFAULT",
                                "TimecodeInsertion": "DISABLED",
                                "AntiAlias": "ENABLED",
                                "Sharpness": 50,
                                "AfdSignaling": "NONE",
                                "DropFrameTimecode": "ENABLED",
                                "RespondToAfd": "NONE",
                                "ColorMetadata": "INSERT",
                            },
                            "AudioDescriptions": [
                                {
                                    "CodecSettings": {
                                        "Codec": "AAC",
                                        "AacSettings": {
                                            "Bitrate": 128000,
                                            "CodingMode": "CODING_MODE_2_0",
                                            "SampleRate": 48000,
                                            "RateControlMode": "CBR",
                                            "RawFormat": "NONE",
                                            "Specification": "MPEG4",
                                            "AudioDescriptionBroadcasterMix": "NORMAL",
                                        },
                                    },
                                    "AudioSourceName": "Audio Selector 1",
                                }
                            ],
                            "Extension": "mp4",
                        }
                    ],
                }
            ],
            "TimecodeConfig": {"Source": "ZEROBASED"},
        }

    def get_job_status(self, job_id: str) -> ConversionJob:
        """Get the status of a conversion job.

        Args:
            job_id: MediaConvert job ID

        Returns:
            ConversionJob with current status
        """
        response = self.mediaconvert.get_job(Id=job_id)
        job = response["Job"]

        status = job["Status"]
        progress = job.get("JobPercentComplete", 0)
        error_message = None
        completed_at = None

        if status == "ERROR":
            error_message = job.get("ErrorMessage", "Unknown error")

        if status in ("COMPLETE", "ERROR", "CANCELED"):
            # Parse completion time if available
            if "Timing" in job and "FinishTime" in job["Timing"]:
                completed_at = job["Timing"]["FinishTime"]

        # Get metadata
        metadata = job.get("UserMetadata", {})
        source_video_uuid = metadata.get("source_video_uuid", "")

        # Get input/output keys from job settings
        source_s3_key = ""
        output_s3_key = ""

        if "Settings" in job:
            settings = job["Settings"]
            if "Inputs" in settings and settings["Inputs"]:
                file_input = settings["Inputs"][0].get("FileInput", "")
                if file_input.startswith(f"s3://{self.s3_bucket}/"):
                    source_s3_key = file_input[len(f"s3://{self.s3_bucket}/") :]

            if "OutputGroups" in settings and settings["OutputGroups"]:
                output_group = settings["OutputGroups"][0]
                if "OutputGroupSettings" in output_group:
                    dest = (
                        output_group["OutputGroupSettings"]
                        .get("FileGroupSettings", {})
                        .get("Destination", "")
                    )
                    if dest.startswith(f"s3://{self.s3_bucket}/"):
                        output_dir = dest[len(f"s3://{self.s3_bucket}/") :]
                        # Construct output key (MediaConvert adds the filename)
                        output_s3_key = output_dir.rstrip("/")

        return ConversionJob(
            job_id=job_id,
            source_video_uuid=source_video_uuid,
            source_s3_key=source_s3_key,
            output_s3_key=output_s3_key,
            status=status,
            progress_percent=progress,
            error_message=error_message,
            completed_at=completed_at,
        )

    def wait_for_completion(
        self, job_id: str, poll_interval: int = 10, timeout: int = 3600
    ) -> ConversionJob:
        """Wait for a job to complete.

        Args:
            job_id: MediaConvert job ID
            poll_interval: Seconds between status checks
            timeout: Maximum seconds to wait

        Returns:
            ConversionJob with final status

        Raises:
            TimeoutError: If job doesn't complete within timeout
        """
        start_time = time.time()

        while True:
            job = self.get_job_status(job_id)

            if job.status in ("COMPLETE", "ERROR", "CANCELED"):
                return job

            elapsed = time.time() - start_time
            if elapsed > timeout:
                raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")

            time.sleep(poll_interval)

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a conversion job.

        Args:
            job_id: MediaConvert job ID

        Returns:
            True if cancellation was successful
        """
        try:
            self.mediaconvert.cancel_job(Id=job_id)
            return True
        except ClientError:
            return False

    def estimate_cost(self, duration_seconds: float, resolution: tuple[int, int]) -> float:
        """Estimate the cost of converting a video.

        Args:
            duration_seconds: Video duration in seconds
            resolution: Video resolution (width, height)

        Returns:
            Estimated cost in USD
        """
        width, height = resolution
        pixels = width * height

        # Determine pricing tier based on resolution
        if pixels <= 921600:  # 1280x720
            tier = "sd"
        elif pixels <= 2073600:  # 1920x1080
            tier = "hd"
        else:
            tier = "4k"

        duration_minutes = duration_seconds / 60
        cost = duration_minutes * self.PRICING_PER_MINUTE[tier]

        return round(cost, 4)

    def list_jobs(self, status: str | None = None, max_results: int = 20) -> list[ConversionJob]:
        """List recent conversion jobs.

        Args:
            status: Filter by status (SUBMITTED, PROGRESSING, COMPLETE, ERROR, CANCELED)
            max_results: Maximum number of results

        Returns:
            List of ConversionJob objects
        """
        kwargs = {"MaxResults": max_results, "Order": "DESCENDING"}
        if status:
            kwargs["Status"] = status

        response = self.mediaconvert.list_jobs(**kwargs)

        jobs = []
        for job_data in response.get("Jobs", []):
            job_id = job_data["Id"]
            jobs.append(self.get_job_status(job_id))

        return jobs


def get_quality_preset(name: str) -> QualityPreset:
    """Get a quality preset by name.

    Args:
        name: Preset name (high, balanced, compression)

    Returns:
        QualityPreset object

    Raises:
        ValueError: If preset name is invalid
    """
    if name not in QUALITY_PRESETS:
        valid = ", ".join(QUALITY_PRESETS.keys())
        raise ValueError(f"Invalid preset name: {name}. Valid options: {valid}")
    return QUALITY_PRESETS[name]


def is_adaptive_preset(name: str) -> bool:
    """Check if a preset name is an adaptive preset.

    Args:
        name: Preset name

    Returns:
        True if the preset is adaptive (e.g., 'balanced+')
    """
    return name in ADAPTIVE_PRESETS


def get_adaptive_preset_chain(name: str) -> list[str]:
    """Get the chain of presets for an adaptive preset.

    Args:
        name: Adaptive preset name (e.g., 'balanced+')

    Returns:
        List of preset names to try in order

    Raises:
        ValueError: If preset name is not an adaptive preset
    """
    if name not in ADAPTIVE_PRESETS:
        raise ValueError(f"{name} is not an adaptive preset")
    return ADAPTIVE_PRESETS[name]
