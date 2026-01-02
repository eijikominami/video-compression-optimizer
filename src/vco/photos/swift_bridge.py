"""Swift Bridge for Photos library access.

This module provides a Python interface to the Swift vco-photos binary,
which uses native PhotoKit APIs for faster and more reliable Photos access.
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from vco.models.types import VideoInfo
from vco.photos.manager import PhotosAccessError


class SwiftBridge:
    """Bridge to Swift vco-photos binary for Photos library access.

    This class provides the same interface as PhotosAccessManager but uses
    the native Swift implementation for better performance and reliability.
    """

    # Default timeout for subprocess calls (seconds)
    DEFAULT_TIMEOUT = 60

    # Timeout for iCloud downloads (seconds)
    DOWNLOAD_TIMEOUT = 300

    def __init__(self, binary_path: Path | None = None):
        """Initialize SwiftBridge.

        Args:
            binary_path: Path to vco-photos binary (auto-detected if not provided)
        """
        self._binary_path = binary_path or self._find_binary()

    def _find_binary(self) -> Path:
        """Find the vco-photos binary.

        Searches in:
        1. Package bin/ directory
        2. Swift build directory (.build/debug or .build/release)
        3. System PATH

        Returns:
            Path to the binary

        Raises:
            PhotosAccessError: If binary not found
        """
        # Check package bin/ directory
        package_bin = Path(__file__).parent.parent.parent.parent / "bin" / "vco-photos"
        if package_bin.exists():
            return package_bin

        # Check Swift build directories
        swift_dir = Path(__file__).parent.parent.parent.parent / "swift"
        for build_type in ["debug", "release"]:
            build_path = swift_dir / ".build" / build_type / "vco-photos"
            if build_path.exists():
                return build_path

        # Check system PATH
        try:
            result = subprocess.run(
                ["which", "vco-photos"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return Path(result.stdout.strip())
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        raise PhotosAccessError("vco-photos binary not found. Build with: cd swift && swift build")

    def _execute_command(
        self,
        command: str,
        args: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """Execute a command via the Swift binary.

        Args:
            command: Command name (scan, import, delete, export, download)
            args: Command arguments
            timeout: Timeout in seconds (default: DEFAULT_TIMEOUT)

        Returns:
            Response dictionary with success, data, and error fields

        Raises:
            PhotosAccessError: If execution fails
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT

        request = {"command": command, "args": args or {}}
        request_json = json.dumps(request)

        try:
            result = subprocess.run(
                [str(self._binary_path)],
                input=request_json,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            # Parse response
            if not result.stdout.strip():
                raise PhotosAccessError(f"Empty response from vco-photos. stderr: {result.stderr}")

            try:
                response: dict[str, Any] = json.loads(result.stdout)
            except json.JSONDecodeError as e:
                raise PhotosAccessError(
                    f"Invalid JSON response from vco-photos: {e}. "
                    f"stdout: {result.stdout}, stderr: {result.stderr}"
                )

            # Check for errors
            if not response.get("success", False):
                error = response.get("error", {})
                error_type = error.get("type", "unknown")
                error_message = error.get("message", "Unknown error")
                raise PhotosAccessError(f"[{error_type}] {error_message}")

            return response

        except subprocess.TimeoutExpired:
            raise PhotosAccessError(f"Command '{command}' timed out after {timeout}s")
        except FileNotFoundError:
            raise PhotosAccessError(f"vco-photos binary not found: {self._binary_path}")

    def _parse_video_info(self, data: dict[str, Any]) -> VideoInfo:
        """Parse VideoInfo from Swift response data.

        Args:
            data: Dictionary with video info fields

        Returns:
            VideoInfo object
        """
        # Parse dates
        capture_date = None
        if data.get("capture_date"):
            try:
                capture_date = datetime.fromisoformat(data["capture_date"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

        creation_date = datetime.now()
        if data.get("creation_date"):
            try:
                creation_date = datetime.fromisoformat(data["creation_date"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

        # Parse location
        location = None
        if data.get("location") and len(data["location"]) == 2:
            location = tuple(data["location"])

        # Parse resolution
        resolution = (0, 0)
        if data.get("resolution") and len(data["resolution"]) == 2:
            resolution = tuple(data["resolution"])

        return VideoInfo(
            uuid=data.get("uuid", ""),
            filename=data.get("filename", ""),
            path=Path(data.get("path", "")) if data.get("path") else Path("/unknown"),
            codec=data.get("codec", "unknown"),
            resolution=resolution,
            bitrate=data.get("bitrate", 0),
            duration=data.get("duration", 0.0),
            frame_rate=data.get("frame_rate", 0.0),
            file_size=data.get("file_size", 0),
            capture_date=capture_date,
            creation_date=creation_date,
            albums=data.get("albums", []),
            is_in_icloud=data.get("is_in_icloud", False),
            is_local=data.get("is_local", True),
            location=location,
        )

    # ========== PhotosAccessManager Compatible Interface ==========

    def get_all_videos(self) -> list[VideoInfo]:
        """Get all videos from Photos library.

        Returns:
            List of VideoInfo objects for all videos
        """
        response = self._execute_command("scan")
        videos = []

        for video_data in response.get("data", []):
            try:
                videos.append(self._parse_video_info(video_data))
            except Exception as e:
                print(f"Warning: Failed to parse video info: {e}", file=sys.stderr)

        return videos

    def get_videos_by_date_range(
        self,
        from_date: datetime | None,
        to_date: datetime | None,
        date_type: str = "capture",
    ) -> list[VideoInfo]:
        """Get videos filtered by date range.

        Args:
            from_date: Start date (inclusive), None for no lower bound
            to_date: End date (inclusive), None for no upper bound
            date_type: "capture" for capture_date, "creation" for creation_date

        Returns:
            List of VideoInfo objects within the date range
        """
        args = {}
        if from_date:
            args["from_date"] = from_date.isoformat()
        if to_date:
            args["to_date"] = to_date.isoformat()

        response = self._execute_command("scan", args)
        videos = []

        for video_data in response.get("data", []):
            try:
                video = self._parse_video_info(video_data)

                # Apply date_type filter (Swift only filters by creation_date)
                if date_type == "capture" and (from_date or to_date):
                    video_date = video.capture_date
                    if video_date:
                        if video_date.tzinfo:
                            video_date = video_date.replace(tzinfo=None)
                        if from_date and video_date < from_date:
                            continue
                        if to_date and video_date > to_date:
                            continue

                videos.append(video)
            except Exception as e:
                print(f"Warning: Failed to parse video info: {e}", file=sys.stderr)

        return videos

    def get_video_by_uuid(self, uuid: str) -> VideoInfo | None:
        """Get a specific video by UUID.

        Args:
            uuid: Photos library UUID

        Returns:
            VideoInfo object or None if not found
        """
        # Use scan and filter by UUID
        videos = self.get_all_videos()
        for video in videos:
            if video.uuid == uuid:
                return video
        return None

    def download_from_icloud(self, video: VideoInfo, timeout: int = 300) -> Path:
        """Download video from iCloud.

        Args:
            video: VideoInfo object for the video to download
            timeout: Download timeout in seconds

        Returns:
            Path to the downloaded video

        Raises:
            PhotosAccessError: If download fails
        """
        if video.is_local and video.path.exists():
            return video.path

        response = self._execute_command(
            "download",
            {"uuid": video.uuid},
            timeout=timeout,
        )

        path_str = response.get("data", "")
        if not path_str:
            raise PhotosAccessError("Download returned empty path")

        return Path(path_str)

    def import_video(self, video_path: Path, album_name: str | None = None) -> str:
        """Import a video into Photos library.

        Args:
            video_path: Path to the video file to import
            album_name: Optional album name to add the video to

        Returns:
            UUID of the imported video

        Raises:
            PhotosAccessError: If import fails
        """
        if not video_path.exists():
            raise PhotosAccessError(f"Video file not found: {video_path}")

        args: dict[str, Any] = {"path": str(video_path)}
        if album_name:
            args["album_names"] = [album_name]

        response = self._execute_command("import", args)
        data = response.get("data", "")
        return str(data) if data else ""

    def delete_video(self, uuid: str) -> bool:
        """Move a video to Photos trash.

        Args:
            uuid: UUID of the video to delete

        Returns:
            True if successful

        Raises:
            PhotosAccessError: If deletion fails
        """
        response = self._execute_command("delete", {"uuid": uuid})
        return bool(response.get("data", False))

    def add_to_albums(self, uuid: str, album_names: list[str]) -> bool:
        """Add a video to multiple albums.

        Args:
            uuid: UUID of the video
            album_names: List of album names to add the video to

        Returns:
            True if successful

        Raises:
            PhotosAccessError: If operation fails
        """
        if not album_names:
            return True

        # Import with album names (re-import to same location adds to albums)
        video = self.get_video_by_uuid(uuid)
        if not video:
            raise PhotosAccessError(f"Video not found: {uuid}")

        # Use export + import workflow to add to albums
        # This is a workaround since Swift doesn't have direct album add
        response = self._execute_command(
            "import",
            {"path": str(video.path), "album_names": album_names},
        )
        return bool(response.get("data"))

    def export_video(self, uuid: str, destination: Path) -> Path:
        """Export a video from Photos library.

        Args:
            uuid: UUID of the video to export
            destination: Directory to export to

        Returns:
            Path to the exported video

        Raises:
            PhotosAccessError: If export fails
        """
        destination.mkdir(parents=True, exist_ok=True)

        # Get video info for filename
        video = self.get_video_by_uuid(uuid)
        if not video:
            raise PhotosAccessError(f"Video not found: {uuid}")

        dest_path = destination / video.filename

        response = self._execute_command(
            "export",
            {"uuid": uuid, "destination": str(dest_path)},
        )

        path_str = response.get("data", "")
        if not path_str:
            raise PhotosAccessError("Export returned empty path")

        return Path(path_str)

    def get_photos_app_link(self, video: VideoInfo) -> str:
        """Generate a Photos app link (deprecated).

        Args:
            video: VideoInfo object

        Returns:
            Empty string (feature removed)
        """
        return ""
