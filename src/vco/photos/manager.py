"""Photos Access Manager for Video Compression Optimizer.

This module provides read-only access to Apple Photos library using osxphotos.
For write operations (import, delete, album management), use SwiftBridge instead.
"""

import subprocess
from datetime import datetime
from pathlib import Path

from vco.models.types import VideoInfo


class PhotosAccessError(Exception):
    """Exception raised for Photos library access errors."""

    pass


class PhotosAccessManager:
    """Manages read-only access to Apple Photos library.

    Uses osxphotos for reading operations (scanning, metadata extraction, export).
    For write operations (import, delete, album management), use SwiftBridge instead.
    """

    # Video file extensions supported by Photos
    VIDEO_EXTENSIONS = {".mov", ".mp4", ".m4v", ".avi", ".mkv", ".wmv", ".mpg", ".mpeg"}

    def __init__(self, library_path: Path | None = None):
        """Initialize PhotosAccessManager.

        Args:
            library_path: Path to Photos library (None for default system library)
        """
        self._library_path = library_path
        self._photosdb = None

    @property
    def photosdb(self):
        """Lazy-load osxphotos PhotosDB."""
        if self._photosdb is None:
            try:
                import osxphotos

                if self._library_path:
                    self._photosdb = osxphotos.PhotosDB(dbfile=str(self._library_path))
                else:
                    self._photosdb = osxphotos.PhotosDB()
            except ImportError:
                raise PhotosAccessError(
                    "osxphotos is not installed. Install with: pip install osxphotos"
                )
            except Exception as e:
                raise PhotosAccessError(f"Failed to open Photos library: {e}")
        return self._photosdb

    def _extract_codec(self, photo) -> str:
        """Extract video codec from photo object.

        Args:
            photo: osxphotos PhotoInfo object

        Returns:
            Codec name (lowercase)
        """
        # Try to get codec from exiftool data
        try:
            exif = photo.exiftool
            if exif:
                # Check various codec fields
                codec = (
                    exif.get("CompressorID")
                    or exif.get("VideoCodec")
                    or exif.get("CompressorName")
                    or ""
                )
                if codec:
                    return codec.lower()
        except Exception:
            pass

        # Fallback: try to detect from file using ffprobe
        if photo.path:
            codec = self._get_codec_from_ffprobe(Path(photo.path))
            if codec:
                return codec

        return "unknown"

    def _get_codec_from_ffprobe(self, video_path: Path) -> str | None:
        """Get video codec using ffprobe.

        Args:
            video_path: Path to video file

        Returns:
            Codec name or None if detection failed
        """
        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_entries",
                    "stream=codec_name",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    str(video_path),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip().lower()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return None

    def _extract_video_info(self, photo) -> VideoInfo | None:
        """Extract VideoInfo from osxphotos PhotoInfo object.

        Args:
            photo: osxphotos PhotoInfo object

        Returns:
            VideoInfo object or None if extraction failed
        """
        try:
            # Get basic info
            path = Path(photo.path) if photo.path else None
            is_local = path is not None and path.exists()

            # Get codec from exif_info first, then try exiftool/ffprobe
            codec = "unknown"
            try:
                exif_info = photo.exif_info
                if exif_info and exif_info.codec:
                    codec = exif_info.codec.lower()
            except Exception:
                pass

            # If codec still unknown and file is local, try other methods
            if codec == "unknown" and is_local:
                codec = self._extract_codec(photo)

            # Get resolution
            width = photo.width or 0
            height = photo.height or 0

            # Get duration from exif_info (in seconds)
            duration = 0.0
            try:
                exif_info = photo.exif_info
                if exif_info and exif_info.duration:
                    duration = exif_info.duration
            except Exception:
                pass

            # Get file size - from local file or estimate from photo metadata
            if is_local and path:
                file_size = path.stat().st_size
            else:
                # For iCloud files, try to get size from photo metadata
                file_size = getattr(photo, "original_filesize", 0) or 0

            # Get dates
            capture_date = photo.date
            creation_date = photo.date_added or datetime.now()

            # Get albums (osxphotos returns album names as strings directly)
            albums = list(photo.albums) if photo.albums else []

            # Get bitrate and frame rate from exif_info first
            bitrate = 0
            frame_rate = 0.0
            try:
                exif_info = photo.exif_info
                if exif_info:
                    if exif_info.bit_rate:
                        bitrate = int(exif_info.bit_rate)
                    if exif_info.fps:
                        frame_rate = float(exif_info.fps)
            except Exception:
                pass

            # Fallback to exiftool if available and local
            if is_local and (bitrate == 0 or frame_rate == 0.0):
                try:
                    exif = photo.exiftool
                    if exif:
                        if bitrate == 0:
                            bitrate = int(exif.get("AvgBitrate", 0) or 0)
                        if frame_rate == 0.0:
                            frame_rate = float(exif.get("VideoFrameRate", 0) or 0)
                except Exception:
                    pass

            # Check iCloud status
            is_in_icloud = photo.iscloudasset

            # Get filename
            filename = photo.original_filename or (path.name if path else f"video_{photo.uuid}")

            # Get location (GPS coordinates) from Photos
            location = None
            if photo.location and photo.location != (None, None):
                location = photo.location  # Returns (latitude, longitude)

            return VideoInfo(
                uuid=photo.uuid,
                filename=filename,
                path=path or Path(f"/icloud/{photo.uuid}"),  # Placeholder path for iCloud files
                codec=codec,
                resolution=(width, height),
                bitrate=bitrate,
                duration=duration,
                frame_rate=frame_rate,
                file_size=file_size,
                capture_date=capture_date,
                creation_date=creation_date,
                albums=albums,
                is_in_icloud=is_in_icloud,
                is_local=is_local,
                location=location,
            )
        except Exception as e:
            # Log error but don't fail the entire scan
            print(f"Warning: Failed to extract info for {photo.uuid}: {e}")
            return None

    def get_all_videos(self) -> list[VideoInfo]:
        """Get all videos from Photos library.

        Returns:
            List of VideoInfo objects for all videos
        """
        videos = []

        # Get all photos that are videos
        for photo in self.photosdb.photos(movies=True, images=False):
            video_info = self._extract_video_info(photo)
            if video_info:
                videos.append(video_info)

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
        all_videos = self.get_all_videos()

        if from_date is None and to_date is None:
            return all_videos

        filtered = []
        for video in all_videos:
            # Select date based on type
            if date_type == "capture":
                video_date = video.capture_date
            else:
                video_date = video.creation_date

            # Skip if no date available
            if video_date is None:
                continue

            # Normalize timezone for comparison
            # If video_date has timezone but filter dates don't, remove timezone
            if video_date.tzinfo is not None:
                video_date_naive = video_date.replace(tzinfo=None)
            else:
                video_date_naive = video_date

            # Apply filters using naive datetime
            if from_date and video_date_naive < from_date:
                continue
            if to_date and video_date_naive > to_date:
                continue

            filtered.append(video)

        return filtered

    def get_photos_app_link(self, video: VideoInfo) -> str:
        """Generate a Photos app link to open the video directly.

        Note: This method is deprecated as the photos:// URL scheme does not
        reliably open specific photos in the Photos app on macOS.

        Args:
            video: VideoInfo object

        Returns:
            Empty string (feature removed)
        """
        return ""

    def get_video_by_uuid(self, uuid: str) -> VideoInfo | None:
        """Get a specific video by UUID.

        Args:
            uuid: Photos library UUID

        Returns:
            VideoInfo object or None if not found
        """
        photos = self.photosdb.photos(uuid=[uuid], movies=True, images=False)
        if photos:
            return self._extract_video_info(photos[0])
        return None

    # ========== Write Operations (deprecated - use SwiftBridge) ==========

    def import_video(self, video_path: Path, album_name: str | None = None) -> str:
        """Import a video into Photos library.

        DEPRECATED: Use SwiftBridge.import_video() instead.
        This method is kept for backward compatibility but should not be used.

        Args:
            video_path: Path to the video file to import
            album_name: Optional album name to add the video to

        Returns:
            UUID of the imported video

        Raises:
            PhotosAccessError: Always raises - use SwiftBridge instead
        """
        raise PhotosAccessError(
            "PhotosAccessManager.import_video() is deprecated. Use SwiftBridge.import_video() instead."
        )

    def _add_to_album_by_uuid(self, uuid: str, album_name: str) -> None:
        """Add a photo to an album by UUID using AppleScript.

        DEPRECATED: Use SwiftBridge instead.

        Args:
            uuid: Photo UUID
            album_name: Album name

        Raises:
            PhotosAccessError: Always raises - use SwiftBridge instead
        """
        raise PhotosAccessError(
            "PhotosAccessManager._add_to_album_by_uuid() is deprecated. Use SwiftBridge instead."
        )

    def delete_video(self, uuid: str) -> bool:
        """Move a video to Photos trash.

        DEPRECATED: Use SwiftBridge.delete_video() instead.

        Args:
            uuid: UUID of the video to delete (osxphotos format)

        Returns:
            Always False

        Raises:
            PhotosAccessError: Always raises - use SwiftBridge instead
        """
        raise PhotosAccessError(
            "PhotosAccessManager.delete_video() is deprecated. Use SwiftBridge.delete_video() instead."
        )

    def add_to_albums(self, uuid: str, album_names: list[str]) -> bool:
        """Add a video to multiple albums.

        DEPRECATED: Use SwiftBridge.add_to_albums() instead.

        Args:
            uuid: UUID of the video
            album_names: List of album names to add the video to

        Returns:
            Always False

        Raises:
            PhotosAccessError: Always raises - use SwiftBridge instead
        """
        raise PhotosAccessError(
            "PhotosAccessManager.add_to_albums() is deprecated. Use SwiftBridge.add_to_albums() instead."
        )

    def _add_to_album_by_name(self, photo, album_name: str) -> None:
        """Add a photo to an album by name, creating the album if needed.

        DEPRECATED: Use SwiftBridge instead.

        Args:
            photo: photoscript Photo object
            album_name: Name of the album

        Raises:
            PhotosAccessError: Always raises - use SwiftBridge instead
        """
        raise PhotosAccessError(
            "PhotosAccessManager._add_to_album_by_name() is deprecated. Use SwiftBridge instead."
        )

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
        try:
            # Get the photo
            photos = self.photosdb.photos(uuid=[uuid], movies=True, images=False)
            if not photos:
                raise PhotosAccessError(f"Video not found: {uuid}")

            photo = photos[0]

            # Ensure destination exists
            destination.mkdir(parents=True, exist_ok=True)

            # Export using osxphotos
            exported = photo.export(str(destination))

            if not exported:
                raise PhotosAccessError(f"Failed to export video: {uuid}")

            return Path(exported[0])

        except ImportError:
            raise PhotosAccessError("osxphotos is not installed")
        except Exception as e:
            raise PhotosAccessError(f"Failed to export video: {e}")
