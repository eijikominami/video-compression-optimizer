"""Photos Access Manager for Video Compression Optimizer.

This module provides access to Apple Photos library using osxphotos for reading
and photoscript for writing operations.
"""

import subprocess
from datetime import datetime
from pathlib import Path

from vco.models.types import VideoInfo


class PhotosAccessError(Exception):
    """Exception raised for Photos library access errors."""

    pass


class PhotosAccessManager:
    """Manages access to Apple Photos library.

    Uses osxphotos for reading (scanning, metadata extraction, export)
    and photoscript for writing (import, album operations, delete).
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
        # Check if file is already local
        if video.is_local and video.path.exists():
            return video.path

        try:
            import tempfile

            from osxphotos.photoexporter import ExportOptions, PhotoExporter

            # Get the photo object
            photos = self.photosdb.photos(uuid=[video.uuid])
            if not photos:
                raise PhotosAccessError(f"Video not found: {video.uuid}")

            photo = photos[0]

            # Check if path is available (local file)
            if photo.path and Path(photo.path).exists():
                return Path(photo.path)

            # For iCloud files, use PhotoExporter with download_missing=True
            # This uses AppleScript to trigger Photos app to download from iCloud
            temp_dir = Path(tempfile.gettempdir()) / "vco_icloud_downloads"
            temp_dir.mkdir(parents=True, exist_ok=True)

            # Use PhotoExporter for better iCloud download support
            exporter = PhotoExporter(photo)
            options = ExportOptions(download_missing=True, use_photos_export=True, timeout=timeout)
            results = exporter.export(str(temp_dir), options=options)

            # Check if export was successful
            if results.exported and len(results.exported) > 0:
                exported_path = Path(results.exported[0])
                if exported_path.exists():
                    return exported_path

            # If still missing, the file couldn't be downloaded
            if results.missing:
                raise PhotosAccessError(
                    f"Video is in iCloud but could not be downloaded. "
                    f"Please download it manually in Photos app first: {video.filename}"
                )

            # Check for errors
            if results.error:
                raise PhotosAccessError(f"Export error: {results.error}")

            raise PhotosAccessError(f"Failed to download video from iCloud: {video.uuid}")

        except ImportError:
            raise PhotosAccessError("osxphotos is not installed")
        except Exception as e:
            raise PhotosAccessError(f"iCloud download failed: {e}")

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

    # ========== Write Operations (using photoscript) ==========

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

        try:
            import photoscript

            photos_app = photoscript.PhotosLibrary()

            # Import the video
            imported = photos_app.import_photos([str(video_path)])

            if not imported:
                raise PhotosAccessError(f"Failed to import video: {video_path}")

            imported_photo = imported[0]

            # Add to album if specified
            if album_name:
                self._add_to_album_by_name(imported_photo, album_name)

            return imported_photo.uuid

        except ImportError:
            raise PhotosAccessError(
                "photoscript is not installed. Install with: pip install photoscript"
            )
        except Exception as e:
            raise PhotosAccessError(f"Failed to import video: {e}")

    def delete_video(self, uuid: str) -> bool:
        """Move a video to Photos trash.

        Args:
            uuid: UUID of the video to delete (osxphotos format)

        Returns:
            True if successful

        Raises:
            PhotosAccessError: If deletion fails

        Note:
            Uses AppleScript with the UUID directly (media item id).
            The osxphotos UUID format works with AppleScript's media item id.
        """
        try:
            import subprocess

            # Use AppleScript to delete by UUID (safest method)
            # The osxphotos UUID works directly with AppleScript's media item id
            script = f'''
            tell application "Photos"
                try
                    set theItem to media item id "{uuid}"
                    delete theItem
                    return "deleted"
                on error errMsg
                    return "error: " & errMsg
                end try
            end tell
            '''

            result = subprocess.run(
                ["osascript", "-e", script], capture_output=True, text=True, timeout=30
            )

            if result.returncode != 0:
                raise PhotosAccessError(f"AppleScript error: {result.stderr}")

            output = result.stdout.strip()

            if output == "deleted":
                return True
            elif "error:" in output:
                # Check if already deleted (not found)
                if "取り出すことはできません" in output or "can't get" in output.lower():
                    # Video already deleted or not found - treat as success
                    return True
                raise PhotosAccessError(f"AppleScript error: {output}")

            return True

        except subprocess.TimeoutExpired:
            raise PhotosAccessError("Timeout while deleting video")
        except PhotosAccessError:
            raise
        except Exception as e:
            raise PhotosAccessError(f"Failed to delete video: {e}")

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

        try:
            import photoscript

            photos_app = photoscript.PhotosLibrary()

            # Find the photo by UUID
            photos = list(photos_app.photos(uuid=[uuid]))
            if not photos:
                raise PhotosAccessError(f"Video not found: {uuid}")

            photo = photos[0]

            # Add to each album
            for album_name in album_names:
                self._add_to_album_by_name(photo, album_name)

            return True

        except ImportError:
            raise PhotosAccessError(
                "photoscript is not installed. Install with: pip install photoscript"
            )
        except Exception as e:
            raise PhotosAccessError(f"Failed to add video to albums: {e}")

    def _add_to_album_by_name(self, photo, album_name: str) -> None:
        """Add a photo to an album by name, creating the album if needed.

        Args:
            photo: photoscript Photo object
            album_name: Name of the album
        """
        import photoscript

        photos_app = photoscript.PhotosLibrary()

        # Try to find existing album by iterating all albums
        album = None
        for a in photos_app.albums():
            if a.name == album_name:
                album = a
                break

        if album is None:
            # Create new album
            album = photos_app.create_album(album_name)

        # Add photo to album
        album.add([photo])

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
