"""Scan service for analyzing Photos library videos.

This service orchestrates the scanning workflow:
1. Scan Photos library for videos
2. Analyze videos for conversion candidates
3. Generate candidates.json report
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

from vco.analyzer.analyzer import CompressionAnalyzer, ConversionCandidate
from vco.models.types import VideoStatus
from vco.photos.manager import PhotosAccessManager, VideoInfo


@dataclass
class ScanFilter:
    """Filter options for scanning."""

    from_date: datetime | None = None
    to_date: datetime | None = None
    date_type: str = "capture"  # 'capture' or 'creation'


@dataclass
class ScanSummary:
    """Summary of scan results."""

    total_videos: int = 0
    conversion_candidates: int = 0
    already_optimized: int = 0
    professional: int = 0
    skipped: int = 0
    estimated_total_savings_bytes: int = 0
    estimated_total_savings_percent: float = 0.0


@dataclass
class ScanResult:
    """Complete scan result."""

    schema_version: str = "1.0"
    scan_date: str = field(default_factory=lambda: datetime.now().isoformat())
    filter: dict | None = None
    summary: ScanSummary = field(default_factory=ScanSummary)
    candidates: list[ConversionCandidate] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "schema_version": self.schema_version,
            "scan_date": self.scan_date,
            "filter": self.filter,
            "summary": asdict(self.summary),
            "candidates": [self._candidate_to_dict(c) for c in self.candidates],
        }

    def _candidate_to_dict(self, candidate: ConversionCandidate) -> dict:
        """Convert a candidate to dictionary."""
        video = candidate.video
        # Convert status enum to string value for JSON serialization
        status_str = (
            candidate.status.value if hasattr(candidate.status, "value") else str(candidate.status)
        )
        return {
            "uuid": video.uuid,
            "filename": video.filename,
            "path": str(video.path),
            "codec": video.codec,
            "resolution": list(video.resolution),
            "bitrate": video.bitrate,
            "duration": video.duration,
            "frame_rate": video.frame_rate,
            "file_size": video.file_size,
            "capture_date": video.capture_date.isoformat() if video.capture_date else None,
            "creation_date": video.creation_date.isoformat() if video.creation_date else None,
            "albums": video.albums,
            "is_in_icloud": video.is_in_icloud,
            "is_local": video.is_local,
            "location": list(video.location) if video.location else None,
            "estimated_savings_bytes": candidate.estimated_savings_bytes,
            "estimated_savings_percent": candidate.estimated_savings_percent,
            "status": status_str,
            "skip_reason": candidate.skip_reason,
        }


class ScanService:
    """Service for scanning Photos library and identifying conversion candidates."""

    def __init__(
        self,
        photos_manager: PhotosAccessManager | None = None,
        analyzer: CompressionAnalyzer | None = None,
        output_dir: Path | None = None,
    ):
        """Initialize ScanService.

        Args:
            photos_manager: PhotosAccessManager instance (created if not provided)
            analyzer: CompressionAnalyzer instance (created if not provided)
            output_dir: Directory for output files (default: ~/.config/vco)
        """
        self.photos_manager = photos_manager or PhotosAccessManager()
        self.analyzer = analyzer or CompressionAnalyzer()
        self.output_dir = output_dir or Path.home() / ".config" / "vco"

    def scan(
        self,
        from_date: datetime | None = None,
        to_date: datetime | None = None,
        date_type: str = "capture",
        quality_preset: str = "balanced",
    ) -> ScanResult:
        """Scan Photos library for conversion candidates.

        Args:
            from_date: Start date for filtering
            to_date: End date for filtering
            date_type: Date field to filter on ('capture' or 'creation')
            quality_preset: Quality preset for savings estimation

        Returns:
            ScanResult with candidates and summary
        """
        # Get videos from Photos library
        if from_date or to_date:
            videos = self.photos_manager.get_videos_by_date_range(
                from_date=from_date, to_date=to_date, date_type=date_type
            )
        else:
            videos = self.photos_manager.get_all_videos()

        # Update analyzer quality preset if different
        if quality_preset != self.analyzer.quality_preset:
            self.analyzer.quality_preset = quality_preset

        # Analyze videos
        candidates = self.analyzer.analyze(videos)

        # Build summary
        summary = self._build_summary(candidates)

        # Build filter info
        filter_info = None
        if from_date or to_date:
            filter_info = {
                "from_date": from_date.isoformat() if from_date else None,
                "to_date": to_date.isoformat() if to_date else None,
                "date_type": date_type,
            }

        # Filter to only pending candidates for output
        pending_candidates = [c for c in candidates if c.status == VideoStatus.PENDING]

        return ScanResult(
            scan_date=datetime.now().isoformat(),
            filter=filter_info,
            summary=summary,
            candidates=pending_candidates,
        )

    def save_candidates(self, result: ScanResult, output_path: Path | None = None) -> Path:
        """Save scan results to candidates.json.

        Args:
            result: ScanResult to save
            output_path: Path to save to (default: output_dir/candidates.json)

        Returns:
            Path to saved file
        """
        if output_path is None:
            output_path = self.output_dir / "candidates.json"

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(result.to_dict(), f, indent=2)

        return output_path

    def load_candidates(self, input_path: Path | None = None) -> ScanResult | None:
        """Load scan results from candidates.json.

        Args:
            input_path: Path to load from (default: output_dir/candidates.json)

        Returns:
            ScanResult if file exists, None otherwise
        """
        if input_path is None:
            input_path = self.output_dir / "candidates.json"

        if not input_path.exists():
            return None

        try:
            with open(input_path) as f:
                data = json.load(f)

            # Parse summary
            summary_data = data.get("summary", {})
            summary = ScanSummary(
                total_videos=summary_data.get("total_videos", 0),
                conversion_candidates=summary_data.get("conversion_candidates", 0),
                already_optimized=summary_data.get("already_optimized", 0),
                professional=summary_data.get("professional", 0),
                skipped=summary_data.get("skipped", 0),
                estimated_total_savings_bytes=summary_data.get("estimated_total_savings_bytes", 0),
                estimated_total_savings_percent=summary_data.get(
                    "estimated_total_savings_percent", 0.0
                ),
            )

            # Parse candidates
            candidates = []
            for c_data in data.get("candidates", []):
                video = VideoInfo(
                    uuid=c_data.get("uuid", ""),
                    filename=c_data.get("filename", ""),
                    path=Path(c_data.get("path", "")),
                    codec=c_data.get("codec", ""),
                    resolution=tuple(c_data.get("resolution", [0, 0])),
                    bitrate=c_data.get("bitrate", 0),
                    duration=c_data.get("duration", 0.0),
                    frame_rate=c_data.get("frame_rate", 0.0),
                    file_size=c_data.get("file_size", 0),
                    capture_date=datetime.fromisoformat(c_data["capture_date"])
                    if c_data.get("capture_date")
                    else None,
                    creation_date=datetime.fromisoformat(c_data["creation_date"])
                    if c_data.get("creation_date")
                    else datetime.now(),
                    albums=c_data.get("albums", []),
                    is_in_icloud=c_data.get("is_in_icloud", False),
                    is_local=c_data.get("is_local", False),
                    location=tuple(c_data["location"]) if c_data.get("location") else None,
                )

                # Convert status string to VideoStatus enum
                status_str = c_data.get("status", "pending")
                try:
                    status = VideoStatus(status_str)
                except ValueError:
                    status = VideoStatus.PENDING

                candidate = ConversionCandidate(
                    video=video,
                    estimated_savings_bytes=c_data.get("estimated_savings_bytes", 0),
                    estimated_savings_percent=c_data.get("estimated_savings_percent", 0.0),
                    status=status,
                    skip_reason=c_data.get("skip_reason"),
                )
                candidates.append(candidate)

            return ScanResult(
                schema_version=data.get("schema_version", "1.0"),
                scan_date=data.get("scan_date", ""),
                filter=data.get("filter"),
                summary=summary,
                candidates=candidates,
            )

        except Exception:
            return None

    def _build_summary(self, candidates: list[ConversionCandidate]) -> ScanSummary:
        """Build summary from candidates list.

        Args:
            candidates: List of analyzed candidates

        Returns:
            ScanSummary with statistics
        """
        total = len(candidates)
        pending = [c for c in candidates if c.status == VideoStatus.PENDING]
        optimized = [c for c in candidates if c.status == VideoStatus.OPTIMIZED]
        professional = [c for c in candidates if c.status == VideoStatus.PROFESSIONAL]
        skipped = [c for c in candidates if c.status == VideoStatus.SKIPPED]

        total_savings = sum(c.estimated_savings_bytes for c in pending)
        total_original_size = sum(c.video.file_size for c in pending)

        savings_percent = 0.0
        if total_original_size > 0:
            savings_percent = (total_savings / total_original_size) * 100

        return ScanSummary(
            total_videos=total,
            conversion_candidates=len(pending),
            already_optimized=len(optimized),
            professional=len(professional),
            skipped=len(skipped),
            estimated_total_savings_bytes=total_savings,
            estimated_total_savings_percent=round(savings_percent, 1),
        )

    def get_candidate_by_uuid(
        self, uuid: str, result: ScanResult | None = None
    ) -> ConversionCandidate | None:
        """Get a specific candidate by UUID.

        Args:
            uuid: Video UUID to find
            result: ScanResult to search (loads from file if not provided)

        Returns:
            ConversionCandidate if found, None otherwise
        """
        if result is None:
            result = self.load_candidates()

        if result is None:
            return None

        for candidate in result.candidates:
            if candidate.video.uuid == uuid:
                return candidate

        return None

    def select_top_n(
        self, candidates: list[ConversionCandidate], n: int
    ) -> list[ConversionCandidate]:
        """Select top N candidates by file size in descending order.

        This method sorts candidates by file size (largest first) and returns
        the top N candidates. Useful for maximizing storage savings by
        processing the largest files first.

        Args:
            candidates: List of conversion candidates
            n: Number of candidates to select (must be positive)

        Returns:
            List of top N candidates sorted by file size (descending)

        Raises:
            ValueError: If n is not positive
        """
        if n <= 0:
            raise ValueError("n must be a positive integer")

        # Sort by file size in descending order
        sorted_candidates = sorted(candidates, key=lambda c: c.video.file_size, reverse=True)

        # Return top N (or all if fewer than N)
        return sorted_candidates[:n]

    def calculate_top_n_summary(self, candidates: list[ConversionCandidate]) -> dict:
        """Calculate summary statistics for a list of candidates.

        Args:
            candidates: List of conversion candidates

        Returns:
            Dictionary with total_size, estimated_savings, and count
        """
        total_size = sum(c.video.file_size for c in candidates)
        estimated_savings = sum(c.estimated_savings_bytes for c in candidates)

        savings_percent = 0.0
        if total_size > 0:
            savings_percent = (estimated_savings / total_size) * 100

        return {
            "count": len(candidates),
            "total_size": total_size,
            "estimated_savings": estimated_savings,
            "estimated_savings_percent": round(savings_percent, 1),
        }
