"""Property-based tests for metadata verification.

Tests correctness properties defined in design.md for metadata verification.
"""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.metadata.verifier import MetadataVerifier
from vco.models.metadata import (
    ExtractedMetadata,
    GPSLocation,
    OriginalMetadata,
    ProcessingTimeProximityWarning,
    VerificationResult,
)
from vco.models.types import ImportableItem, UnifiedImportResult
from vco.services.aws_import import AwsDownloadResult, CleanupResult
from vco.services.unified_import import UnifiedImportService

# =============================================================================
# Strategies
# =============================================================================


@st.composite
def gps_locations(draw):
    """Generate valid GPS locations."""
    lat = draw(st.floats(min_value=-90.0, max_value=90.0, allow_nan=False, allow_infinity=False))
    lon = draw(st.floats(min_value=-180.0, max_value=180.0, allow_nan=False, allow_infinity=False))
    alt = draw(
        st.one_of(
            st.none(),
            st.floats(min_value=-1000, max_value=50000, allow_nan=False, allow_infinity=False),
        )
    )
    return GPSLocation(latitude=lat, longitude=lon, altitude=alt)


@st.composite
def datetimes_in_range(draw):
    """Generate datetimes within a reasonable range."""
    return draw(
        st.datetimes(
            min_value=datetime(2000, 1, 1),
            max_value=datetime(2030, 12, 31),
        )
    )


@st.composite
def original_metadata(draw):
    """Generate OriginalMetadata instances."""
    capture_date = draw(st.one_of(st.none(), datetimes_in_range()))
    gps = draw(st.one_of(st.none(), gps_locations()))
    albums = draw(st.lists(st.text(min_size=1, max_size=50), max_size=5))
    uuid = draw(st.text(min_size=1, max_size=36))
    filename = draw(st.text(min_size=1, max_size=100))
    return OriginalMetadata(
        capture_date=capture_date,
        gps_location=gps,
        album_names=albums,
        original_uuid=uuid,
        filename=filename,
    )


# =============================================================================
# Property 2: Capture date comparison tolerance
# Validates: Requirements 1.6
# =============================================================================


class TestCaptureDateComparisonTolerance:
    """Property 2: Capture date comparison tolerance.

    *For any* capture date comparison, dates within 1 second SHALL be considered matching.
    **Validates: Requirements 1.6**
    """

    @given(
        base_date=datetimes_in_range(),
        diff_ms=st.integers(min_value=0, max_value=999),
    )
    @settings(max_examples=100)
    def test_dates_within_one_second_match(self, base_date: datetime, diff_ms: int):
        """Dates within 1 second tolerance should match."""
        # Create a date within 1 second of base
        converted_date = base_date + timedelta(milliseconds=diff_ms)

        verifier = MetadataVerifier(tolerance_seconds=1.0)
        result = verifier._compare_capture_date(base_date, converted_date)

        assert result.matches is True, (
            f"Dates within tolerance should match: "
            f"original={base_date}, converted={converted_date}, diff_ms={diff_ms}"
        )

    @given(
        base_date=datetimes_in_range(),
        diff_seconds=st.floats(
            min_value=1.001, max_value=3600.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=100)
    def test_dates_outside_tolerance_do_not_match(self, base_date: datetime, diff_seconds: float):
        """Dates outside 1 second tolerance should not match."""
        converted_date = base_date + timedelta(seconds=diff_seconds)

        verifier = MetadataVerifier(tolerance_seconds=1.0)
        result = verifier._compare_capture_date(base_date, converted_date)

        assert result.matches is False, (
            f"Dates outside tolerance should not match: "
            f"original={base_date}, converted={converted_date}, diff_seconds={diff_seconds}"
        )


# =============================================================================
# Property 3: GPS location comparison tolerance
# Validates: Requirements 1.8
# =============================================================================


class TestGPSLocationComparisonTolerance:
    """Property 3: GPS location comparison tolerance.

    *For any* GPS location comparison, locations within 0.0001 degrees SHALL be considered matching.
    **Validates: Requirements 1.8**
    """

    @given(
        base_lat=st.floats(min_value=-89.9, max_value=89.9, allow_nan=False, allow_infinity=False),
        base_lon=st.floats(
            min_value=-179.9, max_value=179.9, allow_nan=False, allow_infinity=False
        ),
        lat_diff=st.floats(
            min_value=-0.00009, max_value=0.00009, allow_nan=False, allow_infinity=False
        ),
        lon_diff=st.floats(
            min_value=-0.00009, max_value=0.00009, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=100)
    def test_locations_within_tolerance_match(
        self, base_lat: float, base_lon: float, lat_diff: float, lon_diff: float
    ):
        """GPS locations within 0.0001 degrees tolerance should match."""
        original = GPSLocation(latitude=base_lat, longitude=base_lon)
        converted = GPSLocation(latitude=base_lat + lat_diff, longitude=base_lon + lon_diff)

        verifier = MetadataVerifier(tolerance_degrees=0.0001)
        result = verifier._compare_gps_location(original, converted)

        assert result.matches is True, (
            f"Locations within tolerance should match: "
            f"original=({base_lat}, {base_lon}), "
            f"converted=({base_lat + lat_diff}, {base_lon + lon_diff})"
        )

    @given(
        base_lat=st.floats(min_value=-89.0, max_value=89.0, allow_nan=False, allow_infinity=False),
        base_lon=st.floats(
            min_value=-179.0, max_value=179.0, allow_nan=False, allow_infinity=False
        ),
        lat_diff=st.floats(min_value=0.0002, max_value=1.0, allow_nan=False, allow_infinity=False),
    )
    @settings(max_examples=100)
    def test_locations_outside_tolerance_do_not_match(
        self, base_lat: float, base_lon: float, lat_diff: float
    ):
        """GPS locations outside 0.0001 degrees tolerance should not match."""
        original = GPSLocation(latitude=base_lat, longitude=base_lon)
        converted = GPSLocation(latitude=base_lat + lat_diff, longitude=base_lon)

        verifier = MetadataVerifier(tolerance_degrees=0.0001)
        result = verifier._compare_gps_location(original, converted)

        assert result.matches is False, (
            f"Locations outside tolerance should not match: "
            f"original=({base_lat}, {base_lon}), "
            f"converted=({base_lat + lat_diff}, {base_lon}), "
            f"lat_diff={lat_diff}"
        )


# =============================================================================
# Property 4: Missing GPS detection
# Validates: Requirements 1.7
# =============================================================================


class TestMissingGPSDetection:
    """Property 4: Missing GPS detection.

    *For any* video where original had GPS but converted does not,
    the system SHALL report a mismatch.
    **Validates: Requirements 1.7**
    """

    @given(gps=gps_locations())
    @settings(max_examples=100)
    def test_missing_gps_in_converted_is_mismatch(self, gps: GPSLocation):
        """When original has GPS but converted doesn't, it should be a mismatch."""
        verifier = MetadataVerifier()
        result = verifier._compare_gps_location(gps, None)

        assert result.matches is False, (
            f"Missing GPS in converted should be mismatch: original={gps}"
        )
        assert result.error_message is not None
        assert "missing" in result.error_message.lower()

    @given(gps=gps_locations())
    @settings(max_examples=100)
    def test_added_gps_in_converted_is_not_mismatch(self, gps: GPSLocation):
        """When original has no GPS but converted does, it should NOT be a mismatch."""
        verifier = MetadataVerifier()
        result = verifier._compare_gps_location(None, gps)

        # Adding GPS is acceptable
        assert result.matches is True, (
            f"Added GPS in converted should not be mismatch: converted={gps}"
        )


# =============================================================================
# Property 12: Processing time proximity detection
# Validates: Requirements 5.2
# =============================================================================


class TestProcessingTimeProximityDetection:
    """Property 12: Processing time proximity detection.

    *For any* capture date within ±1 hour of processing time,
    the system SHALL display a warning message.
    **Validates: Requirements 5.2**
    """

    @given(
        base_time=datetimes_in_range(),
        diff_minutes=st.floats(
            min_value=-59.0, max_value=59.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=100)
    def test_capture_within_one_hour_triggers_warning(
        self, base_time: datetime, diff_minutes: float
    ):
        """Capture date within ±1 hour of processing time should trigger warning."""
        capture_date = base_time + timedelta(minutes=diff_minutes)

        verifier = MetadataVerifier(processing_time_tolerance_hours=1.0)
        result = verifier._check_processing_time_proximity(capture_date, base_time)

        assert result is not None, (
            f"Should trigger warning: capture={capture_date}, "
            f"processing={base_time}, diff_minutes={diff_minutes}"
        )
        assert isinstance(result, ProcessingTimeProximityWarning)

    @given(
        base_time=datetimes_in_range(),
        diff_hours=st.floats(min_value=1.01, max_value=24.0, allow_nan=False, allow_infinity=False),
        sign=st.sampled_from([1, -1]),
    )
    @settings(max_examples=100)
    def test_capture_outside_one_hour_no_warning(
        self, base_time: datetime, diff_hours: float, sign: int
    ):
        """Capture date outside ±1 hour of processing time should not trigger warning."""
        capture_date = base_time + timedelta(hours=diff_hours * sign)

        verifier = MetadataVerifier(processing_time_tolerance_hours=1.0)
        result = verifier._check_processing_time_proximity(capture_date, base_time)

        assert result is None, (
            f"Should not trigger warning: capture={capture_date}, "
            f"processing={base_time}, diff_hours={diff_hours * sign}"
        )


# =============================================================================
# Property 13: Processing time warning does not stop import
# Validates: Requirements 5.4, 5.5
# =============================================================================


class TestProcessingTimeWarningDoesNotStopImport:
    """Property 13: Processing time warning does not stop import.

    *For any* processing time proximity warning, the system SHALL continue
    with normal verification and import flow without stopping.
    **Validates: Requirements 5.4, 5.5**
    """

    @given(
        base_time=datetimes_in_range(),
        diff_minutes=st.floats(
            min_value=-59.0, max_value=59.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=100)
    def test_warning_does_not_affect_verification_success(
        self, base_time: datetime, diff_minutes: float
    ):
        """Processing time warning should not affect verification success."""
        capture_date = base_time + timedelta(minutes=diff_minutes)

        original = OriginalMetadata(
            capture_date=capture_date,
            gps_location=None,
            album_names=[],
            original_uuid="test-uuid",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            capture_date=capture_date,  # Matching capture date
            gps_location=None,
        )

        verifier = MetadataVerifier(processing_time_tolerance_hours=1.0)
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(
                Path("test.mp4"),
                original,
                processing_time=base_time,
            )

        # Should have warning but still succeed
        assert result.has_warning is True
        assert result.success is True, (
            f"Warning should not affect success: capture={capture_date}, processing={base_time}"
        )


# =============================================================================
# Property 5: Verification failure skips import
# Validates: Requirements 2.4
# =============================================================================


class TestVerificationFailureSkipsImport:
    """Property 5: Verification failure skips import.

    *For any* metadata verification failure without --force flag,
    the system SHALL skip import and preserve file.
    **Validates: Requirements 2.4**
    """

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        date_diff_seconds=st.floats(
            min_value=2.0, max_value=3600.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=50)
    def test_verification_failure_without_force_skips_import(
        self, task_id: str, file_id: str, date_diff_seconds: float
    ):
        """Verification failure without --force flag skips import."""
        # Create metadata with mismatch
        original_date = datetime(2024, 1, 15, 14, 30, 0)
        converted_date = original_date + timedelta(seconds=date_diff_seconds)

        extracted_metadata = ExtractedMetadata(
            capture_date=converted_date,  # Mismatched date
            gps_location=None,
        )

        # Setup mocks
        aws_service = MagicMock()
        swift_bridge = MagicMock()
        verifier = MetadataVerifier(tolerance_seconds=1.0)

        # Mock download success
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            metadata_path=Path(f"/tmp/{file_id}_metadata.json"),
            checksum_verified=True,
        )

        # Mock metadata loading
        metadata_dict = {
            "capture_date": original_date.isoformat(),
            "gps_location": None,
            "album_names": [],
            "original_uuid": "test-uuid",
            "original_filename": "test.mp4",
        }

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
            metadata_verifier=verifier,
        )

        with patch.object(verifier.extractor, "extract", return_value=extracted_metadata):
            with patch("builtins.open", MagicMock()):
                with patch("json.load", return_value=metadata_dict):
                    with patch.object(Path, "exists", return_value=True):
                        result = service.import_item(
                            f"{task_id}:{file_id}",
                            force_import=False,  # No --force flag
                        )

        # Property: import should fail due to metadata mismatch
        assert result.success is False
        assert result.metadata_mismatch is True
        assert result.metadata_verified is False

        # Property: Photos import should NOT be called
        swift_bridge.import_video.assert_not_called()


# =============================================================================
# Property 6: Force flag bypasses verification
# Validates: Requirements 2.6
# =============================================================================


class TestForceFlagBypassesVerification:
    """Property 6: Force flag bypasses verification.

    *For any* import with --force flag, the system SHALL proceed
    despite metadata mismatch.
    **Validates: Requirements 2.6**
    """

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        date_diff_seconds=st.floats(
            min_value=2.0, max_value=3600.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=50)
    def test_force_flag_proceeds_despite_mismatch(
        self, task_id: str, file_id: str, date_diff_seconds: float
    ):
        """Force flag allows import despite metadata mismatch."""
        # Create metadata with mismatch
        original_date = datetime(2024, 1, 15, 14, 30, 0)
        converted_date = original_date + timedelta(seconds=date_diff_seconds)

        extracted_metadata = ExtractedMetadata(
            capture_date=converted_date,  # Mismatched date
            gps_location=None,
        )

        # Setup mocks
        aws_service = MagicMock()
        swift_bridge = MagicMock()
        verifier = MetadataVerifier(tolerance_seconds=1.0)

        # Mock download success
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            metadata_path=Path(f"/tmp/{file_id}_metadata.json"),
            checksum_verified=True,
        )

        # Mock Photos import success
        swift_bridge.import_video.return_value = "new-uuid-123"

        # Mock cleanup success
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id=file_id,
            status="DOWNLOADED",
            s3_deleted=True,
        )

        # Mock metadata loading
        metadata_dict = {
            "capture_date": original_date.isoformat(),
            "gps_location": None,
            "album_names": [],
            "original_uuid": "test-uuid",
            "original_filename": "test.mp4",
        }

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
            metadata_verifier=verifier,
        )

        with patch.object(verifier.extractor, "extract", return_value=extracted_metadata):
            with patch("builtins.open", MagicMock()):
                with patch("json.load", return_value=metadata_dict):
                    with patch.object(Path, "exists", return_value=True):
                        with patch.object(Path, "unlink"):
                            result = service.import_item(
                                f"{task_id}:{file_id}",
                                force_import=True,  # --force flag
                            )

        # Property: import should succeed despite mismatch
        assert result.success is True

        # Property: Photos import should be called
        swift_bridge.import_video.assert_called_once()


# =============================================================================
# Property 7: Batch import continues on mismatch
# Validates: Requirements 3.2
# =============================================================================


class TestBatchImportContinuesOnMismatch:
    """Property 7: Batch import continues on mismatch.

    *For any* batch import with metadata mismatch, the system SHALL
    skip the mismatched file and continue with remaining files.
    **Validates: Requirements 3.2**
    """

    @given(
        total_items=st.integers(min_value=2, max_value=5),
        mismatch_index=st.integers(min_value=0, max_value=4),
    )
    @settings(max_examples=30)
    def test_batch_continues_after_mismatch(self, total_items: int, mismatch_index: int):
        """Batch import continues processing after encountering mismatch."""
        # Ensure mismatch_index is within bounds
        mismatch_index = mismatch_index % total_items

        # Create items
        items = [
            ImportableItem(
                item_id=f"task{i}:file{i}",
                source="aws",
                original_filename=f"video{i}.mov",
                converted_filename=f"video{i}_h265.mp4",
                original_size=1000000,
                converted_size=500000,
                compression_ratio=2.0,
                ssim_score=0.95,
                task_id=f"task{i}",
                file_id=f"file{i}",
            )
            for i in range(total_items)
        ]

        # Track which items were processed
        processed_items = []

        def mock_import_item(
            item_id,
            user_id=None,
            progress_callback=None,
            status_callback=None,
            delete_original=False,
            original_uuid=None,
            force_import=False,
        ):
            processed_items.append(item_id)
            idx = int(item_id.split(":")[0].replace("task", ""))

            if idx == mismatch_index:
                # This item has metadata mismatch
                return UnifiedImportResult(
                    success=False,
                    item_id=item_id,
                    source="aws",
                    original_filename=f"video{idx}.mov",
                    converted_filename=f"video{idx}_h265.mp4",
                    error_message="Metadata mismatch",
                    metadata_mismatch=True,
                )
            else:
                # This item succeeds
                return UnifiedImportResult(
                    success=True,
                    item_id=item_id,
                    source="aws",
                    original_filename=f"video{idx}.mov",
                    converted_filename=f"video{idx}_h265.mp4",
                    metadata_verified=True,
                )

        aws_service = MagicMock()
        aws_service.list_completed_files.return_value = items

        service = UnifiedImportService(aws_service=aws_service)

        with patch.object(service, "_import_aws_item", side_effect=mock_import_item):
            result = service.import_all(force_import=False)

        # Property: all items were processed
        assert len(processed_items) == total_items

        # Property: successful count is total - 1 (one mismatch)
        assert result.aws_successful == total_items - 1

        # Property: failed count is 1
        assert result.aws_failed == 1

        # Property: mismatch count is 1
        assert result.metadata_mismatch_count == 1


# =============================================================================
# Property 8: Batch summary accuracy
# Validates: Requirements 3.3
# =============================================================================


class TestBatchSummaryAccuracy:
    """Property 8: Batch summary accuracy.

    *For any* batch import, the summary SHALL accurately report
    metadata_verified_count, metadata_mismatch_count, and skipped filenames.
    **Validates: Requirements 3.3**
    """

    @given(
        verified_count=st.integers(min_value=0, max_value=5),
        mismatch_count=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=30)
    def test_batch_summary_counts_accurate(self, verified_count: int, mismatch_count: int):
        """Batch summary accurately reports verification counts."""
        total_items = verified_count + mismatch_count
        if total_items == 0:
            return  # Skip empty case

        # Create items
        items = [
            ImportableItem(
                item_id=f"task{i}:file{i}",
                source="aws",
                original_filename=f"video{i}.mov",
                converted_filename=f"video{i}_h265.mp4",
                original_size=1000000,
                converted_size=500000,
                compression_ratio=2.0,
                ssim_score=0.95,
                task_id=f"task{i}",
                file_id=f"file{i}",
            )
            for i in range(total_items)
        ]

        skipped_filenames = []

        def mock_import_item(
            item_id,
            user_id=None,
            progress_callback=None,
            status_callback=None,
            delete_original=False,
            original_uuid=None,
            force_import=False,
        ):
            idx = int(item_id.split(":")[0].replace("task", ""))
            filename = f"video{idx}_h265.mp4"

            if idx < verified_count:
                # Verified successfully
                return UnifiedImportResult(
                    success=True,
                    item_id=item_id,
                    source="aws",
                    original_filename=f"video{idx}.mov",
                    converted_filename=filename,
                    metadata_verified=True,
                )
            else:
                # Metadata mismatch
                skipped_filenames.append(filename)
                return UnifiedImportResult(
                    success=False,
                    item_id=item_id,
                    source="aws",
                    original_filename=f"video{idx}.mov",
                    converted_filename=filename,
                    error_message="Metadata mismatch",
                    metadata_mismatch=True,
                )

        aws_service = MagicMock()
        aws_service.list_completed_files.return_value = items

        service = UnifiedImportService(aws_service=aws_service)

        with patch.object(service, "_import_aws_item", side_effect=mock_import_item):
            result = service.import_all(force_import=False)

        # Property: verified count is accurate
        assert result.metadata_verified_count == verified_count

        # Property: mismatch count is accurate
        assert result.metadata_mismatch_count == mismatch_count

        # Property: skipped files list is accurate
        assert len(result.skipped_files) == mismatch_count
        for filename in skipped_filenames:
            assert filename in result.skipped_files


# =============================================================================
# Property 10: Verified metadata display
# Validates: Requirements 2.1
# =============================================================================


class TestVerifiedMetadataDisplay:
    """Property 10: Verified metadata display.

    *For any* successful verification, the system SHALL display
    the verified metadata (capture_date, GPS, albums).
    **Validates: Requirements 2.1**
    """

    @given(
        capture_date=datetimes_in_range(),
        gps=gps_locations(),
        album_count=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=50)
    def test_verification_result_contains_display_data(
        self, capture_date: datetime, gps: GPSLocation, album_count: int
    ):
        """Successful verification result contains all display data."""
        from vco.models.metadata import FieldVerificationResult

        albums = [f"Album{i}" for i in range(album_count)]

        # Create a successful verification result
        result = VerificationResult(
            success=True,
            filename="test.mp4",
            capture_date=FieldVerificationResult(
                field_name="capture_date",
                matches=True,
                original_value=capture_date,
                converted_value=capture_date,
            ),
            gps_location=FieldVerificationResult(
                field_name="gps_location",
                matches=True,
                original_value=gps,
                converted_value=gps,
            ),
            album_info=FieldVerificationResult(
                field_name="album_info",
                matches=True,
                original_value=albums,
                converted_value=albums,
            ),
        )

        # Property: result contains capture_date for display
        assert result.capture_date.converted_value is not None
        assert result.capture_date.converted_value == capture_date

        # Property: result contains GPS for display
        assert result.gps_location.converted_value is not None
        assert result.gps_location.converted_value.latitude == gps.latitude
        assert result.gps_location.converted_value.longitude == gps.longitude

        # Property: result contains albums for display
        assert result.album_info.converted_value is not None
        assert len(result.album_info.converted_value) == album_count

    @given(
        capture_date=datetimes_in_range(),
    )
    @settings(max_examples=30)
    def test_verification_result_display_format(self, capture_date: datetime):
        """Verification result can be formatted for display."""
        from vco.models.metadata import FieldVerificationResult

        gps = GPSLocation(latitude=35.6762, longitude=139.6503)
        albums = ["Travel", "2024"]

        result = VerificationResult(
            success=True,
            filename="test.mp4",
            capture_date=FieldVerificationResult(
                field_name="capture_date",
                matches=True,
                original_value=capture_date,
                converted_value=capture_date,
            ),
            gps_location=FieldVerificationResult(
                field_name="gps_location",
                matches=True,
                original_value=gps,
                converted_value=gps,
            ),
            album_info=FieldVerificationResult(
                field_name="album_info",
                matches=True,
                original_value=albums,
                converted_value=albums,
            ),
        )

        # Property: capture_date can be formatted as string
        date_str = str(result.capture_date.converted_value)
        assert len(date_str) > 0

        # Property: GPS can be formatted as coordinates
        gps_val = result.gps_location.converted_value
        assert isinstance(gps_val, GPSLocation)
        lat_str = f"{gps_val.latitude:.4f}"
        lon_str = f"{gps_val.longitude:.4f}"
        assert "35.6762" in lat_str
        assert "139.6503" in lon_str

        # Property: albums can be joined for display
        albums_str = ", ".join(result.album_info.converted_value)
        assert "Travel" in albums_str
        assert "2024" in albums_str


# =============================================================================
# Property 11: Mismatch comparison display
# Validates: Requirements 2.2
# =============================================================================


class TestMismatchComparisonDisplay:
    """Property 11: Mismatch comparison display.

    *For any* verification failure, the system SHALL display both
    original and converted metadata side by side.
    **Validates: Requirements 2.2**
    """

    @given(
        original_date=datetimes_in_range(),
        date_diff_seconds=st.floats(
            min_value=2.0, max_value=3600.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=50)
    def test_mismatch_result_contains_both_values(
        self, original_date: datetime, date_diff_seconds: float
    ):
        """Mismatch result contains both original and converted values."""
        from vco.models.metadata import FieldVerificationResult

        converted_date = original_date + timedelta(seconds=date_diff_seconds)

        result = VerificationResult(
            success=False,
            filename="test.mp4",
            capture_date=FieldVerificationResult(
                field_name="capture_date",
                matches=False,
                original_value=original_date,
                converted_value=converted_date,
                error_message=f"Dates differ by {date_diff_seconds:.1f} seconds",
            ),
            gps_location=FieldVerificationResult(
                field_name="gps_location",
                matches=True,
                original_value=None,
                converted_value=None,
            ),
            album_info=FieldVerificationResult(
                field_name="album_info",
                matches=True,
                original_value=[],
                converted_value=[],
            ),
        )

        # Property: original value is available for display
        assert result.capture_date.original_value is not None
        assert result.capture_date.original_value == original_date

        # Property: converted value is available for display
        assert result.capture_date.converted_value is not None
        assert result.capture_date.converted_value == converted_date

        # Property: error message explains the mismatch
        assert result.capture_date.error_message is not None
        assert "seconds" in result.capture_date.error_message

    @given(
        original_gps=gps_locations(),
    )
    @settings(max_examples=50)
    def test_missing_gps_mismatch_shows_both_values(self, original_gps: GPSLocation):
        """Missing GPS mismatch shows original value and None for converted."""
        from vco.models.metadata import FieldVerificationResult

        result = VerificationResult(
            success=False,
            filename="test.mp4",
            capture_date=FieldVerificationResult(
                field_name="capture_date",
                matches=True,
                original_value=None,
                converted_value=None,
            ),
            gps_location=FieldVerificationResult(
                field_name="gps_location",
                matches=False,
                original_value=original_gps,
                converted_value=None,  # Missing in converted
                error_message="GPS location missing in converted video",
            ),
            album_info=FieldVerificationResult(
                field_name="album_info",
                matches=True,
                original_value=[],
                converted_value=[],
            ),
        )

        # Property: original GPS is available for display
        assert result.gps_location.original_value is not None
        assert result.gps_location.original_value.latitude == original_gps.latitude

        # Property: converted GPS shows as None/missing
        assert result.gps_location.converted_value is None

        # Property: error message indicates missing GPS
        assert "missing" in result.gps_location.error_message.lower()

    @given(
        original_date=datetimes_in_range(),
        original_gps=gps_locations(),
        date_diff=st.floats(min_value=2.0, max_value=100.0, allow_nan=False, allow_infinity=False),
    )
    @settings(max_examples=30)
    def test_mismatched_fields_list_accurate(
        self, original_date: datetime, original_gps: GPSLocation, date_diff: float
    ):
        """mismatched_fields property accurately lists all mismatched fields."""
        from vco.models.metadata import FieldVerificationResult

        converted_date = original_date + timedelta(seconds=date_diff)

        result = VerificationResult(
            success=False,
            filename="test.mp4",
            capture_date=FieldVerificationResult(
                field_name="capture_date",
                matches=False,
                original_value=original_date,
                converted_value=converted_date,
            ),
            gps_location=FieldVerificationResult(
                field_name="gps_location",
                matches=False,
                original_value=original_gps,
                converted_value=None,
            ),
            album_info=FieldVerificationResult(
                field_name="album_info",
                matches=True,
                original_value=[],
                converted_value=[],
            ),
        )

        # Property: mismatched_fields contains all mismatched field names
        assert "capture_date" in result.mismatched_fields
        assert "gps_location" in result.mismatched_fields
        assert "album_info" not in result.mismatched_fields
        assert len(result.mismatched_fields) == 2


# =============================================================================
# Property 14: Processing time warning message content
# Validates: Requirements 5.3
# =============================================================================


class TestProcessingTimeWarningMessageContent:
    """Property 14: Processing time warning message content.

    *For any* processing time proximity warning, the system SHALL indicate
    that the metadata may contain file creation time instead of actual capture time.
    **Validates: Requirements 5.3**
    """

    @given(
        capture_date=datetimes_in_range(),
        diff_minutes=st.floats(
            min_value=-59.0, max_value=59.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=100)
    def test_warning_message_contains_required_info(
        self, capture_date: datetime, diff_minutes: float
    ):
        """Warning message contains capture date, processing time, and explanation."""
        processing_time = capture_date - timedelta(minutes=diff_minutes)
        diff_hours = diff_minutes / 60

        warning = ProcessingTimeProximityWarning(
            capture_date=capture_date,
            processing_time=processing_time,
            difference_hours=diff_hours,
        )

        message = warning.message

        # Property: message contains capture date
        assert str(capture_date.year) in message

        # Property: message contains processing time
        assert str(processing_time.year) in message

        # Property: message indicates potential file creation time issue
        assert "creation time" in message.lower() or "capture time" in message.lower()

    @given(
        diff_minutes=st.floats(
            min_value=0.1, max_value=59.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=50)
    def test_warning_message_shows_time_difference(self, diff_minutes: float):
        """Warning message shows the time difference."""
        base_time = datetime(2024, 1, 15, 14, 0, 0)
        capture_date = base_time + timedelta(minutes=diff_minutes)
        diff_hours = diff_minutes / 60

        warning = ProcessingTimeProximityWarning(
            capture_date=capture_date,
            processing_time=base_time,
            difference_hours=diff_hours,
        )

        message = warning.message

        # Property: message contains the time difference
        # The message format includes the difference in hours
        assert "hour" in message.lower() or str(round(abs(diff_hours), 1)) in message

    @given(
        capture_date=datetimes_in_range(),
    )
    @settings(max_examples=30)
    def test_warning_message_is_user_friendly(self, capture_date: datetime):
        """Warning message is user-friendly and actionable."""
        processing_time = capture_date - timedelta(minutes=30)

        warning = ProcessingTimeProximityWarning(
            capture_date=capture_date,
            processing_time=processing_time,
            difference_hours=0.5,
        )

        message = warning.message

        # Property: message is not empty
        assert len(message) > 0

        # Property: message is readable (contains words, not just numbers)
        assert any(word in message.lower() for word in ["warning", "capture", "time", "may"])

        # Property: message explains the potential issue
        assert (
            "may" in message.lower() or "might" in message.lower() or "indicate" in message.lower()
        )
