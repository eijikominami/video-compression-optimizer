"""Property tests for local/server state synchronization.

Property 13: Local/Server State Sync
For any file marked as downloaded in DynamoDB (downloaded_at is not null),
the local DownloadProgressStore SHALL NOT have pending progress for that file
after startup sync.

Requirements: 8.5, 8.6
"""

from datetime import datetime
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus
from vco.services.download_progress import DownloadProgress, DownloadProgressStore

# Strategies for generating test data
file_id_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_"),
    min_size=1,
    max_size=36,
).filter(lambda x: x.strip() != "")

task_id_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_"),
    min_size=1,
    max_size=36,
).filter(lambda x: x.strip() != "")


class TestProperty13LocalServerSync:
    """Property 13: Local/Server State Sync.

    For any file marked as downloaded in DynamoDB (downloaded_at is not null),
    the local DownloadProgressStore SHALL NOT have pending progress for that file
    after startup sync.
    """

    @given(
        task_id=task_id_strategy,
        file_id=file_id_strategy,
        downloaded_bytes=st.integers(min_value=0, max_value=1_000_000_000),
        total_bytes=st.integers(min_value=1, max_value=1_000_000_000),
    )
    @settings(max_examples=50)
    def test_downloaded_files_cleared_after_sync(
        self, task_id, file_id, downloaded_bytes, total_bytes, tmp_path_factory
    ):
        """Files marked as downloaded on server are cleared from local store."""
        tmp_path = tmp_path_factory.mktemp("cache")

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id=task_id,
            file_id=file_id,
            total_bytes=total_bytes,
            downloaded_bytes=min(downloaded_bytes, total_bytes),
            local_temp_path=f"/tmp/{file_id}.mp4.part",
            s3_key=f"output/{task_id}/{file_id}/video.mp4",
        )
        store.save_progress(progress)

        # Mock server response - file already downloaded
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = AsyncTask(
            task_id=task_id,
            user_id="user-123",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id=file_id,
                    original_uuid="photos-uuid",
                    filename="video.mov",
                    source_s3_key=f"tasks/{task_id}/source/{file_id}/video.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=datetime(2024, 1, 1, 12, 0, 0),  # Downloaded
                    download_available=True,
                )
            ],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Sync with server
        store.sync_with_server(mock_service)

        # Property: local progress should be cleared
        assert store.get_progress(task_id, file_id) is None

    @given(
        task_id=task_id_strategy,
        file_id=file_id_strategy,
        downloaded_bytes=st.integers(min_value=0, max_value=1_000_000_000),
        total_bytes=st.integers(min_value=1, max_value=1_000_000_000),
    )
    @settings(max_examples=50)
    def test_unavailable_files_cleared_after_sync(
        self, task_id, file_id, downloaded_bytes, total_bytes, tmp_path_factory
    ):
        """Files no longer available on server are cleared from local store."""
        tmp_path = tmp_path_factory.mktemp("cache")

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id=task_id,
            file_id=file_id,
            total_bytes=total_bytes,
            downloaded_bytes=min(downloaded_bytes, total_bytes),
            local_temp_path=f"/tmp/{file_id}.mp4.part",
            s3_key=f"output/{task_id}/{file_id}/video.mp4",
        )
        store.save_progress(progress)

        # Mock server response - file no longer available
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = AsyncTask(
            task_id=task_id,
            user_id="user-123",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id=file_id,
                    original_uuid="photos-uuid",
                    filename="video.mov",
                    source_s3_key=f"tasks/{task_id}/source/{file_id}/video.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=None,
                    download_available=False,  # No longer available
                )
            ],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Sync with server
        store.sync_with_server(mock_service)

        # Property: local progress should be cleared
        assert store.get_progress(task_id, file_id) is None

    @given(
        task_id=task_id_strategy,
        file_id=file_id_strategy,
        downloaded_bytes=st.integers(min_value=0, max_value=1_000_000_000),
        total_bytes=st.integers(min_value=1, max_value=1_000_000_000),
    )
    @settings(max_examples=50)
    def test_pending_files_kept_after_sync(
        self, task_id, file_id, downloaded_bytes, total_bytes, tmp_path_factory
    ):
        """Files still available and not downloaded are kept in local store."""
        tmp_path = tmp_path_factory.mktemp("cache")

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id=task_id,
            file_id=file_id,
            total_bytes=total_bytes,
            downloaded_bytes=min(downloaded_bytes, total_bytes),
            local_temp_path=f"/tmp/{file_id}.mp4.part",
            s3_key=f"output/{task_id}/{file_id}/video.mp4",
        )
        store.save_progress(progress)

        # Mock server response - file still available, not downloaded
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = AsyncTask(
            task_id=task_id,
            user_id="user-123",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id=file_id,
                    original_uuid="photos-uuid",
                    filename="video.mov",
                    source_s3_key=f"tasks/{task_id}/source/{file_id}/video.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=None,  # Not downloaded
                    download_available=True,  # Still available
                )
            ],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Sync with server
        store.sync_with_server(mock_service)

        # Property: local progress should be kept
        assert store.get_progress(task_id, file_id) is not None

    @given(
        task_id=task_id_strategy,
        file_id=file_id_strategy,
    )
    @settings(max_examples=50)
    def test_task_not_found_clears_progress(self, task_id, file_id, tmp_path_factory):
        """Files for tasks not found on server are cleared from local store."""
        tmp_path = tmp_path_factory.mktemp("cache")

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id=task_id,
            file_id=file_id,
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path=f"/tmp/{file_id}.mp4.part",
            s3_key=f"output/{task_id}/{file_id}/video.mp4",
        )
        store.save_progress(progress)

        # Mock server response - task not found
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = None

        # Sync with server
        store.sync_with_server(mock_service)

        # Property: local progress should be cleared
        assert store.get_progress(task_id, file_id) is None


class TestSyncIdempotency:
    """Test that sync operations are idempotent."""

    @given(
        task_id=task_id_strategy,
        file_id=file_id_strategy,
    )
    @settings(max_examples=30)
    def test_sync_is_idempotent(self, task_id, file_id, tmp_path_factory):
        """Multiple sync calls produce the same result."""
        tmp_path = tmp_path_factory.mktemp("cache")

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id=task_id,
            file_id=file_id,
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path=f"/tmp/{file_id}.mp4.part",
            s3_key=f"output/{task_id}/{file_id}/video.mp4",
        )
        store.save_progress(progress)

        # Mock server response - file already downloaded
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = AsyncTask(
            task_id=task_id,
            user_id="user-123",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id=file_id,
                    original_uuid="photos-uuid",
                    filename="video.mov",
                    source_s3_key=f"tasks/{task_id}/source/{file_id}/video.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=datetime(2024, 1, 1, 12, 0, 0),
                    download_available=True,
                )
            ],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # First sync
        store.sync_with_server(mock_service)

        # Second sync (should be no-op)
        result2 = store.sync_with_server(mock_service)

        # Property: second sync should have no effect
        assert result2["cleared"] == []
        assert result2["unavailable"] == []
        assert store.get_progress(task_id, file_id) is None


class TestSyncErrorHandling:
    """Test error handling during sync."""

    @given(
        task_id=task_id_strategy,
        file_id=file_id_strategy,
    )
    @settings(max_examples=30)
    def test_api_error_preserves_progress(self, task_id, file_id, tmp_path_factory):
        """API errors during sync preserve local progress."""
        tmp_path = tmp_path_factory.mktemp("cache")

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id=task_id,
            file_id=file_id,
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path=f"/tmp/{file_id}.mp4.part",
            s3_key=f"output/{task_id}/{file_id}/video.mp4",
        )
        store.save_progress(progress)

        # Mock server response - API error
        mock_service = MagicMock()
        mock_service.get_task_status.side_effect = Exception("API error")

        # Sync with server
        result = store.sync_with_server(mock_service)

        # Property: local progress should be preserved on error
        assert task_id in result["errors"]
        assert store.get_progress(task_id, file_id) is not None
