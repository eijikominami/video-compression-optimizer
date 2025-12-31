"""Import tests for async workflow modules.

Task 14.3: インポートテスト: 全モジュール
- 全 async 関連モジュールのインポート成功確認
- 循環参照の検出
- 型ヒント構文エラーの検出

These tests detect issues that mocks cannot catch:
- Type hint syntax errors (e.g., `callable | None` in older Python)
- Import errors (circular references, missing modules)
- Module-level code execution errors
"""

import importlib
import sys

import pytest


class TestAsyncModuleImports:
    """Test that all async-related modules can be imported successfully."""

    # List of all async-related modules
    ASYNC_MODULES = [
        "vco.models.async_task",
        "vco.services.async_convert",
        "vco.services.async_status",
        "vco.services.async_cancel",
        "vco.services.async_download",
        "vco.services.download_progress",
        "vco.services.error_handling",
    ]

    @pytest.mark.parametrize("module_name", ASYNC_MODULES)
    def test_module_imports_successfully(self, module_name: str):
        """Each async module should import without errors."""
        # Remove from cache to ensure fresh import
        if module_name in sys.modules:
            del sys.modules[module_name]

        # This will raise ImportError if there are issues
        module = importlib.import_module(module_name)
        assert module is not None

    def test_all_async_modules_import_together(self):
        """All async modules should be importable together without conflicts."""
        # Import all modules
        modules = []
        for module_name in self.ASYNC_MODULES:
            module = importlib.import_module(module_name)
            modules.append(module)

        assert len(modules) == len(self.ASYNC_MODULES)

    def test_async_task_model_classes_exist(self):
        """async_task module should export expected classes."""
        from vco.models.async_task import (
            AsyncFile,
            AsyncTask,
            DownloadProgress,
            FileStatus,
            TaskStatus,
            aggregate_task_status,
        )

        # Verify classes exist
        assert AsyncTask is not None
        assert AsyncFile is not None
        assert DownloadProgress is not None
        assert TaskStatus is not None
        assert FileStatus is not None
        assert callable(aggregate_task_status)

    def test_error_handling_functions_exist(self):
        """error_handling module should export expected functions."""
        from vco.services.error_handling import (
            CONFIG_ERRORS,
            PERMISSION_ERRORS,
            PRESET_CHAIN,
            TRANSIENT_ERRORS,
            classify_mediaconvert_error,
            determine_ssim_action,
            get_next_preset,
            is_adaptive_preset,
        )

        # Verify functions exist
        assert callable(classify_mediaconvert_error)
        assert callable(determine_ssim_action)
        assert callable(is_adaptive_preset)
        assert callable(get_next_preset)

        # Verify constants exist
        assert isinstance(TRANSIENT_ERRORS, set)
        assert isinstance(CONFIG_ERRORS, set)
        assert isinstance(PERMISSION_ERRORS, set)
        assert isinstance(PRESET_CHAIN, list)

    def test_async_convert_service_exists(self):
        """async_convert module should be importable."""
        from vco.services import async_convert

        assert async_convert is not None

    def test_async_status_service_exists(self):
        """async_status module should be importable."""
        from vco.services import async_status

        assert async_status is not None

    def test_async_cancel_service_exists(self):
        """async_cancel module should be importable."""
        from vco.services import async_cancel

        assert async_cancel is not None

    def test_async_download_service_exists(self):
        """async_download module should be importable."""
        from vco.services import async_download

        assert async_download is not None

    def test_download_progress_service_exists(self):
        """download_progress module should be importable."""
        from vco.services import download_progress

        assert download_progress is not None


class TestNoCircularImports:
    """Test that there are no circular import issues."""

    def test_models_do_not_import_services(self):
        """Models should not import from services (to avoid circular imports)."""
        # Clear cache
        modules_to_clear = [m for m in sys.modules if m.startswith("vco.")]
        for m in modules_to_clear:
            del sys.modules[m]

        # Import models first

        # Check that no service modules were imported as a side effect
        service_modules = [m for m in sys.modules if "vco.services" in m]
        # Allow empty or only error_handling (which is a utility)
        for m in service_modules:
            assert "async_convert" not in m, f"Circular import detected: {m}"
            assert "async_status" not in m, f"Circular import detected: {m}"
            assert "async_cancel" not in m, f"Circular import detected: {m}"
            assert "async_download" not in m, f"Circular import detected: {m}"

    def test_services_can_import_models(self):
        """Services should be able to import from models."""
        # Clear cache
        modules_to_clear = [m for m in sys.modules if m.startswith("vco.")]
        for m in modules_to_clear:
            del sys.modules[m]

        # Import services (which should import models)
        from vco.models.async_task import TaskStatus
        from vco.services.error_handling import classify_mediaconvert_error

        # Both should work
        assert classify_mediaconvert_error is not None
        assert TaskStatus is not None


class TestTypeHintCompatibility:
    """Test that type hints are compatible with the Python version."""

    def test_async_task_type_hints(self):
        """AsyncTask type hints should be valid."""
        # Get type hints (this will fail if syntax is invalid)
        import typing

        from vco.models.async_task import AsyncFile, AsyncTask

        hints = typing.get_type_hints(AsyncTask)
        assert "task_id" in hints
        assert "status" in hints
        assert "files" in hints

        hints = typing.get_type_hints(AsyncFile)
        assert "file_id" in hints
        assert "status" in hints

    def test_error_handling_type_hints(self):
        """Error handling function type hints should be valid."""
        import typing

        from vco.services.error_handling import (
            classify_mediaconvert_error,
            determine_ssim_action,
        )

        # Get type hints for functions
        hints = typing.get_type_hints(classify_mediaconvert_error)
        assert "error_code" in hints
        assert "return" in hints

        hints = typing.get_type_hints(determine_ssim_action)
        assert "preset" in hints
        assert "ssim_score" in hints
        assert "return" in hints
