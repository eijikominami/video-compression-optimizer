"""Property-based tests for compression ratio calculation.

Feature: video-compression-optimizer, Property 11: Compression ratio calculation accuracy
Validates: Requirements 4.4
"""

from hypothesis import assume, given
from hypothesis import strategies as st

from vco.models.types import QualityResult


class TestCompressionRatioCalculation:
    """Property tests for compression ratio calculation accuracy.

    Property 11: For any successful conversion, compression ratio is calculated as
    (original_size / converted_size) and space savings is calculated as
    ((original_size - converted_size) / original_size * 100)%.

    Validates: Requirements 4.4
    """

    @given(
        original_size=st.integers(min_value=1, max_value=10_000_000_000),
        converted_size=st.integers(min_value=1, max_value=10_000_000_000),
    )
    def test_compression_ratio_formula(self, original_size: int, converted_size: int):
        """For any file sizes, compression ratio equals original_size / converted_size."""
        ratio = QualityResult.calculate_compression_ratio(original_size, converted_size)

        expected = original_size / converted_size
        assert abs(ratio - expected) < 1e-10, (
            f"Compression ratio {ratio} does not match expected {expected}"
        )

    @given(
        original_size=st.integers(min_value=1, max_value=10_000_000_000),
        converted_size=st.integers(min_value=1, max_value=10_000_000_000),
    )
    def test_space_saved_percent_formula(self, original_size: int, converted_size: int):
        """For any file sizes, space saved percent equals ((original - converted) / original * 100)."""
        percent = QualityResult.calculate_space_saved_percent(original_size, converted_size)

        expected = (original_size - converted_size) / original_size * 100
        assert abs(percent - expected) < 1e-10, (
            f"Space saved percent {percent} does not match expected {expected}"
        )

    @given(
        original_size=st.integers(min_value=1, max_value=10_000_000_000),
        converted_size=st.integers(min_value=1, max_value=10_000_000_000),
    )
    def test_compression_ratio_positive_when_sizes_positive(
        self, original_size: int, converted_size: int
    ):
        """For any positive file sizes, compression ratio is positive."""
        ratio = QualityResult.calculate_compression_ratio(original_size, converted_size)
        assert ratio > 0, "Compression ratio should be positive for positive sizes"

    @given(
        original_size=st.integers(min_value=1, max_value=10_000_000_000),
    )
    def test_compression_ratio_greater_than_one_when_size_reduced(self, original_size: int):
        """When converted size is smaller, compression ratio is greater than 1."""
        # Ensure converted size is smaller than original
        converted_size = original_size // 2
        assume(converted_size > 0)

        ratio = QualityResult.calculate_compression_ratio(original_size, converted_size)
        assert ratio > 1.0, "Compression ratio should be > 1 when size is reduced"

    @given(
        original_size=st.integers(min_value=1, max_value=10_000_000_000),
    )
    def test_space_saved_positive_when_size_reduced(self, original_size: int):
        """When converted size is smaller, space saved percent is positive."""
        converted_size = original_size // 2
        assume(converted_size > 0)

        percent = QualityResult.calculate_space_saved_percent(original_size, converted_size)
        assert percent > 0, "Space saved should be positive when size is reduced"

    @given(
        original_size=st.integers(min_value=1, max_value=10_000_000_000),
    )
    def test_space_saved_negative_when_size_increased(self, original_size: int):
        """When converted size is larger, space saved percent is negative."""
        converted_size = original_size * 2

        percent = QualityResult.calculate_space_saved_percent(original_size, converted_size)
        assert percent < 0, "Space saved should be negative when size is increased"

    def test_compression_ratio_zero_for_zero_converted_size(self):
        """Compression ratio returns 0 when converted size is 0 (edge case)."""
        ratio = QualityResult.calculate_compression_ratio(1000, 0)
        assert ratio == 0.0, "Compression ratio should be 0 for zero converted size"

    def test_space_saved_zero_for_zero_original_size(self):
        """Space saved returns 0 when original size is 0 (edge case)."""
        percent = QualityResult.calculate_space_saved_percent(0, 1000)
        assert percent == 0.0, "Space saved should be 0 for zero original size"

    @given(
        original_size=st.integers(min_value=1, max_value=10_000_000_000),
    )
    def test_space_saved_zero_when_sizes_equal(self, original_size: int):
        """When sizes are equal, space saved is 0%."""
        percent = QualityResult.calculate_space_saved_percent(original_size, original_size)
        assert abs(percent) < 1e-10, "Space saved should be 0 when sizes are equal"

    @given(
        original_size=st.integers(min_value=1, max_value=10_000_000_000),
    )
    def test_compression_ratio_one_when_sizes_equal(self, original_size: int):
        """When sizes are equal, compression ratio is 1."""
        ratio = QualityResult.calculate_compression_ratio(original_size, original_size)
        assert abs(ratio - 1.0) < 1e-10, "Compression ratio should be 1 when sizes are equal"
