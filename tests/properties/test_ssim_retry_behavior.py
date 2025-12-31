"""Property tests for SSIM retry behavior.

**Property 3: SSIM リトライ動作の正確性**
**Validates: Requirements 5.3, 5.4**

Tests that SSIM threshold failures are handled correctly:
- Adaptive presets (balanced+, high+): retry with higher preset
- Non-adaptive presets (balanced, high): fail immediately
- SSIM >= threshold: always accept
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.services.error_handling import (
    PRESET_CHAIN,
    SSIMAction,
    determine_ssim_action,
    get_next_preset,
    is_adaptive_preset,
)


class TestSSIMRetryBehavior:
    """Property tests for SSIM retry behavior.

    Validates: Requirements 5.3, 5.4
    - Adaptive presets retry with higher quality when SSIM fails
    - Non-adaptive presets fail immediately when SSIM fails
    """

    # Test data defined independently from implementation
    # Adaptive presets end with "+"
    ADAPTIVE_PRESETS = ["balanced+", "high+"]
    NON_ADAPTIVE_PRESETS = ["balanced", "high"]
    ALL_PRESETS = ADAPTIVE_PRESETS + NON_ADAPTIVE_PRESETS

    DEFAULT_THRESHOLD = 0.95

    @given(
        preset=st.sampled_from(ALL_PRESETS),
        ssim_score=st.floats(min_value=0.95, max_value=1.0),
    )
    @settings(max_examples=100)
    def test_ssim_above_threshold_always_accepted(self, preset: str, ssim_score: float):
        """
        Property: For all presets, SSIM >= threshold results in accept.

        Feature: async-workflow, Property 3: SSIM リトライ動作の正確性
        Validates: Requirements 5.3, 5.4
        """
        result = determine_ssim_action(preset, ssim_score, self.DEFAULT_THRESHOLD)

        assert result.action == SSIMAction.ACCEPT.value

    @given(
        preset=st.sampled_from(NON_ADAPTIVE_PRESETS),
        ssim_score=st.floats(min_value=0.0, max_value=0.9499),
    )
    @settings(max_examples=100)
    def test_non_adaptive_preset_fails_immediately(self, preset: str, ssim_score: float):
        """
        Property: For non-adaptive presets, SSIM < threshold results in fail.

        Feature: async-workflow, Property 3: SSIM リトライ動作の正確性
        Validates: Requirements 5.3, 5.4
        """
        result = determine_ssim_action(preset, ssim_score, self.DEFAULT_THRESHOLD)

        assert result.action == SSIMAction.FAIL.value
        assert result.next_preset is None

    @given(ssim_score=st.floats(min_value=0.0, max_value=0.9499))
    @settings(max_examples=100)
    def test_adaptive_balanced_plus_retries_with_high_plus(self, ssim_score: float):
        """
        Property: For balanced+ preset, SSIM < threshold results in retry with high+.

        Feature: async-workflow, Property 3: SSIM リトライ動作の正確性
        Validates: Requirements 5.3, 5.4
        """
        result = determine_ssim_action("balanced+", ssim_score, self.DEFAULT_THRESHOLD)

        assert result.action == SSIMAction.RETRY_WITH_HIGHER_PRESET.value
        assert result.next_preset == "high"

    @given(ssim_score=st.floats(min_value=0.0, max_value=0.9499))
    @settings(max_examples=100)
    def test_adaptive_high_plus_fails_no_more_presets(self, ssim_score: float):
        """
        Property: For high+ preset (last in chain), SSIM < threshold results in fail.

        Feature: async-workflow, Property 3: SSIM リトライ動作の正確性
        Validates: Requirements 5.3, 5.4
        """
        result = determine_ssim_action("high+", ssim_score, self.DEFAULT_THRESHOLD)

        assert result.action == SSIMAction.FAIL.value
        assert result.next_preset is None

    @given(
        preset=st.sampled_from(ALL_PRESETS),
        ssim_score=st.floats(min_value=0.0, max_value=1.0),
        threshold=st.floats(min_value=0.5, max_value=0.99),
    )
    @settings(max_examples=200)
    def test_action_depends_on_threshold_comparison(
        self, preset: str, ssim_score: float, threshold: float
    ):
        """
        Property: Action depends on whether SSIM >= threshold.

        Feature: async-workflow, Property 3: SSIM リトライ動作の正確性
        Validates: Requirements 5.3, 5.4
        """
        result = determine_ssim_action(preset, ssim_score, threshold)

        if ssim_score >= threshold:
            assert result.action == SSIMAction.ACCEPT.value
        else:
            # Below threshold: depends on preset type
            if is_adaptive_preset(preset):
                # Adaptive: retry or fail depending on chain position
                assert result.action in [
                    SSIMAction.RETRY_WITH_HIGHER_PRESET.value,
                    SSIMAction.FAIL.value,
                ]
            else:
                # Non-adaptive: always fail
                assert result.action == SSIMAction.FAIL.value

    @given(preset=st.sampled_from(ADAPTIVE_PRESETS))
    @settings(max_examples=100)
    def test_is_adaptive_preset_true_for_plus_suffix(self, preset: str):
        """
        Property: Presets ending with + are adaptive.

        Feature: async-workflow, Property 3: SSIM リトライ動作の正確性
        Validates: Requirements 5.3, 5.4
        """
        assert is_adaptive_preset(preset) is True

    @given(preset=st.sampled_from(NON_ADAPTIVE_PRESETS))
    @settings(max_examples=100)
    def test_is_adaptive_preset_false_for_no_suffix(self, preset: str):
        """
        Property: Presets not ending with + are non-adaptive.

        Feature: async-workflow, Property 3: SSIM リトライ動作の正確性
        Validates: Requirements 5.3, 5.4
        """
        assert is_adaptive_preset(preset) is False

    def test_get_next_preset_from_balanced(self):
        """Example: Next preset after balanced is high."""
        assert get_next_preset("balanced") == "high"
        assert get_next_preset("balanced+") == "high"

    def test_get_next_preset_from_high(self):
        """Example: No next preset after high (end of chain)."""
        assert get_next_preset("high") is None
        assert get_next_preset("high+") is None

    def test_get_next_preset_unknown(self):
        """Example: Unknown preset returns None."""
        assert get_next_preset("unknown") is None

    def test_preset_chain_order(self):
        """Verify preset chain is in expected order."""
        assert PRESET_CHAIN == ["balanced", "high"]

    @given(
        ssim_score=st.floats(min_value=0.0, max_value=1.0),
    )
    @settings(max_examples=100)
    def test_result_action_is_valid(self, ssim_score: float):
        """
        Property: Result action is always one of the valid values.

        Feature: async-workflow, Property 3: SSIM リトライ動作の正確性
        Validates: Requirements 5.3, 5.4
        """
        valid_actions = {
            SSIMAction.ACCEPT.value,
            SSIMAction.RETRY_WITH_HIGHER_PRESET.value,
            SSIMAction.FAIL.value,
        }

        for preset in self.ALL_PRESETS:
            result = determine_ssim_action(preset, ssim_score, self.DEFAULT_THRESHOLD)
            assert result.action in valid_actions

    def test_specific_example_balanced_ssim_094(self):
        """Example: balanced preset with SSIM 0.94 fails immediately."""
        result = determine_ssim_action("balanced", 0.94, 0.95)
        assert result.action == "fail"

    def test_specific_example_balanced_plus_ssim_094(self):
        """Example: balanced+ preset with SSIM 0.94 retries with high+."""
        result = determine_ssim_action("balanced+", 0.94, 0.95)
        assert result.action == "retry_with_higher_preset"
        assert result.next_preset == "high"

    def test_specific_example_high_ssim_096(self):
        """Example: high preset with SSIM 0.96 is accepted."""
        result = determine_ssim_action("high", 0.96, 0.95)
        assert result.action == "accept"
