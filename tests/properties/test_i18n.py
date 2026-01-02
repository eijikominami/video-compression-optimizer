"""Property-based tests for i18n module.

Validates:
- Requirements 1.1, 1.2: Help messages in correct language based on locale
- Requirements 1.3, 1.4: Locale detection from environment variables
- Requirements 3.3: All help keys have translations
"""

import os
from unittest.mock import patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from vco.cli.i18n import HELP_MESSAGES, get_help, get_locale


class TestLocaleDetectionPriority:
    """Property 1: Locale detection priority.

    For any combination of LANG and LC_ALL environment variables,
    locale detection SHALL prioritize LC_ALL over LANG.

    Validates: Requirements 1.3, 1.4
    """

    # Japanese locale patterns
    JA_LOCALES = ["ja", "ja_JP", "ja_JP.UTF-8", "ja_JP.eucJP", "JA", "JA_JP"]
    # Non-Japanese locale patterns
    NON_JA_LOCALES = ["en", "en_US", "en_US.UTF-8", "de_DE", "fr_FR", "zh_CN", "ko_KR", ""]

    @given(
        lang=st.sampled_from(JA_LOCALES + NON_JA_LOCALES),
        lc_all=st.sampled_from(JA_LOCALES + NON_JA_LOCALES),
    )
    @settings(max_examples=100)
    def test_lc_all_takes_priority_over_lang(self, lang: str, lc_all: str):
        """LC_ALL should take priority over LANG for locale detection.

        Feature: cli-localization, Property 1: Locale detection priority
        Validates: Requirements 1.3, 1.4
        """
        env = {"LANG": lang, "LC_ALL": lc_all}
        with patch.dict(os.environ, env, clear=True):
            result = get_locale()

            # LC_ALL takes priority
            if lc_all:
                expected = "ja" if lc_all.lower().startswith("ja") else "en"
            else:
                expected = "ja" if lang.lower().startswith("ja") else "en"

            assert result == expected, (
                f"Expected {expected} for LANG={lang}, LC_ALL={lc_all}, got {result}"
            )

    @given(lang=st.sampled_from(JA_LOCALES))
    @settings(max_examples=50)
    def test_japanese_lang_without_lc_all(self, lang: str):
        """Japanese LANG should return 'ja' when LC_ALL is not set.

        Feature: cli-localization, Property 1: Locale detection priority
        Validates: Requirements 1.3
        """
        env = {"LANG": lang}
        with patch.dict(os.environ, env, clear=True):
            result = get_locale()
            assert result == "ja", f"Expected 'ja' for LANG={lang}, got {result}"

    @given(lc_all=st.sampled_from(NON_JA_LOCALES))
    @settings(max_examples=50)
    def test_non_japanese_lc_all_overrides_japanese_lang(self, lc_all: str):
        """Non-Japanese LC_ALL should override Japanese LANG.

        Feature: cli-localization, Property 1: Locale detection priority
        Validates: Requirements 1.4
        """
        env = {"LANG": "ja_JP.UTF-8", "LC_ALL": lc_all}
        with patch.dict(os.environ, env, clear=True):
            result = get_locale()
            # Empty LC_ALL falls back to LANG
            if lc_all:
                assert result == "en", f"Expected 'en' for LC_ALL={lc_all}, got {result}"
            else:
                assert result == "ja", f"Expected 'ja' (fallback to LANG), got {result}"

    def test_empty_environment_defaults_to_english(self):
        """Empty environment should default to English.

        Feature: cli-localization, Property 1: Locale detection priority
        Validates: Requirements 1.3
        """
        with patch.dict(os.environ, {}, clear=True):
            result = get_locale()
            assert result == "en", f"Expected 'en' for empty environment, got {result}"


class TestHelpMessageConsistency:
    """Property 2: Help message consistency.

    For any help message key, the returned message SHALL be in Japanese
    when locale is "ja" and in English otherwise.

    Validates: Requirements 1.1, 1.2
    """

    @given(key=st.sampled_from(list(HELP_MESSAGES.keys())))
    @settings(max_examples=100)
    def test_japanese_locale_returns_japanese_message(self, key: str):
        """Japanese locale should return Japanese help message.

        Feature: cli-localization, Property 2: Help message consistency
        Validates: Requirements 1.1
        """
        env = {"LC_ALL": "ja_JP.UTF-8"}
        with patch.dict(os.environ, env, clear=True):
            result = get_help(key)
            expected = HELP_MESSAGES[key]["ja"]
            assert result == expected, f"Expected Japanese message for key={key}"

    @given(key=st.sampled_from(list(HELP_MESSAGES.keys())))
    @settings(max_examples=100)
    def test_english_locale_returns_english_message(self, key: str):
        """English locale should return English help message.

        Feature: cli-localization, Property 2: Help message consistency
        Validates: Requirements 1.2
        """
        env = {"LC_ALL": "en_US.UTF-8"}
        with patch.dict(os.environ, env, clear=True):
            result = get_help(key)
            expected = HELP_MESSAGES[key]["en"]
            assert result == expected, f"Expected English message for key={key}"

    @given(
        key=st.sampled_from(list(HELP_MESSAGES.keys())),
        locale=st.sampled_from(["de_DE", "fr_FR", "zh_CN", "ko_KR", ""]),
    )
    @settings(max_examples=100)
    def test_non_japanese_locale_returns_english_message(self, key: str, locale: str):
        """Non-Japanese locale should return English help message.

        Feature: cli-localization, Property 2: Help message consistency
        Validates: Requirements 1.2
        """
        env = {"LC_ALL": locale} if locale else {}
        with patch.dict(os.environ, env, clear=True):
            result = get_help(key)
            expected = HELP_MESSAGES[key]["en"]
            assert result == expected, f"Expected English message for key={key}, locale={locale}"

    def test_undefined_key_raises_error(self):
        """Undefined key should raise KeyError.

        Feature: cli-localization, Property 2: Help message consistency
        """
        with pytest.raises(KeyError):
            get_help("undefined.key")


class TestAllKeysHaveTranslations:
    """Property 3: All help keys have translations.

    For any help message key used in the CLI, both Japanese and English
    translations SHALL exist.

    Validates: Requirements 3.3
    """

    # Expected keys that should exist in HELP_MESSAGES
    # Source: CLI command definitions in main.py
    EXPECTED_KEYS = [
        "cli.description",
        "scan.description",
        "scan.from_date",
        "scan.to_date",
        "scan.top_n",
        "scan.json",
        "convert.description",
        "convert.quality",
        "convert.top_n",
        "convert.dry_run",
        "import.description",
        "import.list",
        "import.all",
        "import.clear",
        "import.remove",
        "import.json",
        "config.description",
        "config.json",
        "config.set.description",
        "status.description",
        "status.filter",
        "status.json",
        "cancel.description",
    ]

    def test_all_expected_keys_exist(self):
        """All expected keys should exist in HELP_MESSAGES.

        Feature: cli-localization, Property 3: All keys have translations
        Validates: Requirements 3.3
        """
        for key in self.EXPECTED_KEYS:
            assert key in HELP_MESSAGES, f"Missing key: {key}"

    @given(key=st.sampled_from(EXPECTED_KEYS))
    @settings(max_examples=100)
    def test_all_keys_have_both_translations(self, key: str):
        """Each key should have both Japanese and English translations.

        Feature: cli-localization, Property 3: All keys have translations
        Validates: Requirements 3.3
        """
        assert "ja" in HELP_MESSAGES[key], f"Missing Japanese translation for key: {key}"
        assert "en" in HELP_MESSAGES[key], f"Missing English translation for key: {key}"

    @given(key=st.sampled_from(EXPECTED_KEYS))
    @settings(max_examples=100)
    def test_translations_are_non_empty(self, key: str):
        """Each translation should be non-empty.

        Feature: cli-localization, Property 3: All keys have translations
        Validates: Requirements 3.3
        """
        assert HELP_MESSAGES[key]["ja"].strip(), f"Empty Japanese translation for key: {key}"
        assert HELP_MESSAGES[key]["en"].strip(), f"Empty English translation for key: {key}"

    def test_no_extra_keys_in_help_messages(self):
        """HELP_MESSAGES should not contain unexpected keys.

        Feature: cli-localization, Property 3: All keys have translations
        """
        for key in HELP_MESSAGES:
            assert key in self.EXPECTED_KEYS, f"Unexpected key in HELP_MESSAGES: {key}"
