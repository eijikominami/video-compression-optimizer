"""Internationalization module for VCO CLI.

Provides locale detection and help message localization.
Help messages are displayed in Japanese for Japanese locales,
and in English for all other locales.
"""

import os
from typing import Literal

Locale = Literal["ja", "en"]


def get_locale() -> Locale:
    """Detect locale from environment variables.

    Priority: LC_ALL > LANG
    Returns "ja" for Japanese locales (ja_JP, ja), "en" otherwise.
    """
    # LC_ALL takes priority over LANG
    locale_str = os.environ.get("LC_ALL") or os.environ.get("LANG") or ""
    locale_str = locale_str.lower()

    # Check for Japanese locale
    if locale_str.startswith("ja"):
        return "ja"

    return "en"


def get_help(key: str) -> str:
    """Get localized help message for the given key.

    Args:
        key: The message key (e.g., "cli.description")

    Returns:
        Localized help message string

    Raises:
        KeyError: If the key is not found in HELP_MESSAGES
    """
    locale = get_locale()
    return HELP_MESSAGES[key][locale]


# Help messages dictionary with Japanese and English translations
HELP_MESSAGES: dict[str, dict[Locale, str]] = {
    # Main CLI
    "cli.description": {
        "ja": "Video Compression Optimizer - Apple Photos 動画圧縮最適化ツール",
        "en": "Video Compression Optimizer - Apple Photos video compression tool",
    },
    # scan command
    "scan.description": {
        "ja": "Apple Photos ライブラリをスキャンして変換候補を表示\n\n"
        "--top-n オプションを指定すると、ファイルサイズの大きい順に\n"
        "上位 N 件の候補のみを表示します。ストレージ削減効果を\n"
        "最大化したい場合に有効です。",
        "en": "Scan Apple Photos library and display conversion candidates\n\n"
        "Use --top-n option to display only the top N candidates\n"
        "by file size. Useful for maximizing storage savings.",
    },
    "scan.from_date": {
        "ja": "開始日 (YYYY-MM)",
        "en": "Start date (YYYY-MM)",
    },
    "scan.to_date": {
        "ja": "終了日 (YYYY-MM)",
        "en": "End date (YYYY-MM)",
    },
    "scan.top_n": {
        "ja": "ファイルサイズの大きい順に上位N件のみを表示",
        "en": "Display only top N candidates by file size",
    },
    "scan.json": {
        "ja": "JSON形式で出力",
        "en": "Output in JSON format",
    },
    # convert command
    "convert.description": {
        "ja": "変換候補の動画を H.265 に変換\n\n"
        "--top-n オプションを指定すると、ファイルサイズの大きい順に\n"
        "上位 N 件の候補のみを変換します。",
        "en": "Convert candidate videos to H.265\n\n"
        "Use --top-n option to convert only the top N candidates\n"
        "by file size.",
    },
    "convert.quality": {
        "ja": "品質プリセット (balanced+ は balanced で NG なら high でリトライ)",
        "en": "Quality preset (balanced+ retries with high if balanced fails)",
    },
    "convert.top_n": {
        "ja": "ファイルサイズの大きい順に上位N件のみを変換",
        "en": "Convert only top N candidates by file size",
    },
    "convert.dry_run": {
        "ja": "実際の変換を行わずにシミュレーション",
        "en": "Simulate without actual conversion",
    },
    # import command
    "import.description": {
        "ja": "変換済み動画を Photos ライブラリにインポート\n\n"
        "使用方法:\n"
        "  vco import --list        インポート待ちの一覧を表示\n"
        "  vco import <review_id>   指定した動画をインポート\n"
        "  vco import --all         全ての動画を一括インポート\n"
        "  vco import --remove <id> 指定したIDをキューから削除\n"
        "  vco import --clear       レビューキューを全てクリア\n\n"
        "インポート後、オリジナル動画は Photos アプリで手動削除してください。",
        "en": "Import converted videos to Photos library\n\n"
        "Usage:\n"
        "  vco import --list        List pending imports\n"
        "  vco import <review_id>   Import specified video\n"
        "  vco import --all         Import all pending videos\n"
        "  vco import --remove <id> Remove specified ID from queue\n"
        "  vco import --clear       Clear both local and AWS queues\n\n"
        "After import, manually delete original videos in Photos app.",
    },
    "import.list": {
        "ja": "インポート待ちの一覧を表示",
        "en": "List pending imports",
    },
    "import.all": {
        "ja": "全てのインポート待ちを一括インポート",
        "en": "Import all pending videos at once",
    },
    "import.clear": {
        "ja": "ローカルとAWS両方のキューを全てクリア",
        "en": "Clear both local and AWS queues entirely",
    },
    "import.remove": {
        "ja": "指定したIDをレビューキューから削除",
        "en": "Remove specified ID from review queue",
    },
    "import.json": {
        "ja": "JSON形式で出力",
        "en": "Output in JSON format",
    },
    # config command
    "config.description": {
        "ja": "設定を表示または変更",
        "en": "Display or modify configuration",
    },
    "config.json": {
        "ja": "JSON形式で出力",
        "en": "Output in JSON format",
    },
    "config.set.description": {
        "ja": "設定値を変更",
        "en": "Modify configuration value",
    },
    # status command
    "status.description": {
        "ja": "非同期タスクの状態を確認\n\n"
        "使用方法:\n"
        "  vco status              アクティブなタスク一覧を表示\n"
        "  vco status <task_id>    指定したタスクの詳細を表示",
        "en": "Check async task status\n\n"
        "Usage:\n"
        "  vco status              List active tasks\n"
        "  vco status <task_id>    Show task details",
    },
    "status.filter": {
        "ja": "状態でフィルタ (PENDING, CONVERTING, COMPLETED, FAILED)",
        "en": "Filter by status (PENDING, CONVERTING, COMPLETED, FAILED)",
    },
    "status.json": {
        "ja": "JSON形式で出力",
        "en": "Output in JSON format",
    },
    # cancel command
    "cancel.description": {
        "ja": "実行中の非同期タスクをキャンセル",
        "en": "Cancel a running async task",
    },
    # download command
    "download.description": {
        "ja": "完了した非同期タスクの結果をダウンロード\n\n"
        "COMPLETED または PARTIALLY_COMPLETED 状態のタスクから\n"
        "変換済みファイルをダウンロードします。",
        "en": "Download results from completed async tasks\n\n"
        "Downloads converted files from tasks in COMPLETED\n"
        "or PARTIALLY_COMPLETED status.",
    },
    "download.output": {
        "ja": "出力ディレクトリ",
        "en": "Output directory",
    },
    "download.no_resume": {
        "ja": "中断したダウンロードを再開しない",
        "en": "Do not resume interrupted downloads",
    },
    "download.json": {
        "ja": "JSON形式で出力",
        "en": "Output in JSON format",
    },
}
