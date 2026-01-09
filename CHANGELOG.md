# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-01-09

### Changed

- **MediaConvert エンコード設定の最適化**
  - `QualityTuningLevel`: `MULTI_PASS_HQ` に変更（2パスエンコードで最適なビットレート配分）
  - `DynamicSubGop`: `ADAPTIVE` に変更（コンテンツに基づく動的 B フレーム調整）
  - `GopBReference`: `ENABLED` に変更（B フレーム参照で圧縮効率向上）
  - `AdaptiveQuantization`: `AUTO` に変更（自動量子化最適化）
  - `GopSizeUnits`: `AUTO` に変更（MediaConvert が最適な GOP サイズを自動選択）
  - 固定値（`GopSize`, `NumberBFramesBetweenReferenceFrames`, `MinIInterval`）を削除し、MediaConvert の自動最適化に委任

- **MediaConvert フレームごとのメトリクス機能への移行**
  - Quality Checker Lambda を廃止し、MediaConvert の PerFrameMetrics 機能を使用
  - SSIM に加えて VMAF メトリクスをサポート
  - ワークフローを簡素化（VERIFYING ステータスを削除）
  - 品質評価を Async Workflow Lambda 内で実行

- **メタデータ埋め込みを Lambda から CLI に移行**
  - Lambda (quality-checker) でのメタデータ埋め込みを廃止
  - CLI で exiftool を使用して `Keys:CreationDate` タグに書き込み
  - Photos アプリでタイムゾーンが正しく表示されるように修正（9 時間ズレ問題の解決）
  - 新しい依存関係: exiftool (`brew install exiftool`)

### Removed

- **Quality Checker Lambda の廃止**
  - FFprobe ベースの SSIM 計算を MediaConvert の PerFrameMetrics に置き換え
  - `sam-app/quality-checker/` ディレクトリを削除
  - `VERIFYING` ステータスを削除（`FileStatus` enum から削除）
  - `verification_progress` フィールドを削除（`AsyncFile` から削除）

### Fixed

- **`vco import --all` モードでオリジナル動画削除プロンプトが表示されない問題を修正**
  - `--all` モードでインポート成功後に "Delete N original video(s)?" プロンプトが表示されるように修正
  - `--delete-original` フラグ指定時はプロンプトなしで削除実行
  - `-y` フラグのみ指定時はプロンプトなし、削除なし、リマインダー表示
- **API レスポンスに `metadata_s3_key` フィールドが含まれない問題を修正**
  - `async-task-status` Lambda 関数のレスポンスに `metadata_s3_key` を追加
  - クライアント側でフォールバック S3 パス取得を実装（API が返さない場合の互換性対応）

### Added

- **MediaConvert 品質メトリクス解析モジュール**
  - `QualityMetrics` データクラス（SSIM/VMAF の average, min, max を保持）
  - `QualityMetricsParser` クラス（CSV 解析ロジック）
  - `QualityEvaluator` クラス（閾値評価ロジック）
  - デフォルト閾値: SSIM >= 0.95, VMAF >= 70
  - 環境変数による閾値設定: `SSIM_THRESHOLD`, `VMAF_THRESHOLD`

- **メタデータ検証機能**
  - `vco import` 実行時に変換後動画のメタデータを自動検証
  - 撮影日時の検証（±1 秒許容）
  - GPS 位置情報の検証（±0.0001 度許容）
  - 検証失敗時はインポートをスキップ（`--force` でバイパス可能）
  - 処理時刻近接警告: 撮影日時が処理時刻の ±1 時間以内の場合に警告表示
  - バッチインポート時の検証サマリー表示
  - CLI の進捗表示が VERIFYING フェーズで 65-99% の範囲で動的に更新
- **オリジナル動画削除機能**
  - `vco import --delete-original` オプションでインポート後にオリジナル動画を自動削除
  - 削除された動画は Photos のゴミ箱に移動（30 日間復元可能）
  - 削除失敗時もインポートは成功として扱い、警告メッセージを表示
- **インポート進捗表示の改善**
  - 単一インポート時に Photos へのインポート中スピナー表示を追加
  - ダウンロード完了後、インポート処理中の状態を視覚的に表示
- **iCloud 自動ダウンロード機能**
  - `vco convert` 実行時に iCloud 動画を自動ダウンロード
  - Swift PhotoKit の `PHAssetResourceManager` を使用したネイティブダウンロード
  - Rich Progress による進捗表示
  - `--skip-icloud` オプションで iCloud 動画をスキップ可能
  - `--download-timeout` オプションでタイムアウト設定（デフォルト: 300 秒）
  - `--yes` / `-y` オプションで確認プロンプトをスキップ
  - ディスク容量チェックとエラーハンドリング
- **Swift Native PhotoKit 実装**
  - Photos ライブラリアクセスを Python (osxphotos) から Swift (PhotoKit) に移行
  - パフォーマンス向上: ネイティブ API による高速スキャン
  - Universal Binary (arm64 + x86_64) 対応
  - Swift バイナリは `bin/vco-photos` としてパッケージに同梱
- **`-y` オプションの全コマンド対応**
  - `vco import` コマンドに `--yes` / `-y` オプション追加
  - `vco cancel` コマンドに `--yes` / `-y` オプション追加
  - 全ての確認プロンプトをスキップ可能
- **`vco import` コマンドの AWS 統合**
  - ローカルと AWS 両方の変換済み動画を統合的にインポート
  - `vco import --list` - ローカル + AWS の一覧表示（Source 列付き）
  - `vco import <task-id:file-id>` - AWS アイテムの単一インポート
  - `vco import --all` - ローカル + AWS の一括インポート（AWS は並列ダウンロード）
  - `vco import --remove <task-id:file-id>` - AWS アイテムの削除（S3 ファイル削除）
  - AWS 利用不可時はローカルのみ表示（警告メッセージ付き）
- **非同期変換ワークフロー**
  - AWS Step Functions による非同期変換処理
  - `vco status` - タスク状態の確認
  - `vco status <task-id>` - タスク詳細の表示
  - `vco cancel <task-id>` - 実行中タスクのキャンセル
  - 再開可能なダウンロード
  - 部分完了時の成功ファイルダウンロード
- **AWS インフラストラクチャ（非同期ワークフロー用）**
  - DynamoDB テーブル（タスク管理）
  - API Gateway（REST API）
  - Step Functions ステートマシン
  - Lambda 関数（Submit, Status, Cancel, Workflow）
- **`vco import` コマンド**
  - `vco import --list` - インポート待ちの一覧表示（アルバム名付き）
  - `vco import <review-id>` - 単一動画のインポート
  - `vco import --all` - 全動画の一括インポート
  - `vco import --remove <review-id>` - 指定アイテムをキューから削除（ファイルも削除）
  - `vco import --clear` - キュー内の全アイテムを削除（ファイルも削除）
  - Photos へのインポートとアルバム登録を実行
  - オリジナル動画の削除は手動（macOS 制限のため）
- **ファイル自動削除機能**
  - `vco import --remove` と `vco import --clear` で変換済みファイルとメタデータを自動削除
  - ストレージ効率の向上とユーザビリティの改善
  - エラー時の適切なログ出力と部分成功の報告
- **`balanced+` アダプティブプリセット**
  - balanced で変換後、SSIM が閾値を下回った場合に自動的に high でリトライ
  - 品質を確保しつつ、可能な限り圧縮率を高める
  - `vco convert --quality balanced+` で使用

### Changed

- **非同期モードがデフォルトに**
  - `vco convert` は常に非同期モード（AWS Step Functions）で動作
  - `--async` フラグは削除（不要になったため）
  - 同期モード（ローカル変換）は廃止

### Deprecated

- **`vco download` コマンド**
  - `vco import` に統合されました
  - 引き続き使用可能ですが、廃止予定の警告が表示されます

### Removed

- **ローカル変換関連コード**
  - `ReviewService`, `ImportService` を削除（AWS 専用に移行）

### Fixed

- **AWS アイテムのオリジナル動画削除が機能しない問題を修正**
  - `vco import --delete-original` で AWS アイテムのオリジナル動画が削除されない問題を修正
  - メタデータ JSON から `original_uuid` を抽出し、削除処理に使用するように変更
  - 引数で `original_uuid` が指定されていない場合、ダウンロード結果から自動取得
  - `_upload_metadata()` に `original_uuid` と `original_filename` フィールドを追加（S3 メタデータ JSON に含まれるように修正）
  - `review.py`, `import_service.py` を削除
  - 関連するテストファイルを削除
- **`--legacy` オプション**
  - `vco convert --legacy` は削除
  - iCloud ダウンロードは常に Swift PhotoKit を使用
  - osxphotos によるフォールバックは廃止
- **`--async` フラグ**
  - `vco convert --async` は `vco convert` に統合
  - 非同期モードがデフォルトになったため不要
- **`default_convert_mode` 設定**
  - `vco config set conversion.default_convert_mode` は削除
  - 同期モードが廃止されたため不要
- **同期変換機能（ConvertService）**
  - ローカルでの同期変換機能を削除
  - 全ての変換は AWS Step Functions 経由で実行
- **`vco review` コマンド** - `vco import --list` を使用してください
- **`vco approve` コマンド** - `vco import <id>` を使用してください
- **`vco reject` コマンド** - `vco import --remove <id>` を使用してください

### Fixed

- **アップロード進捗表示の修正**
  - 最後のファイルのアップロード進捗が途中で止まって見える問題を修正
  - 全ファイルのアップロード完了後に進捗バーを 100% に更新
- **確認プロンプトの統合**
  - `vco convert` の 2 回の確認プロンプト（iCloud ダウンロード + アップロード）を 1 回に統合
  - 操作概要を表示してから単一の確認プロンプトを表示
- **CLI エラーハンドリングの改善**
  - AWS エラー発生時に Python Traceback を表示しないよう修正
  - 404, 403, ExpiredToken などの一般的な AWS エラーに対するユーザーフレンドリーなメッセージを追加
  - Photos インポート失敗時に S3 ファイルが削除されないことを保証するテストを追加
- **Lambda エフェメラルストレージ不足の修正**
  - 品質チェック Lambda のエフェメラルストレージを 512MB から 10GB に増加
  - 大容量動画（最大約 5GB）の品質チェックが可能に
  - 原因: 4GB の動画を `/tmp` にダウンロードする際に容量不足エラーが発生
- **S3 クリーンアップの不備修正**
  - エラー発生時に `metadata.json` が S3 に残る問題を修正
  - エラーハンドリングで `metadata_s3_key` の削除を追加
- **boto3 Lambda 呼び出しタイムアウトの修正**
  - Lambda クライアントの read_timeout を 60 秒から 900 秒に延長
  - 長時間の品質チェック処理でタイムアウトしなくなった

## [0.1.0] - 2024-12-27

### Added

- **Photos ライブラリスキャン**
  - Apple Photos ライブラリの動画を自動スキャン
  - 日付範囲フィルタリング（`--from`, `--to`）
  - コーデック分類（非効率/最適化済み/プロフェッショナル）
  - 推定容量削減の計算
- **H.265 変換**
  - AWS MediaConvert による高品質な H.265 変換
  - 3 つの品質プリセット（high, balanced, compression）
  - QVBR（Quality-Defined Variable Bitrate）エンコーディング
  - バッチ変換とエラー耐性
- **品質検証**
  - Lambda 関数による SSIM ベースの品質検証
  - ファイルサイズ比較
  - 再生可能性検証
  - 品質ゲート（SSIM 0.85 以上）
- **メタデータ保持**
  - 撮影日時（capture_date）の保持
  - 位置情報（GPS 座標）の保持
  - アルバム情報の保持
  - Lambda でのメタデータ埋め込み（フォールバック付き）
- **レビューワークフロー**
  - 変換結果のレビューキュー
  - 承認/拒否機能
  - Photos へのインポートとオリジナルのゴミ箱移動
  - 変換後の自動レビューキュー登録
- **Top-N 選択機能**
  - ファイルサイズの大きい順に上位 N 件を選択
  - `vco scan --top-n N` でスキャン結果を絞り込み
  - `vco convert --top-n N` で変換対象を絞り込み
  - 合計ファイルサイズと推定削減量の表示
- **iCloud 対応**
  - iCloud 状態の検出（ローカル/iCloud）
  - iCloud のみの動画のスキップ処理
  - 実行時の iCloud 状態再チェック
- **ファイル安全性**
  - ディスク容量の事前チェック
  - オリジナルファイルの保護
  - 変換成功・検証完了まで元ファイルを変更しない
- **CLI コマンド**
  - `vco scan` - Photos ライブラリのスキャン
  - `vco convert` - 動画の変換
  - `vco import` - 変換済み動画のインポート
  - `vco config` - 設定の表示・変更
- **AWS インフラストラクチャ**
  - SAM テンプレートによる自動デプロイ
  - S3 バケット、Lambda 関数、IAM ロールの作成
  - FFmpeg Lambda Layer 作成スクリプト

### Technical Details

- Python 3.10+ 対応
- osxphotos/photoscript による Photos ライブラリアクセス
- boto3 による AWS サービス連携
- Hypothesis によるプロパティベーステスト（156 テスト）
- Rich による CLI 出力フォーマット
