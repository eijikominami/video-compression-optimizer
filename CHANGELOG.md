# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`vco import` コマンドの AWS 統合**
  - ローカルと AWS 両方の変換済み動画を統合的にインポート
  - `vco import --list` - ローカル + AWS の一覧表示（Source 列付き）
  - `vco import <task-id:file-id>` - AWS アイテムの単一インポート
  - `vco import --all` - ローカル + AWS の一括インポート（AWS は並列ダウンロード）
  - `vco import --remove <task-id:file-id>` - AWS アイテムの削除（S3 ファイル削除）
  - AWS 利用不可時はローカルのみ表示（警告メッセージ付き）

- **非同期変換ワークフロー（`vco convert --async`）**
  - AWS Step Functions による非同期変換処理
  - `vco status` - タスク状態の確認
  - `vco status <task-id>` - タスク詳細の表示
  - `vco cancel <task-id>` - 実行中タスクのキャンセル
  - 再開可能なダウンロード
  - 部分完了時の成功ファイルダウンロード

- **デフォルト変換モード設定**
  - `vco config set conversion.default_convert_mode sync|async`
  - 同期/非同期モードの切り替え

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

### Deprecated

- **`vco download` コマンド**
  - `vco import` に統合されました
  - 引き続き使用可能ですが、廃止予定の警告が表示されます

### Removed

- **`vco review` コマンド** - `vco import --list` を使用してください
- **`vco approve` コマンド** - `vco import <id>` を使用してください
- **`vco reject` コマンド** - `vco import --remove <id>` を使用してください

### Fixed

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
