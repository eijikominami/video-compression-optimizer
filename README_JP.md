[English](README.md) / **日本語**

# Video Compression Optimizer (VCO)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)

Apple Photos 内の動画を H.265 形式に変換してストレージを節約するツール。

## 特徴

- Apple Photos ライブラリの動画を自動スキャン
- AWS MediaConvert による高品質な H.265 変換
- SSIM ベースの品質検証
- メタデータ（撮影日時、位置情報、アルバム）の保持
- iCloud 動画の状態検出
- Top-N 選択による効率的な変換

## 前提条件

- macOS 10.15 (Catalina) 以降
- Python 3.10 以降
- AWS アカウント（MediaConvert、S3、Lambda）
- iCloud 動画を変換する場合は、事前に Photos アプリで「オリジナルをダウンロード」を実行

## インストール

```bash
pip install .
```

開発環境：

```bash
pip install -e ".[dev]"
```

## AWS インフラストラクチャのデプロイ

### 1. FFmpeg Lambda Layer の作成

品質チェック Lambda 関数には FFmpeg が必要です。以下のスクリプトで Lambda Layer を作成します：

```bash
cd sam-app/scripts

# Layer を作成してデプロイ
./create-ffmpeg-layer.sh \
  --bucket <your-s3-bucket> \
  --profile <your-aws-profile> \
  --region ap-northeast-1

# dry-run モード（ZIP 作成のみ、デプロイなし）
./create-ffmpeg-layer.sh --dry-run
```

スクリプトは以下を実行します：
1. FFmpeg 静的ビルドをダウンロード
2. Lambda Layer 用の ZIP を作成
3. S3 にアップロード
4. Lambda Layer を発行

### 2. SAM テンプレートのデプロイ

```bash
cd sam-app
sam build
sam deploy --stack-name vco-infrastructure \
  --capabilities CAPABILITY_NAMED_IAM \
  --resolve-s3 \
  --profile <your-aws-profile> \
  --region ap-northeast-1
```

## 使い方

### スキャン

```bash
# Apple Photos ライブラリをスキャン
vco scan

# 日付範囲を指定
vco scan --from 2020-01 --to 2020-12

# ファイルサイズの大きい順に上位 N 件を表示
vco scan --top-n 10

# JSON 形式で出力
vco scan --json
```

### 変換

```bash
# 変換を実行（デフォルト: balanced）
vco convert

# 品質プリセットを指定
vco convert --quality high

# ファイルサイズの大きい順に上位 N 件のみ変換
vco convert --top-n 5

# ドライラン（実際の変換なし）
vco convert --dry-run

# 非同期変換（AWS Step Functions に送信）
vco convert --async
```

### 非同期ワークフロー

大量の動画を変換する場合は、非同期ワークフローを使用して AWS Step Functions にタスクを送信できます：

```bash
# 非同期変換タスクを送信
vco convert --async

# タスク状態を確認
vco status                    # アクティブなタスク一覧
vco status <task-id>          # タスク詳細を表示

# 実行中のタスクをキャンセル
vco cancel <task-id>

# 完了したファイルをダウンロード
vco download <task-id>        # 全ての完了ファイルをダウンロード
vco download <task-id> --resume  # 中断したダウンロードを再開
```

#### 非同期ワークフローの利点

- **バックグラウンド処理**: タスクを送信して後で状態を確認
- **並列変換**: 複数ファイルを同時に処理
- **再開可能なダウンロード**: 中断したダウンロードを再開可能
- **部分完了**: 一部失敗しても成功したファイルをダウンロード可能

#### 設定

```bash
# デフォルトの変換モードを設定
vco config set conversion.default_convert_mode async

# 現在のモードを確認
vco config
```

### インポート

```bash
# インポート待ちの一覧を表示（アルバム名付き）
vco import --list

# 指定した動画を Photos にインポート
vco import <review-id>

# 全ての動画を一括インポート
vco import --all

# 指定した ID をキューから削除（ファイルも削除）
vco import --remove <review-id>

# レビューキューを全てクリア（ファイルも削除）
vco import --clear
```

**注意**: `--remove` と `--clear` オプションは、レビューキューからの削除と同時に、対応する変換済み動画ファイルとメタデータファイルもステージングフォルダから削除します。

インポート後、オリジナル動画は Photos アプリで手動削除してください。

### 設定

```bash
# 現在の設定を表示
vco config

# AWS 設定
vco config set aws.s3_bucket <bucket>
vco config set aws.role_arn <arn>
vco config set aws.region ap-northeast-1

# 変換設定
vco config set conversion.quality_preset balanced
vco config set conversion.max_concurrent 3
```

## 品質プリセット

| プリセット | QVBR | 用途 |
|-----------|------|------|
| `high` | 8-9 | 高品質を維持したい場合 |
| `balanced` | 6-7 | 品質とサイズのバランス（推奨） |
| `balanced+` | 6-7 → 8-9 | balanced で品質 NG なら high でリトライ（ベストエフォート） |
| `compression` | 4-5 | 最大限の圧縮 |

### balanced+ プリセット（アダプティブ）

`balanced+` は adaptive プリセットで、以下の動作をします：

1. まず `balanced` で変換し、SSIM スコアをチェック
2. SSIM >= 0.95 なら成功として終了
3. SSIM < 0.95 なら `high` で再変換
4. `high` でも SSIM < 0.95 の場合、**ベストエフォートモード**が適用され、より高い SSIM スコアの結果を採用

ベストエフォートモードでは、SSIM 閾値を満たせなくても変換は成功として扱われます。CLI 出力でベストエフォートモードが使用されたことが表示されます：

```
Best-effort mode used:
  - video.mp4: preset=balanced, SSIM=0.9132
```

## iCloud 動画の処理

iCloud にのみ保存されている動画（ローカルにダウンロードされていない動画）は自動的にダウンロードできません。これは osxphotos ライブラリの制限です。

### スキャン時の動作

`vco scan` を実行すると、各動画の iCloud 状態（Local/iCloud）が表示されます：

```
⚠ 10 videos are in iCloud only and need to be downloaded first.
Open Photos app and download these videos before running 'vco convert':

  - IMG_1234.mov
  - IMG_5678.mov
  ...
```

### 変換時の動作

`vco convert` を実行すると、ローカルで利用可能な動画のみが変換されます。iCloud のみの動画はスキップされます。

### 手動ダウンロード手順

1. Photos アプリを開く
2. iCloud のみの動画を選択
3. 右クリック → 「オリジナルをダウンロード」を選択
4. ダウンロード完了後、`vco scan` を再実行してファイルパスを更新
5. `vco convert` を実行

## ワークフロー

### 基本的な使い方

```bash
# 1. スキャン
vco scan

# 2. AWS 設定（初回のみ）
vco config set aws.s3_bucket my-bucket
vco config set aws.role_arn arn:aws:iam::123456789012:role/vco-mediaconvert-role

# 3. 変換
vco convert

# 4. インポート
vco import --list          # 一覧確認
vco import --all           # 一括インポート

# 5. オリジナル動画の削除（手動）
# Photos アプリで元の動画を選択して削除
```

### 効率的な変換（Top-N）

ストレージ削減効果を最大化するには、ファイルサイズの大きい動画から変換します：

```bash
# 上位 10 件をスキャン
vco scan --top-n 10

# 上位 5 件を変換
vco convert --top-n 5
```

## 言語サポート

VCO CLI は **英語** と **日本語** のヘルプメッセージをサポートしています。

### 自動言語検出

CLI はシステムロケールを自動検出します：
- **日本語ロケール** (ja, ja_JP など): ヘルプメッセージを日本語で表示
- **その他のロケール**: ヘルプメッセージを英語で表示

**注意**: 出力メッセージ（進捗、結果、エラー）は一貫性と検索性のため、常に英語で表示されます。

## 開発

### テスト実行

```bash
# 全テスト
python3.11 -m pytest tests/ -v

# プロパティテスト
python3.11 -m pytest tests/properties/ -v

# カバレッジ
python3.11 -m pytest tests/ --cov=src/vco --cov-report=term-missing
```

### コード品質

```bash
# フォーマット
ruff format src/ tests/

# Lint
ruff check src/ tests/

# 型チェック
mypy src/
```

## ライセンス

MIT License

## コントリビュート & サポート

- **バグ報告**: [GitHub Issues](https://github.com/eijikominami/video-compression-optimizer/issues)
- **機能リクエスト**: [GitHub Issues](https://github.com/eijikominami/video-compression-optimizer/issues)
- **変更履歴**: [CHANGELOG.md](CHANGELOG.md)
