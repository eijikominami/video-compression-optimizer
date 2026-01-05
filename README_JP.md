[English](README.md) / **日本語**

# Video Compression Optimizer (VCO)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)

Apple Photos 内の動画を H.265 形式に変換してストレージを節約するツール。

## 特徴

- Apple Photos ライブラリの動画を自動スキャン
- ネイティブ Swift PhotoKit 実装による高速で信頼性の高い Photos アクセス
- AWS MediaConvert による高品質な H.265 変換
- SSIM ベースの品質検証
- メタデータ（撮影日時、位置情報、アルバム）の保持
- iCloud 動画の状態検出
- Top-N 選択による効率的な変換

## 前提条件

- macOS 10.15 (Catalina) 以降
- Python 3.10 以降
- AWS アカウント（MediaConvert、S3、Lambda）

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

# iCloud 動画をスキップ（ローカルのみ処理）
vco convert --skip-icloud

# iCloud ダウンロードのタイムアウトを設定（デフォルト: 300 秒、範囲: 30-3600）
vco convert --download-timeout 600

# 確認プロンプトをスキップ
vco convert --yes
```

**iCloud 動画の自動ダウンロード**: `vco convert` を実行すると、iCloud のみの動画は Swift PhotoKit を使用して自動的にダウンロードされます。`--skip-icloud` でスキップ可能です。

変換は AWS Step Functions を通じて非同期で処理されます。変換を送信した後、状態を確認してタスクを管理できます：

```bash
# タスク状態を確認
vco status                    # 直近のタスク一覧（デフォルト: 10件）
vco status -n 20              # 表示件数を指定
vco status <task-id>          # タスク詳細を表示

# 実行中のタスクをキャンセル
vco cancel <task-id>

# 完了したファイルをインポート
vco import --list             # インポート可能なアイテム一覧（ローカル + AWS）
vco import --all              # 全アイテムをインポート
vco import <task-id:file-id>  # 特定の AWS ファイルをインポート
vco import --delete-original <task-id:file-id>  # インポート後にオリジナルを削除
```

### インポート

ローカルキューと AWS 完了タスクの両方から変換済み動画をインポートします：

```bash
# インポート待ちの一覧を表示（ローカル + AWS）
vco import --list

# 指定した動画を Photos にインポート
vco import <item-id>          # ローカル: review-id, AWS: task-id:file-id

# インポート後にオリジナル動画を自動削除
vco import --delete-original <item-id>

# 全ての動画を一括インポート（ローカル + AWS）
vco import --all

# 確認プロンプトをスキップ
vco import -y <item-id>
vco import -y --all

# 指定した ID をキューから削除（ファイルも削除）
vco import --remove <item-id>

# ローカルレビューキューのみクリア（ファイルも削除）
vco import --clear
```

**Item ID 形式**:
- ローカルアイテム: `abc123`（review ID）
- AWS アイテム: `task-uuid:file-uuid`（task:file 形式）

**オプション**:
- `--delete-original`: インポート成功後にオリジナル動画を Photos から自動削除（ゴミ箱に移動）
- `-y, --yes`: 確認プロンプトをスキップ

**注意**: 
- `--remove` と `--clear` オプションは、キューからの削除と同時に対応するファイルも削除します。
- `--clear` はローカルキューのみに影響し、AWS アイテムは S3 に残ります。
- `--delete-original` を指定しない場合、インポート後にオリジナル動画を Photos アプリで手動削除する必要があります。

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

### Swift バイナリのビルド

開発時は Swift バイナリを手動でビルドできます：

```bash
cd swift

# 現在のアーキテクチャ用にビルド
swift build

# Universal Binary をビルド（arm64 + x86_64）
./scripts/build_swift.sh
```

ビルドされたバイナリは `bin/vco-photos` に配置されます。

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
