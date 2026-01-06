# アーキテクチャ

## 概要

Video Compression Optimizer (VCO) は、Apple Photos の動画を H.265 形式に変換してストレージ容量を削減するツールです。AWS クラウドサービスを使用し、Step Functions による非同期処理で高品質な動画変換を提供します。

## システムコンテキスト

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              ユーザー環境                                │
│  ┌──────────┐    ┌─────────────┐    ┌──────────────────────────────┐   │
│  │ ユーザー │───▶│   VCO CLI   │───▶│    Apple Photos ライブラリ    │   │
│  └──────────┘    └──────┬──────┘    └──────────────────────────────┘   │
│                         │                                                │
└─────────────────────────┼────────────────────────────────────────────────┘
                          │ HTTPS/AWS SigV4
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              AWS クラウド                                │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────┐ │
│  │ API Gateway │───▶│   Lambda    │───▶│      Step Functions         │ │
│  └─────────────┘    └─────────────┘    └──────────────┬──────────────┘ │
│                                                        │                 │
│  ┌─────────────┐    ┌─────────────┐    ┌──────────────▼──────────────┐ │
│  │  DynamoDB   │◀───│MediaConvert │◀───│    Workflow Lambda          │ │
│  └─────────────┘    └─────────────┘    └─────────────────────────────┘ │
│                           │                                              │
│                     ┌─────▼─────┐                                        │
│                     │    S3     │                                        │
│                     └───────────┘                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

## コンポーネント

### VCO CLI

動画操作用の Python ベースのコマンドラインインターフェース。

| コマンド | 説明 |
|---------|------|
| `vco scan` | Apple Photos ライブラリの動画をスキャン |
| `vco convert` | H.265 変換タスクを送信 |
| `vco status` | 変換タスクの状態を確認 |
| `vco import` | 変換済み動画を Photos にインポート |
| `vco cancel` | 実行中の変換タスクをキャンセル |
| `vco config` | 設定を管理 |

### Swift PhotoKit バイナリ

Photos ライブラリアクセス用のネイティブ Swift 実装（`bin/vco-photos`）。

- Universal Binary（arm64 + x86_64）
- iCloud 動画の自動ダウンロード
- Photos ライブラリへの動画インポート

### AWS インフラストラクチャ

| リソース | 用途 |
|----------|------|
| **API Gateway** | 非同期操作用の REST API エンドポイント |
| **Lambda 関数** | タスク送信、状態確認、キャンセル、ワークフロー制御 |
| **Step Functions** | 非同期ワークフローのステートマシン |
| **MediaConvert** | H.265 動画トランスコーディング |
| **S3** | 動画ファイルストレージ（ソース、出力、メタデータ） |
| **DynamoDB** | タスクとファイルの状態追跡（90 日 TTL） |

## データフロー

### 変換ワークフロー

```
1. ユーザーが `vco convert` を実行
2. CLI が Photos ライブラリをスキャン（osxphotos）
3. iCloud 動画を自動ダウンロード（Swift PhotoKit）
4. CLI が署名付き URL 経由で動画を S3 にアップロード
5. CLI が Step Functions 実行を開始
6. Workflow Lambda がオーケストレーション:
   a. MediaConvert ジョブ作成
   b. 品質検証（SSIM チェック）
   c. DynamoDB への状態更新
7. ユーザーが `vco status` で状態を確認
8. ユーザーが `vco import` で完了ファイルをインポート:
   a. S3 から変換済み動画をダウンロード
   b. exiftool でメタデータを埋め込み（タイムゾーン付き Keys:CreationDate）
   c. メタデータが元動画と一致するか検証
   d. Photos ライブラリにインポート（Swift PhotoKit）
```

### ファイル状態遷移

```
PENDING → CONVERTING → COMPLETED → DOWNLOADED
                    ↘ FAILED     ↘ REMOVED
```

注: 品質評価（SSIM/VMAF）は MediaConvert のフレームごとのメトリクスを使用して CONVERTING フェーズ完了時に実行されます。

## 技術スタック

| レイヤー | 技術 |
|---------|------|
| CLI | Python 3.10+、Click、Rich |
| Photos スキャン | osxphotos |
| iCloud ダウンロード・インポート | Swift PhotoKit |
| メタデータ埋め込み | exiftool |
| AWS SDK | boto3 |
| インフラストラクチャ | SAM/CloudFormation |
| 動画処理 | AWS MediaConvert |
| 品質チェック | MediaConvert フレームごとのメトリクス（SSIM、VMAF） |

## 設計判断

### Swift ネイティブ実装

iCloud ダウンロードと Photos インポートに Swift PhotoKit を採用した理由:
- ネイティブ iCloud ダウンロードサポート（osxphotos では不安定）
- Photos ライブラリへの直接インポート
- スキャンは osxphotos を継続使用（安定性と詳細なメタデータ取得）

### 非同期専用処理

同期変換モードを削除:
- 全ての変換は AWS Step Functions を使用
- 長時間実行ジョブの適切な処理
- 並列ファイル処理
- 再開可能なダウンロード

### 品質プリセット

| プリセット | QVBR | ユースケース |
|-----------|------|-------------|
| `high` | 8-9 | 品質優先 |
| `balanced` | 6-7 | 推奨デフォルト |
| `balanced+` | 6-7 → 8-9 | アダプティブ（SSIM < 0.95 で high にリトライ） |
| `compression` | 4-5 | 最大圧縮 |

### S3 キー構造

```
tasks/{task_id}/source/{file_id}/{filename}        # ソースファイル
output/{task_id}/{file_id}/{stem}_h265.mp4         # 変換済みファイル
tasks/{task_id}/metadata/{file_id}/{filename}.json # メタデータ
```

## セキュリティ

- 全 API 呼び出しに AWS Signature V4 認証
- 最小権限の IAM ロール
- 安全なアップロード/ダウンロード用の S3 署名付き URL
- CLI に認証情報を保存しない（AWS プロファイルを使用）

## 参照

- [README_JP.md](README_JP.md) - インストールと使用方法
- [docs/api-specification.md](docs/api-specification.md) - REST API 詳細
- [docs/data-models.md](docs/data-models.md) - データモデル仕様
