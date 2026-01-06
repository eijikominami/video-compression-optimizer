# データモデル仕様書

## 概要

Video Compression Optimizer (VCO) のデータモデル仕様を定義します。

### データフロー

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Photos    │     │     CLI     │     │   AWS API   │     │  DynamoDB   │
│  Library    │────▶│  VideoInfo  │────▶│   Request   │────▶│    Task     │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                          │                                        │
                          ▼                                        ▼
                    ┌─────────────┐                          ┌─────────────┐
                    │  Metadata   │─────────────────────────▶│     S3      │
                    │    JSON     │                          │   Files     │
                    └─────────────┘                          └─────────────┘
```

### データストア

| ストア | 用途 | 保持期間 |
|--------|------|---------|
| DynamoDB | タスク・ファイル状態管理 | 90日 (TTL) |
| S3 | 動画ファイル、メタデータ JSON | 手動削除まで |

## データストア

### DynamoDB スキーマ

**テーブル名**: `vco-async-tasks-{Environment}`

#### テーブル構造

| 属性 | 型 | 説明 |
|------|-----|------|
| `task_id` (PK) | String | タスク UUID |
| `sk` (SK) | String | ソートキー（`TASK` 固定） |
| `user_id` | String | ユーザー識別子 |
| `status` | String | タスク状態 |
| `quality_preset` | String | 品質プリセット |
| `files` | List | ファイル情報配列 |
| `progress_percentage` | Number | 全体進捗 (0-100) |
| `current_step` | String | 現在の処理ステップ |
| `execution_arn` | String | Step Functions 実行 ARN |
| `created_at` | String | 作成日時 (ISO 8601) |
| `updated_at` | String | 更新日時 (ISO 8601) |
| `ttl` | Number | TTL タイムスタンプ（90日後） |

#### グローバルセカンダリインデックス

| インデックス名 | PK | SK | 用途 |
|---------------|-----|-----|------|
| GSI1-UserTasks | `user_id` | `created_at` | ユーザー別タスク一覧 |
| GSI2-StatusTasks | `status` | `created_at` | ステータス別タスク検索 |

#### タスクアイテム例

```json
{
  "task_id": "123e4567-e89b-12d3-a456-426614174000",
  "sk": "TASK",
  "user_id": "user-123",
  "status": "PROCESSING",
  "quality_preset": "balanced",
  "files": [
    {
      "file_id": "f1",
      "filename": "video1.mp4",
      "status": "COMPLETED",
      "source_s3_key": "async/task123/input/f1/video1.mp4",
      "output_s3_key": "output/task123/f1/video1_h265.mp4",
      "metadata_s3_key": "async/task123/input/f1/metadata.json"
    }
  ],
  "progress_percentage": 50,
  "current_step": "converting",
  "execution_arn": "arn:aws:states:ap-northeast-1:123456789012:execution:vco-workflow:vco-task123",
  "created_at": "2024-01-01T10:00:00+00:00",
  "updated_at": "2024-01-01T10:30:00+00:00",
  "ttl": 1712345678
}
```

### S3 ファイル構造

#### キー構造

```
async/{task_id}/input/{file_id}/{filename}        # ソースファイル
async/{task_id}/input/{file_id}/metadata.json     # メタデータ JSON
output/{task_id}/{file_id}/{stem}_h265.mp4        # 変換済みファイル
```

#### メタデータ JSON 形式

**パス**: `async/{task_id}/input/{file_id}/metadata.json`

```json
{
  "capture_date": "2024-01-01T12:00:00+09:00",
  "creation_date": "2024-01-01T12:00:00+09:00",
  "location": [35.6762, 139.6503],
  "albums": ["Vacation", "2024"],
  "original_uuid": "ABC123-DEF456-GHI789",
  "original_filename": "MVI_0001.MOV"
}
```

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `capture_date` | string \| null | 撮影日時 (ISO 8601) |
| `creation_date` | string \| null | ファイル作成日時 (ISO 8601) |
| `location` | [number, number] \| null | GPS座標 [緯度, 経度] |
| `albums` | string[] | 所属アルバム名 |
| `original_uuid` | string \| null | Photos ライブラリ内の UUID |
| `original_filename` | string \| null | オリジナルファイル名 |

## API データ形式

API リクエスト/レスポンスの詳細は [API 仕様書](api-specification.md) を参照。

### FileStatus

ファイル処理状態を表す列挙型。DynamoDB、API、Python モデルで共通。

| 値 | 説明 |
|-----|------|
| `PENDING` | 処理待ち |
| `CONVERTING` | MediaConvert 変換中 |
| `VERIFYING` | SSIM 品質検証中 |
| `COMPLETED` | 完了、ダウンロード可能 |
| `DOWNLOADED` | ダウンロード済み |
| `REMOVED` | ユーザーにより削除 |
| `FAILED` | 処理失敗 |

### 進捗計算ロジック

#### ファイル単位の進捗

| FileStatus | 進捗率 | 説明 |
|------------|--------|------|
| `PENDING` | 0% | 処理待ち |
| `CONVERTING` | 32% | MediaConvert 変換中（0-65% 範囲のデフォルト中間値） |
| `VERIFYING` | 65-99% | 品質検証中（verification_progress に基づく） |
| `COMPLETED` | 100% | 完了 |
| `DOWNLOADED` | 100% | ダウンロード済み |
| `FAILED` | 100% | 失敗（進捗計算上は完了扱い） |

#### VERIFYING 進捗の計算式

```
overall_progress = 65 + (verification_progress * 0.34)
```

- `verification_progress = 0` → 65%（SSIM 計算開始）
- `verification_progress = 30` → 75%（フレーム抽出完了）
- `verification_progress = 100` → 99%（SSIM 計算完了）

#### タスク全体の進捗

全ファイルの進捗率の平均値:

```python
task_progress = sum(file_progress for file in files) // len(files)
```

## 実装モデル（Python）

### 基底クラス

#### BaseVideoMetadata

全てのビデオメタデータの基底クラス。

**ファイル**: `src/vco/models/base.py`

| フィールド | 型 | 必須 | デフォルト | 説明 |
|-----------|-----|------|-----------|------|
| `uuid` | `str` | ○ | `""` | ビデオの一意識別子 |
| `filename` | `str` | ○ | `""` | 元のファイル名 |
| `file_size` | `int` | ○ | `0` | ファイルサイズ（バイト） |
| `capture_date` | `datetime \| None` | - | `None` | 撮影日時 |
| `location` | `tuple[float, float] \| None` | - | `None` | GPS座標 (緯度, 経度) |

**制約**:
- `uuid`: 空文字列不可
- `filename`: 空文字列不可
- `file_size`: 正の整数
- `location`: 緯度 -90〜90、経度 -180〜180

**変換**:
- `to_dict()`: 辞書への変換
- `from_dict()`: 辞書からの復元

### 継承モデル

#### VideoInfo

Apple Photos ライブラリからの動画情報。

**ファイル**: `src/vco/models/types.py`  
**継承**: `BaseVideoMetadata`

| フィールド | 型 | デフォルト | 説明 |
|-----------|-----|-----------|------|
| `path` | `Path` | `Path()` | 動画ファイルのパス |
| `codec` | `str` | `""` | 動画コーデック (h264, hevc等) |
| `resolution` | `tuple[int, int]` | `(0, 0)` | 解像度 (幅, 高さ) |
| `bitrate` | `int` | `0` | ビットレート (bps) |
| `duration` | `float` | `0.0` | 再生時間（秒） |
| `frame_rate` | `float` | `0.0` | フレームレート (fps) |
| `creation_date` | `datetime` | `datetime.now()` | ファイル作成日時 |
| `albums` | `list[str]` | `[]` | 所属アルバム名 |
| `is_in_icloud` | `bool` | `False` | iCloud 保存状態 |
| `is_local` | `bool` | `True` | ローカル利用可能状態 |

#### AsyncFile

非同期ワークフロー内の個別ファイル。

**ファイル**: `src/vco/models/async_task.py`  
**継承**: `BaseVideoMetadata`

| フィールド | 型 | デフォルト | 説明 |
|-----------|-----|-----------|------|
| `file_id` | `str` | `""` | ファイル固有ID |
| `source_s3_key` | `str` | `""` | アップロード先S3キー |
| `output_s3_key` | `str \| None` | `None` | 変換後S3キー |
| `metadata_s3_key` | `str \| None` | `None` | メタデータS3キー |
| `status` | `FileStatus` | `PENDING` | 処理状態 |
| `mediaconvert_job_id` | `str \| None` | `None` | MediaConvert ジョブID |
| `verification_progress` | `int` | `0` | VERIFYING フェーズの進捗 (0-100) |
| `quality_result` | `dict \| None` | `None` | 品質検証結果 |
| `error_code` | `int \| None` | `None` | エラーコード |
| `error_message` | `str \| None` | `None` | エラーメッセージ |
| `retry_count` | `int` | `0` | リトライ回数 |
| `preset_attempts` | `list[str]` | `[]` | 試行プリセット履歴 |
| `output_size_bytes` | `int \| None` | `None` | 変換後ファイルサイズ |
| `output_checksum` | `str \| None` | `None` | 出力ファイルチェックサム |
| `checksum_algorithm` | `str` | `"ETag"` | チェックサムアルゴリズム |
| `downloaded_at` | `datetime \| None` | `None` | ダウンロード完了日時 |
| `download_available` | `bool` | `False` | ダウンロード可能状態 |

**後方互換性**:
```python
@property
def original_uuid(self) -> str:
    """uuid フィールドのエイリアス"""
    return self.uuid
```

#### ConversionResult

動画変換結果。

**ファイル**: `src/vco/services/convert.py`  
**継承**: `BaseVideoMetadata`

| フィールド | 型 | デフォルト | 説明 |
|-----------|-----|-----------|------|
| `success` | `bool` | `False` | 変換成功状態 |
| `original_path` | `Path` | `Path()` | 元ファイルパス |
| `converted_path` | `Path \| None` | `None` | 変換後ファイルパス |
| `quality_result` | `QualityResult \| None` | `None` | 品質検証結果 |
| `metadata` | `VideoMetadata \| None` | `None` | 保持メタデータ |
| `error_message` | `str \| None` | `None` | エラーメッセージ |
| `mediaconvert_job_id` | `str \| None` | `None` | MediaConvert ジョブID |
| `quality_job_id` | `str \| None` | `None` | 品質チェックジョブID |
| `best_effort` | `bool` | `False` | ベストエフォートモード使用 |
| `selected_preset` | `str \| None` | `None` | 選択されたプリセット |

#### VideoMetadata

メタデータ保存・復元用クラス。S3 メタデータ JSON との相互変換に使用。

**ファイル**: `src/vco/metadata/manager.py`

| フィールド | 型 | デフォルト | 説明 |
|-----------|-----|-----------|------|
| `capture_date` | `datetime \| None` | `None` | 撮影日時 |
| `creation_date` | `datetime \| None` | `None` | ファイル作成日時 |
| `albums` | `list[str]` | `[]` | 所属アルバム名 |
| `title` | `str \| None` | `None` | 動画タイトル |
| `description` | `str \| None` | `None` | 動画説明 |
| `location` | `tuple[float, float] \| None` | `None` | GPS座標 (緯度, 経度) |
| `original_uuid` | `str \| None` | `None` | Photos ライブラリ内の UUID |
| `original_filename` | `str \| None` | `None` | オリジナルファイル名 |

**変換**:
- `to_dict()`: S3 メタデータ JSON 形式への変換
- `from_dict()`: S3 メタデータ JSON からの復元

### 変換・バリデーション

#### 変換関数

**ファイル**: `src/vco/models/converters.py`

```python
def video_info_to_async_file(video: VideoInfo, file_id: str | None = None) -> AsyncFile
def async_file_to_conversion_result(async_file: AsyncFile, original_path: Path) -> ConversionResult
def video_info_to_conversion_result(video: VideoInfo, success: bool = False, error_message: str | None = None) -> ConversionResult
```

#### 変換マッピング

| 変換先 | 変換元（優先1） | 変換元（優先2） | 欠損時 |
|--------|---------------|---------------|--------|
| `uuid` | `original_uuid` | `uuid` | エラー |
| `file_size` | `file_size` | `source_size_bytes` | 0 |
| `capture_date` | `capture_date` | - | `None` |
| `location` | `location` | - | `None` |

#### バリデーション

**ファイル**: `src/vco/models/validators.py`

| フィールド | 検証内容 | エラーメッセージ |
|-----------|---------|----------------|
| `uuid` | 非空文字列 | "uuid is required and cannot be empty" |
| `filename` | 非空文字列 | "filename is required and cannot be empty" |
| `file_size` | 正の整数 | "file_size must be positive" |
| `location` | 座標範囲 | "latitude must be between -90 and 90" |

### データストアとの対応

| DynamoDB 属性 | Python モデル | 変換 |
|--------------|--------------|------|
| `task_id` | `AsyncTask.task_id` | そのまま |
| `files[].file_id` | `AsyncFile.file_id` | そのまま |
| `files[].status` | `AsyncFile.status` | `FileStatus` Enum |
| `files[].metadata_s3_key` | `AsyncFile.metadata_s3_key` | そのまま |
| `created_at` | `AsyncTask.created_at` | ISO 8601 → `datetime` |

## 変更履歴

| バージョン | 日付 | 変更内容 |
|-----------|------|---------|
| 1.3.0 | 2026-01-06 | ドキュメント構成をリファクタリング（データストア → API → 実装の順序に変更） |
| 1.2.0 | 2026-01-05 | VideoMetadata に original_uuid, original_filename フィールド追加 |
| 1.1.0 | 2026-01-05 | AsyncFile に verification_progress フィールド追加 |
| 1.0.0 | 2026-01-01 | 初版作成 |

## 関連ドキュメント

- [API 仕様書](api-specification.md)
- [アーキテクチャ](../ARCHITECTURE.md)
