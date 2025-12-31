#!/bin/bash
# FFmpeg Lambda Layer 作成スクリプト
#
# このスクリプトは FFmpeg の静的ビルドをダウンロードし、
# Lambda Layer として AWS にデプロイします。
#
# 前提条件:
# - AWS CLI がインストールされていること
# - AWS 認証情報が設定されていること
# - curl, tar, zip コマンドが利用可能であること
#
# 使用方法:
#   ./create-ffmpeg-layer.sh [OPTIONS]
#
# オプション:
#   --profile PROFILE    AWS プロファイル名 (デフォルト: default)
#   --region REGION      AWS リージョン (デフォルト: ap-northeast-1)
#   --bucket BUCKET      S3 バケット名 (必須)
#   --layer-name NAME    Layer 名 (デフォルト: vco-ffmpeg)
#   --dry-run            実際にデプロイせずに Layer zip を作成のみ
#   --help               ヘルプを表示

set -e

# デフォルト値
AWS_PROFILE="default"
AWS_REGION="ap-northeast-1"
S3_BUCKET=""
LAYER_NAME="vco-ffmpeg"
DRY_RUN=false
FFMPEG_URL="https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"

# 一時ディレクトリ
WORK_DIR=$(mktemp -d)
trap "rm -rf $WORK_DIR" EXIT

# ヘルプ表示
show_help() {
    cat << EOF
FFmpeg Lambda Layer 作成スクリプト

使用方法:
  $0 [OPTIONS]

オプション:
  --profile PROFILE    AWS プロファイル名 (デフォルト: default)
  --region REGION      AWS リージョン (デフォルト: ap-northeast-1)
  --bucket BUCKET      S3 バケット名 (必須)
  --layer-name NAME    Layer 名 (デフォルト: vco-ffmpeg)
  --dry-run            実際にデプロイせずに Layer zip を作成のみ
  --help               このヘルプを表示

例:
  $0 --bucket my-bucket --profile my-profile
  $0 --bucket my-bucket --layer-name custom-ffmpeg --dry-run
EOF
}

# 引数解析
while [[ $# -gt 0 ]]; do
    case $1 in
        --profile)
            AWS_PROFILE="$2"
            shift 2
            ;;
        --region)
            AWS_REGION="$2"
            shift 2
            ;;
        --bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        --layer-name)
            LAYER_NAME="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            echo "エラー: 不明なオプション: $1"
            show_help
            exit 1
            ;;
    esac
done

# 必須パラメータチェック
if [[ -z "$S3_BUCKET" ]] && [[ "$DRY_RUN" == false ]]; then
    echo "エラー: --bucket オプションは必須です"
    show_help
    exit 1
fi

echo "=== FFmpeg Lambda Layer 作成 ==="
echo "AWS Profile: $AWS_PROFILE"
echo "AWS Region: $AWS_REGION"
echo "S3 Bucket: $S3_BUCKET"
echo "Layer Name: $LAYER_NAME"
echo "Dry Run: $DRY_RUN"
echo ""

# Step 1: FFmpeg ダウンロード
echo "[1/5] FFmpeg 静的ビルドをダウンロード中..."
curl -L "$FFMPEG_URL" -o "$WORK_DIR/ffmpeg.tar.xz"
echo "  ダウンロード完了: $(ls -lh "$WORK_DIR/ffmpeg.tar.xz" | awk '{print $5}')"

# Step 2: 展開
echo "[2/5] アーカイブを展開中..."
tar -xf "$WORK_DIR/ffmpeg.tar.xz" -C "$WORK_DIR"
FFMPEG_DIR=$(ls -d "$WORK_DIR"/ffmpeg-*-amd64-static 2>/dev/null | head -1)

if [[ -z "$FFMPEG_DIR" ]]; then
    echo "エラー: FFmpeg ディレクトリが見つかりません"
    exit 1
fi

# Step 3: Layer 構造作成
echo "[3/5] Layer 構造を作成中..."
mkdir -p "$WORK_DIR/layer/bin"
cp "$FFMPEG_DIR/ffmpeg" "$WORK_DIR/layer/bin/"
cp "$FFMPEG_DIR/ffprobe" "$WORK_DIR/layer/bin/"
chmod +x "$WORK_DIR/layer/bin/"*

# バージョン確認
FFMPEG_VERSION=$("$WORK_DIR/layer/bin/ffmpeg" -version 2>/dev/null | head -1 | awk '{print $3}' || echo "unknown")
echo "  FFmpeg バージョン: $FFMPEG_VERSION"

# Step 4: ZIP 作成
echo "[4/5] ZIP アーカイブを作成中..."
LAYER_ZIP="$WORK_DIR/ffmpeg-layer.zip"
(cd "$WORK_DIR/layer" && zip -r9 "$LAYER_ZIP" bin/)
echo "  ZIP サイズ: $(ls -lh "$LAYER_ZIP" | awk '{print $5}')"

# Dry run の場合はここで終了
if [[ "$DRY_RUN" == true ]]; then
    OUTPUT_ZIP="./ffmpeg-layer.zip"
    cp "$LAYER_ZIP" "$OUTPUT_ZIP"
    echo ""
    echo "=== Dry Run 完了 ==="
    echo "Layer ZIP: $OUTPUT_ZIP"
    echo ""
    echo "手動でデプロイする場合:"
    echo "  aws s3 cp $OUTPUT_ZIP s3://YOUR_BUCKET/layers/ffmpeg-layer.zip"
    echo "  aws lambda publish-layer-version \\"
    echo "    --layer-name $LAYER_NAME \\"
    echo "    --content S3Bucket=YOUR_BUCKET,S3Key=layers/ffmpeg-layer.zip \\"
    echo "    --compatible-runtimes python3.11 python3.12 \\"
    echo "    --compatible-architectures x86_64"
    exit 0
fi

# Step 5: AWS にデプロイ
echo "[5/5] AWS にデプロイ中..."

# S3 にアップロード
S3_KEY="layers/ffmpeg-layer.zip"
echo "  S3 にアップロード: s3://$S3_BUCKET/$S3_KEY"
aws s3 cp "$LAYER_ZIP" "s3://$S3_BUCKET/$S3_KEY" \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION"

# Lambda Layer を発行
echo "  Lambda Layer を発行中..."
LAYER_OUTPUT=$(aws lambda publish-layer-version \
    --layer-name "$LAYER_NAME" \
    --description "FFmpeg $FFMPEG_VERSION - static binaries for video processing" \
    --content "S3Bucket=$S3_BUCKET,S3Key=$S3_KEY" \
    --compatible-runtimes python3.11 python3.12 \
    --compatible-architectures x86_64 \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --output json)

LAYER_VERSION=$(echo "$LAYER_OUTPUT" | grep -o '"Version": [0-9]*' | grep -o '[0-9]*')
LAYER_ARN=$(echo "$LAYER_OUTPUT" | grep -o '"LayerVersionArn": "[^"]*"' | cut -d'"' -f4)

echo ""
echo "=== デプロイ完了 ==="
echo "Layer ARN: $LAYER_ARN"
echo "Layer Version: $LAYER_VERSION"
echo ""
echo "SAM テンプレートでの使用例:"
echo "  Layers:"
echo "    - !Sub 'arn:aws:lambda:\${AWS::Region}:\${AWS::AccountId}:layer:$LAYER_NAME:$LAYER_VERSION'"
