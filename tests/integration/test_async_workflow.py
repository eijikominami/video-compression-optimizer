"""Integration tests for async workflow using moto.

Tests the end-to-end flow of async conversion workflow:
1. Task submission
2. Status checking
3. Task cancellation
4. File download

Requirements: 1.1, 2.1, 3.1, 4.1
"""

import json
import uuid
from datetime import datetime, timedelta

import boto3
import pytest
from moto import mock_aws

from vco.models.async_task import (
    AsyncFile,
    AsyncTask,
    FileStatus,
    TaskStatus,
    aggregate_task_status,
)
from vco.services.error_handling import (
    classify_mediaconvert_error,
    determine_ssim_action,
)

# Test constants
TEST_REGION = "ap-northeast-1"
TEST_BUCKET = "test-vco-bucket"
TEST_TABLE = "test-vco-async-tasks"
TEST_USER_ID = "test-user-123"


@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = TEST_REGION


@pytest.fixture
def dynamodb_table(aws_credentials):
    """Create mock DynamoDB table."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=TEST_REGION)

        # Create table matching SAM template
        table = dynamodb.create_table(
            TableName=TEST_TABLE,
            KeySchema=[
                {"AttributeName": "task_id", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "task_id", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "user-tasks-index",
                    "KeySchema": [
                        {"AttributeName": "user_id", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "status-index",
                    "KeySchema": [
                        {"AttributeName": "status", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        table.wait_until_exists()
        yield table


@pytest.fixture
def s3_bucket(aws_credentials):
    """Create mock S3 bucket."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=TEST_REGION)
        s3.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
        )
        yield s3


class TestAsyncTaskDynamoDB:
    """Integration tests for AsyncTask with DynamoDB."""

    def test_save_and_load_task(self, dynamodb_table):
        """Test saving and loading task from DynamoDB."""
        task_id = str(uuid.uuid4())
        now = datetime.now()

        # Create task
        task = AsyncTask(
            task_id=task_id,
            user_id=TEST_USER_ID,
            status=TaskStatus.PENDING,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id=str(uuid.uuid4()),
                    uuid="photo-uuid-1",
                    filename="video1.mp4",
                    source_s3_key=f"async/{task_id}/input/video1.mp4",
                    status=FileStatus.PENDING,
                    source_size_bytes=1000000,
                )
            ],
            created_at=now,
            updated_at=now,
        )

        # Save to DynamoDB
        task_dict = task.to_dict()
        task_dict["sk"] = "TASK"

        dynamodb_table.put_item(Item=task_dict)

        # Load from DynamoDB
        response = dynamodb_table.get_item(Key={"task_id": task_id, "sk": "TASK"})

        assert "Item" in response
        loaded_dict = response["Item"]

        # Remove sk before converting back
        del loaded_dict["sk"]
        loaded_task = AsyncTask.from_dict(loaded_dict)

        assert loaded_task.task_id == task.task_id
        assert loaded_task.user_id == task.user_id
        assert loaded_task.status == task.status
        assert loaded_task.quality_preset == task.quality_preset
        assert len(loaded_task.files) == 1
        assert loaded_task.files[0].filename == "video1.mp4"

    def test_query_tasks_by_user(self, dynamodb_table):
        """Test querying tasks by user_id using GSI."""
        now = datetime.now()

        # Create multiple tasks for same user
        for i in range(3):
            task_id = str(uuid.uuid4())
            task = AsyncTask(
                task_id=task_id,
                user_id=TEST_USER_ID,
                status=TaskStatus.PENDING if i < 2 else TaskStatus.COMPLETED,
                quality_preset="balanced",
                files=[],
                created_at=now + timedelta(minutes=i),
                updated_at=now + timedelta(minutes=i),
            )

            task_dict = task.to_dict()
            task_dict["sk"] = "TASK"
            dynamodb_table.put_item(Item=task_dict)

        # Query by user_id
        response = dynamodb_table.query(
            IndexName="user-tasks-index",
            KeyConditionExpression="user_id = :uid",
            ExpressionAttributeValues={":uid": TEST_USER_ID},
        )

        assert response["Count"] == 3

    def test_query_tasks_by_status(self, dynamodb_table):
        """Test querying tasks by status using GSI."""
        now = datetime.now()

        # Create tasks with different statuses
        statuses = [TaskStatus.PENDING, TaskStatus.CONVERTING, TaskStatus.COMPLETED]
        for i, status in enumerate(statuses):
            task_id = str(uuid.uuid4())
            task = AsyncTask(
                task_id=task_id,
                user_id=TEST_USER_ID,
                status=status,
                quality_preset="balanced",
                files=[],
                created_at=now + timedelta(minutes=i),
                updated_at=now + timedelta(minutes=i),
            )

            task_dict = task.to_dict()
            task_dict["sk"] = "TASK"
            dynamodb_table.put_item(Item=task_dict)

        # Query by status
        response = dynamodb_table.query(
            IndexName="status-index",
            KeyConditionExpression="#status = :status",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": "PENDING"},
        )

        assert response["Count"] == 1

    def test_update_task_status(self, dynamodb_table):
        """Test updating task status in DynamoDB."""
        task_id = str(uuid.uuid4())
        now = datetime.now()

        # Create initial task
        task = AsyncTask(
            task_id=task_id,
            user_id=TEST_USER_ID,
            status=TaskStatus.PENDING,
            quality_preset="balanced",
            files=[],
            created_at=now,
            updated_at=now,
        )

        task_dict = task.to_dict()
        task_dict["sk"] = "TASK"
        dynamodb_table.put_item(Item=task_dict)

        # Update status
        dynamodb_table.update_item(
            Key={"task_id": task_id, "sk": "TASK"},
            UpdateExpression="SET #status = :status, updated_at = :updated",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "CONVERTING",
                ":updated": datetime.now().isoformat(),
            },
        )

        # Verify update
        response = dynamodb_table.get_item(Key={"task_id": task_id, "sk": "TASK"})
        assert response["Item"]["status"] == "CONVERTING"


class TestAsyncFileS3:
    """Integration tests for file operations with S3."""

    def test_upload_and_download_file(self, s3_bucket):
        """Test uploading and downloading file from S3."""
        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())
        s3_key = f"async/{task_id}/input/{file_id}/video.mp4"

        # Upload file
        content = b"fake video content"
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=s3_key, Body=content)

        # Download file
        response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=s3_key)
        downloaded = response["Body"].read()

        assert downloaded == content

    def test_upload_metadata_json(self, s3_bucket):
        """Test uploading metadata JSON to S3."""
        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())
        s3_key = f"async/{task_id}/input/{file_id}/metadata.json"

        metadata = {
            "capture_date": "2024-01-15T10:30:00",
            "location": [35.6762, 139.6503],
            "albums": ["Vacation", "2024"],
        }

        s3_bucket.put_object(
            Bucket=TEST_BUCKET,
            Key=s3_key,
            Body=json.dumps(metadata),
            ContentType="application/json",
        )

        # Download and verify
        response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=s3_key)
        loaded = json.loads(response["Body"].read())

        assert loaded["capture_date"] == metadata["capture_date"]
        assert loaded["location"] == metadata["location"]
        assert loaded["albums"] == metadata["albums"]

    def test_list_task_files(self, s3_bucket):
        """Test listing files for a task."""
        task_id = str(uuid.uuid4())

        # Upload multiple files
        for i in range(3):
            file_id = str(uuid.uuid4())
            s3_key = f"async/{task_id}/input/{file_id}/video{i}.mp4"
            s3_bucket.put_object(Bucket=TEST_BUCKET, Key=s3_key, Body=b"content")

        # List files
        response = s3_bucket.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f"async/{task_id}/input/")

        assert response["KeyCount"] == 3

    def test_delete_task_files(self, s3_bucket):
        """Test deleting all files for a task."""
        task_id = str(uuid.uuid4())

        # Upload files
        keys = []
        for i in range(3):
            file_id = str(uuid.uuid4())
            s3_key = f"async/{task_id}/input/{file_id}/video{i}.mp4"
            s3_bucket.put_object(Bucket=TEST_BUCKET, Key=s3_key, Body=b"content")
            keys.append(s3_key)

        # Delete files
        s3_bucket.delete_objects(Bucket=TEST_BUCKET, Delete={"Objects": [{"Key": k} for k in keys]})

        # Verify deletion
        response = s3_bucket.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f"async/{task_id}/")

        assert response.get("KeyCount", 0) == 0


class TestErrorHandlingIntegration:
    """Integration tests for error handling logic."""

    def test_mediaconvert_error_classification_flow(self):
        """Test MediaConvert error classification in workflow context."""
        # Simulate workflow receiving different errors
        test_cases = [
            # Transient errors - should retry
            (1517, True, "transient"),
            (1522, True, "transient"),
            (1550, True, "transient"),
            (1999, True, "transient"),
            # Configuration errors - should fail immediately
            (1010, False, "config_or_input"),
            (1030, False, "config_or_input"),
            (1040, False, "config_or_input"),
            # Permission errors
            (1401, False, "permission"),
            # Unknown errors
            (9999, False, "unknown"),
        ]

        for error_code, expected_retryable, expected_category in test_cases:
            result = classify_mediaconvert_error(error_code)
            assert result.is_retryable == expected_retryable, (
                f"Error {error_code} retryable should be {expected_retryable}"
            )
            assert result.category == expected_category, (
                f"Error {error_code} category should be {expected_category}"
            )

    def test_ssim_retry_flow(self):
        """Test SSIM retry logic in workflow context."""
        # Adaptive presets should retry with next preset when SSIM is low
        adaptive_presets = ["balanced+", "high+"]
        for preset in adaptive_presets:
            result = determine_ssim_action(preset, 0.89)
            # balanced+ should retry, high+ should fail (end of chain)
            if preset == "balanced+":
                assert result.action == "retry_with_higher_preset"
                assert result.next_preset == "high"
            else:
                assert result.action == "fail"

        # Non-adaptive presets should not retry
        non_adaptive_presets = ["balanced", "high", "compression"]
        for preset in non_adaptive_presets:
            result = determine_ssim_action(preset, 0.89)
            assert result.action == "fail", f"Preset {preset} should fail"

        # High SSIM should always accept
        for preset in ["balanced", "balanced+", "high", "high+"]:
            result = determine_ssim_action(preset, 0.98)
            assert result.action == "accept", f"High SSIM should accept for {preset}"


class TestTaskStatusAggregation:
    """Integration tests for task status aggregation."""

    def test_aggregate_status_all_completed(self, dynamodb_table):
        """Test status aggregation when all files complete."""
        task_id = str(uuid.uuid4())
        now = datetime.now()

        files = [
            AsyncFile(
                file_id=str(uuid.uuid4()),
                uuid=f"uuid-{i}",
                filename=f"video{i}.mp4",
                source_s3_key=f"async/{task_id}/input/video{i}.mp4",
                status=FileStatus.COMPLETED,
                source_size_bytes=1000000,
            )
            for i in range(3)
        ]

        task = AsyncTask(
            task_id=task_id,
            user_id=TEST_USER_ID,
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=files,
            created_at=now,
            updated_at=now,
        )

        # Aggregate status - pass list of FileStatus
        file_statuses = [f.status for f in task.files]
        new_status = aggregate_task_status(file_statuses)
        assert new_status == TaskStatus.COMPLETED

    def test_aggregate_status_partial_failure(self, dynamodb_table):
        """Test status aggregation with partial failure."""
        task_id = str(uuid.uuid4())
        now = datetime.now()

        files = [
            AsyncFile(
                file_id=str(uuid.uuid4()),
                uuid="uuid-1",
                filename="video1.mp4",
                source_s3_key=f"async/{task_id}/input/video1.mp4",
                status=FileStatus.COMPLETED,
                source_size_bytes=1000000,
            ),
            AsyncFile(
                file_id=str(uuid.uuid4()),
                uuid="uuid-2",
                filename="video2.mp4",
                source_s3_key=f"async/{task_id}/input/video2.mp4",
                status=FileStatus.FAILED,
                source_size_bytes=1000000,
                error_message="Conversion failed",
            ),
        ]

        task = AsyncTask(
            task_id=task_id,
            user_id=TEST_USER_ID,
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=files,
            created_at=now,
            updated_at=now,
        )

        # Aggregate status - pass list of FileStatus
        file_statuses = [f.status for f in task.files]
        new_status = aggregate_task_status(file_statuses)
        assert new_status == TaskStatus.PARTIALLY_COMPLETED

    def test_aggregate_status_all_failed(self, dynamodb_table):
        """Test status aggregation when all files fail."""
        task_id = str(uuid.uuid4())
        now = datetime.now()

        files = [
            AsyncFile(
                file_id=str(uuid.uuid4()),
                uuid=f"uuid-{i}",
                filename=f"video{i}.mp4",
                source_s3_key=f"async/{task_id}/input/video{i}.mp4",
                status=FileStatus.FAILED,
                source_size_bytes=1000000,
                error_message="Conversion failed",
            )
            for i in range(3)
        ]

        task = AsyncTask(
            task_id=task_id,
            user_id=TEST_USER_ID,
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=files,
            created_at=now,
            updated_at=now,
        )

        # Aggregate status - pass list of FileStatus
        file_statuses = [f.status for f in task.files]
        new_status = aggregate_task_status(file_statuses)
        assert new_status == TaskStatus.FAILED


class TestEndToEndWorkflow:
    """End-to-end workflow tests."""

    def test_complete_workflow_simulation(self, dynamodb_table, s3_bucket):
        """Simulate complete async workflow."""
        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())
        now = datetime.now()

        # Step 1: Create task (PENDING)
        task = AsyncTask(
            task_id=task_id,
            user_id=TEST_USER_ID,
            status=TaskStatus.PENDING,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id=file_id,
                    uuid="photo-uuid-1",
                    filename="video.mp4",
                    source_s3_key=f"async/{task_id}/input/{file_id}/video.mp4",
                    status=FileStatus.PENDING,
                    source_size_bytes=1000000,
                )
            ],
            created_at=now,
            updated_at=now,
        )

        task_dict = task.to_dict()
        task_dict["sk"] = "TASK"
        dynamodb_table.put_item(Item=task_dict)

        # Step 2: Upload source file
        s3_bucket.put_object(
            Bucket=TEST_BUCKET,
            Key=f"async/{task_id}/input/{file_id}/video.mp4",
            Body=b"source video content",
        )

        # Step 3: Update to UPLOADING
        dynamodb_table.update_item(
            Key={"task_id": task_id, "sk": "TASK"},
            UpdateExpression="SET #status = :status",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": "UPLOADING"},
        )

        # Step 4: Update to CONVERTING
        dynamodb_table.update_item(
            Key={"task_id": task_id, "sk": "TASK"},
            UpdateExpression="SET #status = :status",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": "CONVERTING"},
        )

        # Step 5: Upload output file
        s3_bucket.put_object(
            Bucket=TEST_BUCKET,
            Key=f"async/{task_id}/output/{file_id}/video.mp4",
            Body=b"converted video content",
        )

        # Step 6: Update to COMPLETED
        dynamodb_table.update_item(
            Key={"task_id": task_id, "sk": "TASK"},
            UpdateExpression="SET #status = :status, completed_at = :completed",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "COMPLETED",
                ":completed": datetime.now().isoformat(),
            },
        )

        # Verify final state
        response = dynamodb_table.get_item(Key={"task_id": task_id, "sk": "TASK"})
        assert response["Item"]["status"] == "COMPLETED"
        assert "completed_at" in response["Item"]

        # Verify output file exists
        output_response = s3_bucket.list_objects_v2(
            Bucket=TEST_BUCKET, Prefix=f"async/{task_id}/output/"
        )
        assert output_response["KeyCount"] == 1

    def test_cancellation_workflow(self, dynamodb_table, s3_bucket):
        """Test task cancellation workflow."""
        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())
        now = datetime.now()

        # Create task in CONVERTING state
        task = AsyncTask(
            task_id=task_id,
            user_id=TEST_USER_ID,
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id=file_id,
                    uuid="photo-uuid-1",
                    filename="video.mp4",
                    source_s3_key=f"async/{task_id}/input/{file_id}/video.mp4",
                    status=FileStatus.CONVERTING,
                    source_size_bytes=1000000,
                )
            ],
            created_at=now,
            updated_at=now,
        )

        task_dict = task.to_dict()
        task_dict["sk"] = "TASK"
        dynamodb_table.put_item(Item=task_dict)

        # Upload source file
        s3_bucket.put_object(
            Bucket=TEST_BUCKET,
            Key=f"async/{task_id}/input/{file_id}/video.mp4",
            Body=b"source video content",
        )

        # Cancel task
        dynamodb_table.update_item(
            Key={"task_id": task_id, "sk": "TASK"},
            UpdateExpression="SET #status = :status",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": "CANCELLED"},
        )

        # Clean up S3 files
        s3_bucket.delete_object(
            Bucket=TEST_BUCKET, Key=f"async/{task_id}/input/{file_id}/video.mp4"
        )

        # Verify cancellation
        response = dynamodb_table.get_item(Key={"task_id": task_id, "sk": "TASK"})
        assert response["Item"]["status"] == "CANCELLED"

        # Verify S3 cleanup
        s3_response = s3_bucket.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f"async/{task_id}/")
        assert s3_response.get("KeyCount", 0) == 0
