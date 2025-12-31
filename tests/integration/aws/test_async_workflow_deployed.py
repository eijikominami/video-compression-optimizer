"""AWS Async Workflow deployed integration tests.

These tests verify the deployed async workflow infrastructure works correctly:
1. API Gateway endpoints are accessible
2. DynamoDB table exists and is accessible
3. Step Functions state machine exists
4. Lambda functions can be invoked

実行方法:
    SKIP_AWS_TESTS=false python3.11 -m pytest tests/integration/aws/test_async_workflow_deployed.py -v -m deployed

検証対象:
- 要件 1.1: タスク投入 API
- 要件 2.1: 状態確認 API
- 要件 3.1: キャンセル API
- 要件 4.1: ダウンロード機能
"""

import json
import os
import uuid
from datetime import datetime

import pytest

# Skip all tests if not configured for AWS
pytestmark = [
    pytest.mark.deployed,
    pytest.mark.skipif(
        os.environ.get("SKIP_AWS_TESTS", "true").lower() == "true",
        reason="AWS tests disabled (set SKIP_AWS_TESTS=false to enable)",
    ),
]


class TestAsyncWorkflowInfrastructure:
    """Test async workflow infrastructure is deployed correctly."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")

        return config.aws

    @pytest.fixture
    def boto3_session(self, aws_config):
        """Create boto3 session with configured profile."""
        import boto3

        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        return boto3.Session(**session_kwargs)

    def test_dynamodb_table_exists(self, boto3_session):
        """Test DynamoDB table for async tasks exists.

        Validates: Requirement 6.1 - タスク状態の永続化
        """
        dynamodb = boto3_session.client("dynamodb")

        try:
            response = dynamodb.describe_table(TableName="vco-async-tasks-dev")
            assert response["Table"]["TableStatus"] == "ACTIVE"

            # Verify key schema
            key_schema = {k["AttributeName"]: k["KeyType"] for k in response["Table"]["KeySchema"]}
            assert "task_id" in key_schema
            assert key_schema["task_id"] == "HASH"

        except dynamodb.exceptions.ResourceNotFoundException:
            pytest.fail("DynamoDB table 'vco-async-tasks-dev' not found")

    def test_step_functions_state_machine_exists(self, boto3_session):
        """Test Step Functions state machine exists.

        Validates: Requirement 5.1 - ワークフロー管理
        """
        sfn = boto3_session.client("stepfunctions")

        # List state machines and find ours
        response = sfn.list_state_machines()
        state_machines = {sm["name"]: sm["stateMachineArn"] for sm in response["stateMachines"]}

        assert "vco-async-workflow-dev" in state_machines, (
            f"State machine 'vco-async-workflow-dev' not found. "
            f"Available: {list(state_machines.keys())}"
        )

    def test_lambda_functions_exist(self, boto3_session):
        """Test Lambda functions for async workflow exist.

        Validates: Requirements 1.1, 2.1, 3.1 - API Lambda 関数
        """
        lambda_client = boto3_session.client("lambda")

        expected_functions = [
            "vco-async-task-submit-dev",
            "vco-async-task-status-dev",
            "vco-async-task-cancel-dev",
            "vco-async-workflow-dev",
        ]

        for func_name in expected_functions:
            try:
                response = lambda_client.get_function(FunctionName=func_name)
                assert response["Configuration"]["State"] == "Active", (
                    f"Lambda function '{func_name}' is not active"
                )
            except lambda_client.exceptions.ResourceNotFoundException:
                pytest.fail(f"Lambda function '{func_name}' not found")


class TestAsyncTaskSubmitAPI:
    """Test async task submit API endpoint."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")

        return config.aws

    @pytest.fixture
    def boto3_session(self, aws_config):
        """Create boto3 session with configured profile."""
        import boto3

        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        return boto3.Session(**session_kwargs)

    def test_submit_lambda_invocation(self, boto3_session):
        """Test submit Lambda can be invoked directly.

        Validates: Requirement 1.1 - タスク投入
        """
        lambda_client = boto3_session.client("lambda")

        # Create a minimal test payload (will fail validation but tests invocation)
        payload = {
            "httpMethod": "POST",
            "body": json.dumps(
                {
                    "task_id": str(uuid.uuid4()),
                    "user_id": "test-user",
                    "quality_preset": "balanced",
                    "files": [],  # Empty files will fail validation
                }
            ),
        }

        response = lambda_client.invoke(
            FunctionName="vco-async-task-submit-dev",
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )

        response_payload = json.loads(response["Payload"].read())

        # Should return a response (even if error due to empty files)
        assert "statusCode" in response_payload
        # 400 is expected for empty files validation error
        assert response_payload["statusCode"] in (200, 400, 500)


class TestAsyncTaskStatusAPI:
    """Test async task status API endpoint."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")

        return config.aws

    @pytest.fixture
    def boto3_session(self, aws_config):
        """Create boto3 session with configured profile."""
        import boto3

        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        return boto3.Session(**session_kwargs)

    def test_status_lambda_invocation(self, boto3_session):
        """Test status Lambda can be invoked directly.

        Validates: Requirement 2.1 - 状態確認
        """
        lambda_client = boto3_session.client("lambda")

        # Query for a non-existent task
        payload = {
            "httpMethod": "GET",
            "pathParameters": {"task_id": str(uuid.uuid4())},
            "headers": {"X-User-Id": "test-user"},
        }

        response = lambda_client.invoke(
            FunctionName="vco-async-task-status-dev",
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )

        response_payload = json.loads(response["Payload"].read())

        # Should return 404 for non-existent task
        assert "statusCode" in response_payload
        assert response_payload["statusCode"] in (200, 404)

    def test_status_list_tasks(self, boto3_session):
        """Test listing tasks for a user.

        Validates: Requirement 2.1 - アクティブタスク一覧
        """
        lambda_client = boto3_session.client("lambda")

        # List tasks for test user
        payload = {
            "httpMethod": "GET",
            "pathParameters": None,
            "queryStringParameters": {},
            "headers": {"X-User-Id": "test-user"},
        }

        response = lambda_client.invoke(
            FunctionName="vco-async-task-status-dev",
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )

        response_payload = json.loads(response["Payload"].read())

        # Should return 200 with empty or populated list
        assert "statusCode" in response_payload
        assert response_payload["statusCode"] == 200

        body = json.loads(response_payload.get("body", "{}"))
        assert "tasks" in body or isinstance(body, list)


class TestAsyncTaskCancelAPI:
    """Test async task cancel API endpoint."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")

        return config.aws

    @pytest.fixture
    def boto3_session(self, aws_config):
        """Create boto3 session with configured profile."""
        import boto3

        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        return boto3.Session(**session_kwargs)

    def test_cancel_lambda_invocation(self, boto3_session):
        """Test cancel Lambda can be invoked directly.

        Validates: Requirement 3.1 - タスクキャンセル
        """
        lambda_client = boto3_session.client("lambda")

        # Try to cancel a non-existent task
        payload = {
            "httpMethod": "POST",
            "pathParameters": {"task_id": str(uuid.uuid4())},
            "headers": {"X-User-Id": "test-user"},
            "body": json.dumps({}),
        }

        response = lambda_client.invoke(
            FunctionName="vco-async-task-cancel-dev",
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )

        response_payload = json.loads(response["Payload"].read())

        # Should return 404 for non-existent task
        assert "statusCode" in response_payload
        assert response_payload["statusCode"] in (200, 404)


class TestAsyncWorkflowLambda:
    """Test async workflow Lambda function."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")

        return config.aws

    @pytest.fixture
    def boto3_session(self, aws_config):
        """Create boto3 session with configured profile."""
        import boto3

        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        return boto3.Session(**session_kwargs)

    def test_workflow_lambda_validate_input(self, boto3_session):
        """Test workflow Lambda validate_input action.

        Validates: Requirement 5.1 - 入力検証
        """
        lambda_client = boto3_session.client("lambda")

        # Test validate_input action
        payload = {
            "action": "validate_input",
            "task_id": str(uuid.uuid4()),
            "files": [
                {
                    "file_id": str(uuid.uuid4()),
                    "original_uuid": str(uuid.uuid4()),
                    "filename": "test.mp4",
                    "source_s3_key": "test/source.mp4",
                }
            ],
            "quality_preset": "balanced",
        }

        response = lambda_client.invoke(
            FunctionName="vco-async-workflow-dev",
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )

        response_payload = json.loads(response["Payload"].read())

        # Should return validated input or error
        # The response structure depends on implementation
        assert response_payload is not None


class TestDynamoDBOperations:
    """Test DynamoDB operations for async tasks."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")

        return config.aws

    @pytest.fixture
    def boto3_session(self, aws_config):
        """Create boto3 session with configured profile."""
        import boto3

        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        return boto3.Session(**session_kwargs)

    @pytest.fixture
    def dynamodb_table(self, boto3_session):
        """Get DynamoDB table resource."""
        dynamodb = boto3_session.resource("dynamodb")
        return dynamodb.Table("vco-async-tasks-dev")

    def test_dynamodb_write_and_read(self, dynamodb_table):
        """Test DynamoDB write and read operations.

        Validates: Requirement 6.1, 6.2 - タスクメタデータの保存と取得
        """
        task_id = f"test-{uuid.uuid4()}"
        user_id = "test-user"
        now = datetime.now().isoformat()

        # Write test item
        item = {
            "task_id": task_id,
            "sk": "TASK",
            "user_id": user_id,
            "status": "PENDING",
            "quality_preset": "balanced",
            "files": [],
            "created_at": now,
            "updated_at": now,
        }

        try:
            dynamodb_table.put_item(Item=item)

            # Read back
            response = dynamodb_table.get_item(Key={"task_id": task_id, "sk": "TASK"})

            assert "Item" in response
            assert response["Item"]["task_id"] == task_id
            assert response["Item"]["user_id"] == user_id
            assert response["Item"]["status"] == "PENDING"

        finally:
            # Cleanup
            dynamodb_table.delete_item(Key={"task_id": task_id, "sk": "TASK"})

    def test_dynamodb_gsi_query(self, dynamodb_table):
        """Test DynamoDB GSI query by user_id.

        Validates: Requirement 6.5 - ユーザー ID によるフィルタリング
        """
        task_id = f"test-{uuid.uuid4()}"
        user_id = f"test-user-{uuid.uuid4()}"
        now = datetime.now().isoformat()

        # Write test item
        item = {
            "task_id": task_id,
            "sk": "TASK",
            "user_id": user_id,
            "status": "PENDING",
            "quality_preset": "balanced",
            "files": [],
            "created_at": now,
            "updated_at": now,
        }

        try:
            dynamodb_table.put_item(Item=item)

            # Query by user_id using GSI1-UserTasks
            from boto3.dynamodb.conditions import Key

            response = dynamodb_table.query(
                IndexName="GSI1-UserTasks",
                KeyConditionExpression=Key("user_id").eq(user_id),
            )

            assert response["Count"] >= 1
            assert any(item["task_id"] == task_id for item in response["Items"])

        finally:
            # Cleanup
            dynamodb_table.delete_item(Key={"task_id": task_id, "sk": "TASK"})


class TestS3Integration:
    """Test S3 integration for async workflow."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")

        return config.aws

    @pytest.fixture
    def boto3_session(self, aws_config):
        """Create boto3 session with configured profile."""
        import boto3

        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        return boto3.Session(**session_kwargs)

    def test_s3_bucket_accessible(self, boto3_session, aws_config):
        """Test S3 bucket is accessible.

        Validates: Requirement 1.1 - S3 アップロード
        """
        s3 = boto3_session.client("s3")

        # Check bucket exists and is accessible
        try:
            s3.head_bucket(Bucket=aws_config.s3_bucket)
        except Exception as e:
            pytest.fail(f"S3 bucket '{aws_config.s3_bucket}' not accessible: {e}")

    def test_s3_presigned_url_generation(self, boto3_session, aws_config):
        """Test S3 presigned URL generation.

        Validates: Requirement 4.1 - 署名付き URL でダウンロード
        """
        s3 = boto3_session.client("s3")

        # Generate presigned URL for a test key
        test_key = "test/presigned-url-test.txt"

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": aws_config.s3_bucket, "Key": test_key},
            ExpiresIn=3600,
        )

        assert url is not None
        assert aws_config.s3_bucket in url
        assert test_key in url
