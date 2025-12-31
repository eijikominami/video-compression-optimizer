# Video Compression Optimizer - Architecture Documentation

## Overview

Video Compression Optimizer (VCO) は、Apple Photos の動画を H.265 形式に変換してストレージ容量を削減するツールです。同期処理と非同期処理の両方をサポートし、AWS クラウドサービスを活用した高品質な動画変換を提供します。

## C4 Model Level 1: System Context

```mermaid
C4Context
    title System Context Diagram for Video Compression Optimizer

    Person(user, "User", "macOS user who wants to optimize video storage")
    System(vco, "Video Compression Optimizer", "CLI tool for video conversion and optimization")
    
    System_Ext(photos, "Apple Photos", "macOS Photos app with video library")
    System_Ext(aws, "AWS Cloud", "Cloud services for video processing")
    System_Ext(icloud, "iCloud", "Apple's cloud storage service")

    Rel(user, vco, "Uses CLI commands", "Terminal")
    Rel(vco, photos, "Reads video metadata", "osxphotos/photoscript")
    Rel(vco, aws, "Processes videos", "AWS SDK")
    Rel(photos, icloud, "Syncs videos", "iCloud Photos")
    
    UpdateElementStyle(vco, $fontColor="white", $bgColor="blue")
    UpdateElementStyle(aws, $fontColor="white", $bgColor="orange")
```

### External Dependencies

| System | Purpose | Interface |
|--------|---------|-----------|
| **Apple Photos** | Video library access | osxphotos library |
| **AWS Cloud** | Video processing infrastructure | AWS SDK (boto3) |
| **iCloud** | Video storage and sync | Photos app integration |

## C4 Model Level 2: Container Diagram

```mermaid
C4Container
    title Container Diagram for Video Compression Optimizer

    Person(user, "User", "macOS user")
    
    Container_Boundary(vco_system, "Video Compression Optimizer") {
        Container(cli, "VCO CLI", "Python", "Command-line interface for video operations")
        Container(config, "Configuration", "Python", "Quality presets and settings")
        Container(models, "Data Models", "Python", "AsyncTask, AsyncFile data structures")
    }
    
    Container_Boundary(aws_cloud, "AWS Cloud") {
        Container(api_gw, "API Gateway", "REST API", "HTTP endpoints for async operations")
        Container(lambda_submit, "Task Submit Lambda", "Python", "Creates conversion tasks")
        Container(lambda_status, "Task Status Lambda", "Python", "Queries task progress")
        Container(lambda_workflow, "Workflow Lambda", "Python", "Orchestrates conversion process")
        Container(step_functions, "Step Functions", "State Machine", "Async workflow orchestration")
        Container(mediaconvert, "MediaConvert", "AWS Service", "Video transcoding service")
        Container(s3, "S3 Storage", "Object Storage", "Video files and metadata")
        Container(dynamodb, "DynamoDB", "NoSQL Database", "Task and file status tracking")
        Container(quality_lambda, "Quality Checker", "Python + FFmpeg", "SSIM-based quality verification")
    }
    
    System_Ext(photos, "Apple Photos", "Video library")

    Rel(user, cli, "Executes commands", "Terminal")
    Rel(cli, photos, "Scans videos", "osxphotos")
    Rel(cli, config, "Uses presets", "Import")
    Rel(cli, models, "Creates objects", "Import")
    
    Rel(cli, api_gw, "HTTP requests", "AWS SigV4")
    Rel(api_gw, lambda_submit, "POST /tasks", "JSON")
    Rel(api_gw, lambda_status, "GET /tasks/{id}", "JSON")
    
    Rel(lambda_submit, dynamodb, "Creates records", "AWS SDK")
    Rel(lambda_submit, s3, "Generates presigned URLs", "AWS SDK")
    Rel(lambda_submit, step_functions, "Starts execution", "AWS SDK")
    
    Rel(step_functions, lambda_workflow, "Invokes", "JSON")
    Rel(lambda_workflow, mediaconvert, "Creates jobs", "AWS SDK")
    Rel(lambda_workflow, quality_lambda, "Invokes", "JSON")
    Rel(lambda_workflow, dynamodb, "Updates status", "AWS SDK")
    
    Rel(mediaconvert, s3, "Reads/writes files", "S3 API")
    Rel(quality_lambda, s3, "Downloads files", "S3 API")
    
    Rel(lambda_status, dynamodb, "Queries status", "AWS SDK")
    
    UpdateElementStyle(cli, $fontColor="white", $bgColor="blue")
    UpdateElementStyle(api_gw, $fontColor="white", $bgColor="orange")
```

### Container Responsibilities

| Container | Primary Responsibility | Technology Stack |
|-----------|----------------------|------------------|
| **VCO CLI** | User interface, local operations | Python 3.11, Rich, Click |
| **API Gateway** | HTTP endpoint management | AWS API Gateway, IAM auth |
| **Lambda Functions** | Serverless business logic | Python 3.11, boto3 |
| **Step Functions** | Workflow orchestration | AWS Step Functions |
| **MediaConvert** | Video transcoding | AWS MediaConvert |
| **S3 Storage** | File storage | AWS S3 |
| **DynamoDB** | State management | AWS DynamoDB |

## Data Flow Diagrams

### Synchronous Conversion Flow

```mermaid
sequenceDiagram
    participant User
    participant CLI
    participant Photos as Apple Photos
    participant MC as MediaConvert
    participant QC as Quality Checker
    participant S3

    User->>CLI: vco convert
    CLI->>Photos: Scan videos
    Photos-->>CLI: Video metadata
    CLI->>S3: Upload video
    CLI->>MC: Create job
    MC->>S3: Process video
    MC-->>CLI: Job complete
    CLI->>QC: Verify quality
    QC->>S3: Download files
    QC-->>CLI: SSIM score
    CLI->>S3: Download result
    CLI-->>User: Conversion complete
```

### Asynchronous Conversion Flow

```mermaid
sequenceDiagram
    participant User
    participant CLI
    participant API as API Gateway
    participant Submit as Submit Lambda
    participant SF as Step Functions
    participant Workflow as Workflow Lambda
    participant MC as MediaConvert
    participant QC as Quality Checker
    participant Status as Status Lambda
    participant DB as DynamoDB
    participant S3

    User->>CLI: vco convert --async
    CLI->>API: POST /tasks
    API->>Submit: Create task
    Submit->>DB: Store task
    Submit->>S3: Generate presigned URLs
    Submit-->>CLI: Task ID + URLs
    CLI->>S3: Upload files
    CLI->>SF: Start execution
    
    SF->>Workflow: Process files
    Workflow->>MC: Create jobs
    MC->>S3: Process videos
    MC-->>Workflow: Job complete
    Workflow->>QC: Verify quality
    QC-->>Workflow: SSIM results
    Workflow->>DB: Update status
    
    User->>CLI: vco status <task-id>
    CLI->>API: GET /tasks/{id}
    API->>Status: Get status
    Status->>DB: Query task
    Status-->>CLI: Progress info
    
    User->>CLI: vco download <task-id>
    CLI->>S3: Download files
    CLI->>API: POST /tasks/{id}/download-status
    API->>Status: Update download status
    Status->>DB: Mark downloaded
```

## AWS Service Architecture

### Infrastructure Components

```mermaid
graph TB
    subgraph "User Environment"
        CLI[VCO CLI]
    end
    
    subgraph "AWS Account: 857135586997"
        subgraph "API Layer"
            APIGW[API Gateway<br/>vco-async-api-dev]
        end
        
        subgraph "Compute Layer"
            SUBMIT[async-task-submit<br/>Lambda]
            STATUS[async-task-status<br/>Lambda]
            WORKFLOW[async-workflow<br/>Lambda]
            QUALITY[quality-checker<br/>Lambda]
            SF[Step Functions<br/>State Machine]
        end
        
        subgraph "Storage Layer"
            S3[S3 Bucket<br/>vco-video-storage-857135586997]
            DDB[DynamoDB<br/>vco-async-tasks-dev]
        end
        
        subgraph "Processing Layer"
            MC[MediaConvert<br/>Transcoding Service]
        end
        
        subgraph "Monitoring"
            CW[CloudWatch<br/>Logs & Metrics]
        end
    end

    CLI -->|HTTPS/SigV4| APIGW
    APIGW --> SUBMIT
    APIGW --> STATUS
    
    SUBMIT --> DDB
    SUBMIT --> S3
    SUBMIT --> SF
    
    SF --> WORKFLOW
    WORKFLOW --> MC
    WORKFLOW --> QUALITY
    WORKFLOW --> DDB
    
    STATUS --> DDB
    
    MC --> S3
    QUALITY --> S3
    
    SUBMIT --> CW
    STATUS --> CW
    WORKFLOW --> CW
    QUALITY --> CW
```

### Resource Configuration

| Resource | Configuration | Purpose |
|----------|---------------|---------|
| **S3 Bucket** | `vco-video-storage-857135586997` | Video file storage with lifecycle rules |
| **DynamoDB Table** | `vco-async-tasks-dev` | Task state with TTL (90 days) |
| **Lambda Memory** | 256MB - 10GB | Based on processing requirements |
| **API Gateway** | REST API with IAM auth | Secure HTTP endpoints |
| **Step Functions** | Express workflow | Fast async orchestration |

## Security Architecture

### Authentication & Authorization

```mermaid
graph LR
    subgraph "Client Side"
        CLI[VCO CLI]
        CREDS[AWS Credentials<br/>~/.aws/credentials]
    end
    
    subgraph "AWS IAM"
        ROLE[MediaConvert Role<br/>vco-mediaconvert-role-dev]
        POLICY[IAM Policies]
    end
    
    subgraph "AWS Services"
        APIGW[API Gateway]
        LAMBDA[Lambda Functions]
        S3[S3 Bucket]
        DDB[DynamoDB]
        MC[MediaConvert]
    end

    CLI --> CREDS
    CREDS --> APIGW
    APIGW -->|SigV4| LAMBDA
    LAMBDA --> ROLE
    ROLE --> POLICY
    POLICY --> S3
    POLICY --> DDB
    POLICY --> MC
```

### Security Boundaries

| Boundary | Protection Method | Scope |
|----------|------------------|-------|
| **Network** | HTTPS/TLS 1.2+ | All API communications |
| **Authentication** | AWS SigV4 | API Gateway endpoints |
| **Authorization** | IAM roles/policies | Service-to-service access |
| **Data Encryption** | AES-256 | S3 objects, DynamoDB records |
| **Access Control** | Least privilege | Lambda execution roles |

## Deployment Architecture

### Environment Configuration

```mermaid
graph TB
    subgraph "Development Environment"
        DEV_CLI[VCO CLI<br/>Local Development]
        DEV_AWS[AWS Stack<br/>vco-infrastructure-dev]
    end
    
    subgraph "Production Environment"
        PROD_CLI[VCO CLI<br/>User Installation]
        PROD_AWS[AWS Stack<br/>vco-infrastructure-prod]
    end
    
    subgraph "Shared Resources"
        GITHUB[GitHub Repository<br/>Source Code]
        PYPI[PyPI Package<br/>Distribution]
    end

    GITHUB -->|CI/CD| DEV_AWS
    GITHUB -->|Release| PYPI
    PYPI -->|pip install| PROD_CLI
    
    DEV_CLI --> DEV_AWS
    PROD_CLI --> PROD_AWS
```

### Deployment Process

| Stage | Tool | Target | Validation |
|-------|------|--------|------------|
| **Build** | SAM CLI | Local | Unit tests, linting |
| **Deploy** | SAM Deploy | AWS CloudFormation | Integration tests |
| **Package** | pip/setuptools | PyPI | End-to-end tests |
| **Monitor** | CloudWatch | Production | Health checks |

## Performance & Scalability

### Processing Capacity

| Component | Limit | Scaling Method |
|-----------|-------|----------------|
| **API Gateway** | 10,000 RPS | Auto-scaling |
| **Lambda Concurrent** | 1,000 executions | Reserved concurrency |
| **MediaConvert** | 20 concurrent jobs | Queue management |
| **S3 Transfer** | 5GB per file | Multipart upload |
| **DynamoDB** | On-demand | Auto-scaling |

### Quality Presets Performance

| Preset | Processing Time | Quality Score | Use Case |
|--------|----------------|---------------|----------|
| **compression** | ~0.3x duration | SSIM ≥ 0.85 | Maximum compression |
| **balanced** | ~0.5x duration | SSIM ≥ 0.95 | Recommended default |
| **high** | ~0.8x duration | SSIM ≥ 0.98 | Quality priority |
| **balanced+** | ~0.8x duration | Best effort | Adaptive quality |

## Monitoring & Observability

### CloudWatch Metrics

```mermaid
graph TB
    subgraph "Application Metrics"
        TASK_COUNT[Task Count]
        SUCCESS_RATE[Success Rate]
        PROCESSING_TIME[Processing Time]
        QUALITY_SCORE[Quality Scores]
    end
    
    subgraph "Infrastructure Metrics"
        LAMBDA_DURATION[Lambda Duration]
        API_LATENCY[API Latency]
        S3_REQUESTS[S3 Requests]
        DDB_THROTTLES[DynamoDB Throttles]
    end
    
    subgraph "Business Metrics"
        STORAGE_SAVED[Storage Saved]
        CONVERSION_RATIO[Compression Ratio]
        USER_ADOPTION[User Adoption]
    end

    TASK_COUNT --> LAMBDA_DURATION
    SUCCESS_RATE --> API_LATENCY
    PROCESSING_TIME --> S3_REQUESTS
    QUALITY_SCORE --> DDB_THROTTLES
```

### Log Aggregation

| Service | Log Level | Retention | Purpose |
|---------|-----------|-----------|---------|
| **Lambda Functions** | INFO | 30 days | Execution traces |
| **API Gateway** | ERROR | 30 days | Request/response logs |
| **Step Functions** | ALL | 30 days | Workflow execution |
| **MediaConvert** | INFO | 7 days | Job status |

## Disaster Recovery

### Backup Strategy

| Component | Backup Method | RTO | RPO |
|-----------|---------------|-----|-----|
| **DynamoDB** | Point-in-time recovery | < 1 hour | < 1 minute |
| **S3 Objects** | Cross-region replication | < 15 minutes | < 1 minute |
| **Lambda Code** | Version control | < 30 minutes | 0 (immutable) |
| **Configuration** | Infrastructure as Code | < 1 hour | 0 (version controlled) |

### Recovery Procedures

1. **Service Outage**: Automatic failover to backup region
2. **Data Corruption**: Point-in-time recovery from DynamoDB
3. **Code Issues**: Rollback to previous Lambda version
4. **Infrastructure**: Redeploy from CloudFormation template

## Cost Optimization

### Resource Utilization

| Service | Cost Driver | Optimization Strategy |
|---------|-------------|----------------------|
| **MediaConvert** | Processing minutes | Batch processing, quality presets |
| **Lambda** | Execution time | Memory optimization, cold start reduction |
| **S3** | Storage + requests | Lifecycle policies, intelligent tiering |
| **DynamoDB** | Read/write capacity | On-demand billing, TTL cleanup |

### Estimated Monthly Costs

| Usage Scenario | MediaConvert | Lambda | S3 | DynamoDB | Total |
|----------------|--------------|--------|----|---------|----|
| **Light (10 videos/month)** | $2 | $1 | $1 | $1 | $5 |
| **Medium (100 videos/month)** | $20 | $5 | $5 | $3 | $33 |
| **Heavy (1000 videos/month)** | $200 | $25 | $25 | $10 | $260 |

## Future Architecture Considerations

### Planned Enhancements

1. **Multi-region Support**: Deploy to multiple AWS regions for global users
2. **Batch Processing**: Optimize for large-scale video processing
3. **Real-time Notifications**: WebSocket support for live progress updates
4. **Advanced Analytics**: Machine learning for quality prediction
5. **Mobile Support**: iOS/Android companion apps

### Scalability Roadmap

| Phase | Target Scale | Key Changes |
|-------|-------------|-------------|
| **Phase 1** | 1K users | Current architecture |
| **Phase 2** | 10K users | Multi-region deployment |
| **Phase 3** | 100K users | Microservices architecture |
| **Phase 4** | 1M users | Event-driven architecture |

---

## References

- [AWS Well-Architected Framework](https://aws.amazon.com/architecture/well-architected/)
- [C4 Model Documentation](https://c4model.com/)
- [AWS MediaConvert User Guide](https://docs.aws.amazon.com/mediaconvert/)
- [Project Repository](https://github.com/eijikominami/video-compression-optimizer)
