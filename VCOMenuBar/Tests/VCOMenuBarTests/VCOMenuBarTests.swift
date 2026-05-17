import XCTest
@testable import VCOMenuBar

final class ModelsTests: XCTestCase {
    func testPipelineStateRoundtrip() throws {
        let state = PipelineState(
            stage: .polling,
            taskIds: ["task-123"],
            files: [FileState(filename: "video.mov", status: .processing, progressPercentage: 50, errorReason: nil)],
            lastUpdated: Date(timeIntervalSince1970: 1700000000),
            topNUsed: 3,
            lastPolledStatus: "PROGRESSING",
            lastStatusChangeTime: Date(timeIntervalSince1970: 1700000000)
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(state)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(PipelineState.self, from: data)
        XCTAssertEqual(decoded.stage, .polling)
        XCTAssertEqual(decoded.taskIds, ["task-123"])
        XCTAssertEqual(decoded.files.count, 1)
        XCTAssertEqual(decoded.files[0].filename, "video.mov")
        XCTAssertEqual(decoded.files[0].status, .processing)
        XCTAssertEqual(decoded.files[0].progressPercentage, 50)
        XCTAssertEqual(decoded.topNUsed, 3)
    }

    func testConvertResponseDecode() throws {
        let json = """
        {"task_id": "abc-123", "file_count": 5, "status": "submitted"}
        """.data(using: .utf8)!
        let response = try JSONDecoder().decode(ConvertResponse.self, from: json)
        XCTAssertEqual(response.taskId, "abc-123")
        XCTAssertEqual(response.fileCount, 5)
        XCTAssertEqual(response.status, "submitted")
    }

    func testImportResponseDecode() throws {
        let json = """
        {
            "total": 2, "successful": 1, "failed": 1,
            "results": [
                {"success": true, "item_id": "id1", "original_filename": "a.mov", "converted_filename": "a_h265.mov", "error_message": null, "original_deleted": true, "original_delete_error": null},
                {"success": false, "item_id": "id2", "original_filename": "b.mov", "converted_filename": "b_h265.mov", "error_message": "failed", "original_deleted": false, "original_delete_error": "permission denied"}
            ]
        }
        """.data(using: .utf8)!
        let response = try JSONDecoder().decode(ImportResponse.self, from: json)
        XCTAssertEqual(response.total, 2)
        XCTAssertEqual(response.successful, 1)
        XCTAssertEqual(response.results[0].originalDeleted, true)
        XCTAssertEqual(response.results[1].originalDeleteError, "permission denied")
    }
}

final class ConfigReaderTests: XCTestCase {
    func testReadMissingFile() {
        let reader = ConfigReader(configPath: "/tmp/nonexistent_vco_config.json")
        XCTAssertThrowsError(try reader.read())
    }

    func testReadValidConfig() throws {
        let json = """
        {"aws": {"s3_bucket": "my-bucket", "role_arn": "arn:aws:iam::123:role/test", "region": "us-east-1"}, "conversion": {"top_n": 5}}
        """
        let path = "/tmp/test_vco_config.json"
        try json.write(toFile: path, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let reader = ConfigReader(configPath: path)
        let config = try reader.read()
        XCTAssertEqual(config.aws?.s3Bucket, "my-bucket")
        XCTAssertEqual(config.conversion?.topN, 5)
    }

    func testValidateMissingFields() throws {
        let json = """
        {"aws": {}, "conversion": {}}
        """
        let path = "/tmp/test_vco_config_missing.json"
        try json.write(toFile: path, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(atPath: path) }

        let reader = ConfigReader(configPath: path)
        let missing = reader.validate()
        XCTAssertTrue(missing.contains("aws.s3_bucket"))
        XCTAssertTrue(missing.contains("aws.role_arn"))
    }
}

final class StateStoreTests: XCTestCase {
    let testPath = "/tmp/test_vco_menubar_state.json"

    override func tearDown() {
        try? FileManager.default.removeItem(atPath: testPath)
    }

    func testSaveAndLoad() throws {
        let store = StateStore(path: testPath)
        let state = PipelineState(
            stage: .importing,
            taskIds: ["t1"],
            files: [FileState(filename: "x.mov", status: .success, progressPercentage: 100, errorReason: nil)],
            lastUpdated: Date(),
            topNUsed: nil,
            lastPolledStatus: nil,
            lastStatusChangeTime: nil
        )
        try store.save(state)
        let loaded = try store.load()
        XCTAssertEqual(loaded?.stage, .importing)
        XCTAssertEqual(loaded?.files[0].filename, "x.mov")
    }

    func testLoadNonexistent() throws {
        let store = StateStore(path: "/tmp/nonexistent_state_12345.json")
        let result = try store.load()
        XCTAssertNil(result)
    }

    func testClear() throws {
        let store = StateStore(path: testPath)
        let state = PipelineState(stage: .idle, taskIds: [], files: [], lastUpdated: Date(), topNUsed: nil, lastPolledStatus: nil, lastStatusChangeTime: nil)
        try store.save(state)
        try store.clear()
        XCTAssertFalse(FileManager.default.fileExists(atPath: testPath))
    }
}

final class CLIRunnerTests: XCTestCase {
    func testBuildArgumentsScan() {
        let runner = CLIRunner(vcoPath: "/usr/local/bin/vco")
        let args = runner.buildArguments(for: .scan(topN: 5))
        XCTAssertEqual(args, ["scan", "--json", "--top-n", "5"])
    }

    func testBuildArgumentsScanNoTopN() {
        let runner = CLIRunner(vcoPath: "/usr/local/bin/vco")
        let args = runner.buildArguments(for: .scan(topN: nil))
        XCTAssertEqual(args, ["scan", "--json"])
    }

    func testBuildArgumentsConvert() {
        let runner = CLIRunner(vcoPath: "/usr/local/bin/vco")
        let args = runner.buildArguments(for: .convert(topN: 3, yes: true))
        XCTAssertEqual(args, ["convert", "--json", "--top-n", "3", "--yes"])
    }

    func testBuildArgumentsImportAll() {
        let runner = CLIRunner(vcoPath: "/usr/local/bin/vco")
        let args = runner.buildArguments(for: .importAll(deleteOriginal: true))
        XCTAssertEqual(args, ["import", "--all", "--yes", "--json", "--delete-original"])
    }

    func testBuildArgumentsStatus() {
        let runner = CLIRunner(vcoPath: "/usr/local/bin/vco")
        let args = runner.buildArguments(for: .status(taskId: "abc"))
        XCTAssertEqual(args, ["status", "--json", "abc"])
    }

    func testClassifyErrorDiskSpace() {
        let runner = CLIRunner(vcoPath: "/usr/local/bin/vco")
        let result = CLIResult(exitCode: 1, stdout: Data(), stderr: "No space left on device".data(using: .utf8)!)
        if case .diskSpace = runner.classifyError(result: result) {
            // pass
        } else {
            XCTFail("Expected diskSpace error")
        }
    }

    func testClassifyErrorAuth() {
        let runner = CLIRunner(vcoPath: "/usr/local/bin/vco")
        let result = CLIResult(exitCode: 1, stdout: Data(), stderr: "credentials expired".data(using: .utf8)!)
        if case .authExpired = runner.classifyError(result: result) {
            // pass
        } else {
            XCTFail("Expected authExpired error")
        }
    }

    func testParseJSON() throws {
        let runner = CLIRunner(vcoPath: "/usr/local/bin/vco")
        let json = """
        {"task_id": "t1", "file_count": 2, "status": "submitted", "error_message": null}
        """.data(using: .utf8)!
        let response = try runner.parseJSON(ConvertResponse.self, from: json)
        XCTAssertEqual(response.taskId, "t1")
        XCTAssertEqual(response.fileCount, 2)
    }
}

@MainActor
final class PipelineControllerTests: XCTestCase {
    func testInitialState() {
        let controller = PipelineController()
        XCTAssertEqual(controller.stage, .idle)
        XCTAssertTrue(controller.files.isEmpty)
        XCTAssertFalse(controller.isCancelRequested)
    }

    func testRequestStop() {
        let controller = PipelineController()
        controller.stage = .polling
        controller.requestStop()
        XCTAssertTrue(controller.isCancelRequested)
        XCTAssertEqual(controller.stage, .cancelling)
    }

    func testRequestStopWhenIdle() {
        let controller = PipelineController()
        controller.requestStop()
        XCTAssertFalse(controller.isCancelRequested)
        XCTAssertEqual(controller.stage, .idle)
    }
}
