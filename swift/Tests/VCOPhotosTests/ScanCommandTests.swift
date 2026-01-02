import XCTest
@testable import VCOPhotosLib

final class ScanCommandTests: XCTestCase {
    
    // MARK: - JSON Input Parsing Tests
    
    func testScanCommandRequestParsing() throws {
        let json = """
        {"command": "scan", "args": {}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.command, "scan")
        XCTAssertNil(request.args.fromDate)
        XCTAssertNil(request.args.toDate)
    }
    
    func testScanCommandWithDateRangeParsing() throws {
        let json = """
        {"command": "scan", "args": {"from_date": "2024-01-01T00:00:00Z", "to_date": "2024-12-31T23:59:59Z"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.command, "scan")
        XCTAssertEqual(request.args.fromDate, "2024-01-01T00:00:00Z")
        XCTAssertEqual(request.args.toDate, "2024-12-31T23:59:59Z")
    }
    
    func testScanCommandWithOnlyFromDate() throws {
        let json = """
        {"command": "scan", "args": {"from_date": "2024-06-01T00:00:00Z"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.args.fromDate, "2024-06-01T00:00:00Z")
        XCTAssertNil(request.args.toDate)
    }
    
    func testScanCommandWithOnlyToDate() throws {
        let json = """
        {"command": "scan", "args": {"to_date": "2024-06-30T23:59:59Z"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertNil(request.args.fromDate)
        XCTAssertEqual(request.args.toDate, "2024-06-30T23:59:59Z")
    }
    
    // MARK: - Date Filtering Logic Tests
    
    func testDateParsingForFiltering() {
        // Valid ISO 8601 dates should parse
        let validDate = "2024-01-15T10:30:00Z"
        let parsed = DateFormatting.parse(validDate)
        XCTAssertNotNil(parsed)
        
        // Invalid dates should return nil
        let invalidDate = "not-a-date"
        let parsedInvalid = DateFormatting.parse(invalidDate)
        XCTAssertNil(parsedInvalid)
    }
    
    func testDateRangeLogic() {
        let fromDateStr = "2024-01-01T00:00:00Z"
        let toDateStr = "2024-12-31T23:59:59Z"
        
        let fromDate = DateFormatting.parse(fromDateStr)
        let toDate = DateFormatting.parse(toDateStr)
        
        XCTAssertNotNil(fromDate)
        XCTAssertNotNil(toDate)
        
        // from_date should be before to_date
        if let from = fromDate, let to = toDate {
            XCTAssertTrue(from < to)
        }
    }
    
    // MARK: - JSON Output Format Tests
    
    func testSuccessResponseFormat() throws {
        let videos = [
            VideoInfo(
                uuid: "test-uuid-1",
                filename: "video1.mov",
                path: "/path/to/video1.mov",
                codec: "hevc",
                resolution: [1920, 1080],
                bitrate: 5000000,
                duration: 60.0,
                frameRate: 30.0,
                fileSize: 1000000,
                creationDate: "2024-01-01T00:00:00Z",
                albums: ["Album1"],
                isInIcloud: false,
                isLocal: true
            )
        ]
        
        let response = CommandResponse<[VideoInfo]>.success(videos)
        let json = try JSONCoding.encode(response)
        
        // Verify JSON structure
        XCTAssertTrue(json.contains("\"success\":true"))
        XCTAssertTrue(json.contains("\"data\""))
        XCTAssertTrue(json.contains("\"uuid\":\"test-uuid-1\""))
        XCTAssertFalse(json.contains("\"error\""))
    }
    
    func testEmptyResultResponseFormat() throws {
        let videos: [VideoInfo] = []
        let response = CommandResponse<[VideoInfo]>.success(videos)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":true"))
        XCTAssertTrue(json.contains("\"data\":[]"))
    }
}
