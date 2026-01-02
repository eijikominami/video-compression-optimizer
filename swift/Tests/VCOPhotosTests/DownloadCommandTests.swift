import XCTest
@testable import VCOPhotosLib

final class DownloadCommandTests: XCTestCase {
    
    // MARK: - Parameter Validation Tests
    
    func testDownloadCommandRequestParsing() throws {
        let json = """
        {"command": "download", "args": {"uuid": "ABC123-DEF456"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.command, "download")
        XCTAssertEqual(request.args.uuid, "ABC123-DEF456")
    }
    
    func testDownloadCommandMissingUUID() throws {
        let json = """
        {"command": "download", "args": {}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertNil(request.args.uuid)
    }
    
    // MARK: - Error Response Format Tests
    
    func testMissingUUIDErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .unknown, message: "Missing required parameter: uuid")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":false"))
        XCTAssertTrue(json.contains("Missing required parameter: uuid"))
    }
    
    func testNotFoundErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .notFound, message: "Video not found: ABC123")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"not_found\""))
    }
    
    func testICloudDownloadFailedErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .icloudDownloadFailed, message: "Failed to download from iCloud")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"icloud_download_failed\""))
    }
    
    func testTimeoutErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .timeout, message: "Download timed out")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"timeout\""))
    }
    
    // MARK: - Success Response Format Tests
    
    func testSuccessResponseWithPath() throws {
        let downloadedPath = "/var/folders/tmp/video.mov"
        let response = CommandResponse<String>.success(downloadedPath)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":true"))
        // Note: Swift's JSONEncoder escapes forward slashes as \/
        XCTAssertTrue(json.contains("\"data\":"))
        XCTAssertTrue(json.contains("video.mov"))
    }
}
