import XCTest
@testable import VCOPhotosLib

final class ExportCommandTests: XCTestCase {
    
    // MARK: - Parameter Validation Tests
    
    func testExportCommandRequestParsing() throws {
        let json = """
        {"command": "export", "args": {"uuid": "ABC123-DEF456", "destination": "/path/to/output.mov"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.command, "export")
        XCTAssertEqual(request.args.uuid, "ABC123-DEF456")
        XCTAssertEqual(request.args.destination, "/path/to/output.mov")
    }
    
    func testExportCommandMissingUUID() throws {
        let json = """
        {"command": "export", "args": {"destination": "/path/to/output.mov"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertNil(request.args.uuid)
        XCTAssertEqual(request.args.destination, "/path/to/output.mov")
    }
    
    func testExportCommandMissingDestination() throws {
        let json = """
        {"command": "export", "args": {"uuid": "ABC123-DEF456"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.args.uuid, "ABC123-DEF456")
        XCTAssertNil(request.args.destination)
    }
    
    func testExportCommandMissingBothParams() throws {
        let json = """
        {"command": "export", "args": {}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertNil(request.args.uuid)
        XCTAssertNil(request.args.destination)
    }
    
    // MARK: - Error Response Format Tests
    
    func testMissingUUIDErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .unknown, message: "Missing required parameter: uuid")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":false"))
        XCTAssertTrue(json.contains("Missing required parameter: uuid"))
    }
    
    func testMissingDestinationErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .unknown, message: "Missing required parameter: destination")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("Missing required parameter: destination"))
    }
    
    func testNotFoundErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .notFound, message: "Video not found: ABC123")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"not_found\""))
    }
    
    func testExportFailedErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .exportFailed, message: "Failed to export video")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"export_failed\""))
    }
    
    // MARK: - Success Response Format Tests
    
    func testSuccessResponseWithPath() throws {
        let exportedPath = "/path/to/output.mov"
        let response = CommandResponse<String>.success(exportedPath)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":true"))
        // Note: Swift's JSONEncoder escapes forward slashes as \/
        XCTAssertTrue(json.contains("\"data\":"))
        XCTAssertTrue(json.contains("output.mov"))
    }
}
