import XCTest
@testable import VCOPhotosLib

final class DeleteCommandTests: XCTestCase {
    
    // MARK: - Parameter Validation Tests
    
    func testDeleteCommandRequestParsing() throws {
        let json = """
        {"command": "delete", "args": {"uuid": "ABC123-DEF456"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.command, "delete")
        XCTAssertEqual(request.args.uuid, "ABC123-DEF456")
    }
    
    func testDeleteCommandMissingUUID() throws {
        let json = """
        {"command": "delete", "args": {}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertNil(request.args.uuid)
    }
    
    func testDeleteCommandWithEmptyUUID() throws {
        let json = """
        {"command": "delete", "args": {"uuid": ""}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.args.uuid, "")
    }
    
    // MARK: - Error Response Format Tests
    
    func testMissingUUIDErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .unknown, message: "Missing required parameter: uuid")
        let response = CommandResponse<Bool>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":false"))
        XCTAssertTrue(json.contains("\"error\""))
        XCTAssertTrue(json.contains("Missing required parameter: uuid"))
    }
    
    func testNotFoundErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .notFound, message: "Video not found: ABC123")
        let response = CommandResponse<Bool>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"not_found\""))
    }
    
    func testUserCancelledErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .userCancelled, message: "User cancelled the deletion")
        let response = CommandResponse<Bool>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"user_cancelled\""))
    }
    
    func testDeleteFailedErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .deleteFailed, message: "Failed to delete video")
        let response = CommandResponse<Bool>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"delete_failed\""))
    }
    
    // MARK: - Success Response Format Tests
    
    func testSuccessResponseTrue() throws {
        let response = CommandResponse<Bool>.success(true)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":true"))
        XCTAssertTrue(json.contains("\"data\":true"))
    }
    
    func testSuccessResponseFalse() throws {
        // When video was already deleted (idempotent)
        let response = CommandResponse<Bool>.success(false)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":true"))
        XCTAssertTrue(json.contains("\"data\":false"))
    }
}
