import XCTest
@testable import VCOPhotosLib

final class ImportCommandTests: XCTestCase {
    
    // MARK: - Parameter Validation Tests
    
    func testImportCommandRequestParsing() throws {
        let json = """
        {"command": "import", "args": {"path": "/path/to/video.mov"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.command, "import")
        XCTAssertEqual(request.args.path, "/path/to/video.mov")
        XCTAssertNil(request.args.albumNames)
    }
    
    func testImportCommandWithAlbumNames() throws {
        let json = """
        {"command": "import", "args": {"path": "/path/to/video.mov", "album_names": ["Album1", "Album2"]}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.args.path, "/path/to/video.mov")
        XCTAssertEqual(request.args.albumNames, ["Album1", "Album2"])
    }
    
    func testImportCommandWithEmptyAlbumNames() throws {
        let json = """
        {"command": "import", "args": {"path": "/path/to/video.mov", "album_names": []}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.args.albumNames, [])
    }
    
    func testImportCommandMissingPath() throws {
        let json = """
        {"command": "import", "args": {}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertNil(request.args.path)
    }
    
    // MARK: - Error Response Format Tests
    
    func testMissingPathErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .unknown, message: "Missing required parameter: path")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":false"))
        XCTAssertTrue(json.contains("\"error\""))
        XCTAssertTrue(json.contains("Missing required parameter: path"))
    }
    
    func testFileNotFoundErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .fileNotFound, message: "File not found: /path/to/missing.mov")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":false"))
        XCTAssertTrue(json.contains("\"type\":\"file_not_found\""))
    }
    
    func testImportFailedErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .importFailed, message: "Failed to import video")
        let response = CommandResponse<String>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"import_failed\""))
    }
    
    // MARK: - Success Response Format Tests
    
    func testSuccessResponseWithLocalIdentifier() throws {
        let localIdentifier = "ABC123-DEF456-L0-001"
        let response = CommandResponse<String>.success(localIdentifier)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":true"))
        // Note: Using identifier without slashes to avoid JSON escaping issues
        XCTAssertTrue(json.contains("\"data\":"))
        XCTAssertTrue(json.contains("ABC123-DEF456-L0-001"))
    }
}
