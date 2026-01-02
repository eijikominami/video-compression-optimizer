import XCTest
@testable import VCOPhotosLib

final class AddToAlbumsCommandTests: XCTestCase {
    
    // MARK: - Parameter Validation Tests
    
    func testAddToAlbumsCommandRequestParsing() throws {
        let json = """
        {"command": "add_to_albums", "args": {"uuid": "ABC123-DEF456", "album_names": ["Album1", "Album2"]}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.command, "add_to_albums")
        XCTAssertEqual(request.args.uuid, "ABC123-DEF456")
        XCTAssertEqual(request.args.albumNames, ["Album1", "Album2"])
    }
    
    func testAddToAlbumsCommandSingleAlbum() throws {
        let json = """
        {"command": "add_to_albums", "args": {"uuid": "ABC123", "album_names": ["MyAlbum"]}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.args.albumNames, ["MyAlbum"])
    }
    
    func testAddToAlbumsCommandMissingUUID() throws {
        let json = """
        {"command": "add_to_albums", "args": {"album_names": ["Album1"]}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertNil(request.args.uuid)
        XCTAssertEqual(request.args.albumNames, ["Album1"])
    }
    
    func testAddToAlbumsCommandMissingAlbumNames() throws {
        let json = """
        {"command": "add_to_albums", "args": {"uuid": "ABC123"}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.args.uuid, "ABC123")
        XCTAssertNil(request.args.albumNames)
    }
    
    func testAddToAlbumsCommandEmptyAlbumNames() throws {
        let json = """
        {"command": "add_to_albums", "args": {"uuid": "ABC123", "album_names": []}}
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        XCTAssertEqual(request.args.albumNames, [])
    }
    
    // MARK: - Error Response Format Tests
    
    func testMissingUUIDErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .unknown, message: "Missing required parameter: uuid")
        let response = CommandResponse<Bool>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":false"))
        XCTAssertTrue(json.contains("Missing required parameter: uuid"))
    }
    
    func testMissingAlbumNamesErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .unknown, message: "Missing or empty required parameter: album_names")
        let response = CommandResponse<Bool>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("Missing or empty required parameter: album_names"))
    }
    
    func testNotFoundErrorResponse() throws {
        let errorInfo = ErrorInfo(type: .notFound, message: "Video not found: ABC123")
        let response = CommandResponse<Bool>.failure(errorInfo)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"type\":\"not_found\""))
    }
    
    // MARK: - Success Response Format Tests
    
    func testSuccessResponse() throws {
        let response = CommandResponse<Bool>.success(true)
        let json = try JSONCoding.encode(response)
        
        XCTAssertTrue(json.contains("\"success\":true"))
        XCTAssertTrue(json.contains("\"data\":true"))
    }
}
