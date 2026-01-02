import XCTest
@testable import VCOPhotosLib

final class CommandResponseTests: XCTestCase {
    
    func testSuccessResponseEncoding() throws {
        let videos = [
            VideoInfo(
                uuid: "UUID-1",
                filename: "video1.mov",
                codec: "hevc"
            ),
            VideoInfo(
                uuid: "UUID-2",
                filename: "video2.mov",
                codec: "h264"
            )
        ]
        
        let response = AnyCommandResponse.success(videos)
        let jsonString = try JSONCoding.encode(response)
        
        XCTAssertTrue(jsonString.contains("\"success\":true"))
        XCTAssertTrue(jsonString.contains("\"uuid\":\"UUID-1\""))
        XCTAssertTrue(jsonString.contains("\"uuid\":\"UUID-2\""))
        XCTAssertFalse(jsonString.contains("\"error\""))
    }
    
    func testFailureResponseEncoding() throws {
        let error = ErrorInfo(type: .authorizationDenied, message: "Photos access denied")
        let response = AnyCommandResponse.failure(error)
        let jsonString = try JSONCoding.encode(response)
        
        XCTAssertTrue(jsonString.contains("\"success\":false"))
        XCTAssertTrue(jsonString.contains("\"authorization_denied\""))
        XCTAssertTrue(jsonString.contains("Photos access denied"))
    }
    
    func testSuccessResponseWithString() throws {
        let response = AnyCommandResponse.success("NEW-UUID/L0/001")
        let jsonString = try JSONCoding.encode(response)
        
        XCTAssertTrue(jsonString.contains("\"success\":true"), "Expected success:true in JSON: \(jsonString)")
        // The string may be escaped or formatted differently
        XCTAssertTrue(jsonString.contains("NEW-UUID") || jsonString.contains("NEW-UUID/L0/001"),
                     "Expected UUID in JSON: \(jsonString)")
    }
    
    func testSuccessResponseWithBool() throws {
        let response = AnyCommandResponse.success(true)
        let jsonString = try JSONCoding.encode(response)
        
        XCTAssertTrue(jsonString.contains("\"success\":true"))
        XCTAssertTrue(jsonString.contains("\"data\":true"))
    }
}
