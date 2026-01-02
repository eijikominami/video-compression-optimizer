import XCTest
@testable import VCOPhotosLib

final class ErrorInfoTests: XCTestCase {
    
    func testAllErrorTypes() throws {
        let errorTypes: [(ErrorType, String)] = [
            (.authorizationDenied, "authorization_denied"),
            (.notFound, "not_found"),
            (.icloudDownloadFailed, "icloud_download_failed"),
            (.timeout, "timeout"),
            (.fileNotFound, "file_not_found"),
            (.importFailed, "import_failed"),
            (.deleteFailed, "delete_failed"),
            (.exportFailed, "export_failed"),
            (.userCancelled, "user_cancelled"),
            (.unknown, "unknown")
        ]
        
        for (errorType, expectedString) in errorTypes {
            let error = ErrorInfo(type: errorType, message: "Test message")
            let jsonString = try JSONCoding.encode(error)
            
            XCTAssertTrue(jsonString.contains("\"\(expectedString)\""), 
                         "Expected \(expectedString) in JSON: \(jsonString)")
        }
    }
    
    func testErrorInfoEncoding() throws {
        let error = ErrorInfo(
            type: .authorizationDenied,
            message: "Photos library access was denied"
        )
        
        let jsonString = try JSONCoding.encode(error)
        
        XCTAssertTrue(jsonString.contains("\"type\":\"authorization_denied\""))
        XCTAssertTrue(jsonString.contains("\"message\":\"Photos library access was denied\""))
    }
    
    func testErrorInfoDecoding() throws {
        let json = """
        {
            "type": "not_found",
            "message": "Video not found: UUID-123"
        }
        """
        
        let error = try JSONCoding.decode(json, as: ErrorInfo.self)
        
        XCTAssertEqual(error.type, .notFound)
        XCTAssertEqual(error.message, "Video not found: UUID-123")
    }
    
    func testErrorInfoRoundTrip() throws {
        let original = ErrorInfo(
            type: .icloudDownloadFailed,
            message: "Network connection lost"
        )
        
        let jsonString = try JSONCoding.encode(original)
        let decoded = try JSONCoding.decode(jsonString, as: ErrorInfo.self)
        
        XCTAssertEqual(original, decoded)
    }
    
    func testPhotosErrorConversion() {
        let errors: [(PhotosError, ErrorType)] = [
            (.authorizationDenied("msg"), .authorizationDenied),
            (.notFound("msg"), .notFound),
            (.icloudDownloadFailed("msg"), .icloudDownloadFailed),
            (.timeout("msg"), .timeout),
            (.fileNotFound("msg"), .fileNotFound),
            (.importFailed("msg"), .importFailed),
            (.deleteFailed("msg"), .deleteFailed),
            (.exportFailed("msg"), .exportFailed),
            (.userCancelled("msg"), .userCancelled),
            (.unknown("msg"), .unknown)
        ]
        
        for (photosError, expectedType) in errors {
            let errorInfo = photosError.errorInfo
            XCTAssertEqual(errorInfo.type, expectedType)
            XCTAssertEqual(errorInfo.message, "msg")
        }
    }
    
    func testUnknownErrorTypeDecoding() throws {
        let json = """
        {
            "type": "some_future_error_type",
            "message": "Unknown error type"
        }
        """
        
        let error = try JSONCoding.decode(json, as: ErrorInfo.self)
        
        // Unknown types should default to .unknown
        XCTAssertEqual(error.type, .unknown)
        XCTAssertEqual(error.message, "Unknown error type")
    }
}
