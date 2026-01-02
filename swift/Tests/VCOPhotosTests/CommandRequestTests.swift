import XCTest
@testable import VCOPhotosLib

final class CommandRequestTests: XCTestCase {
    
    func testScanCommandParsing() throws {
        let json = """
        {
            "command": "scan",
            "args": {}
        }
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        
        XCTAssertEqual(request.command, "scan")
        XCTAssertNil(request.args.fromDate)
        XCTAssertNil(request.args.toDate)
    }
    
    func testScanCommandWithDateRange() throws {
        let json = """
        {
            "command": "scan",
            "args": {
                "from_date": "2024-01-01T00:00:00",
                "to_date": "2024-12-31T23:59:59"
            }
        }
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        
        XCTAssertEqual(request.command, "scan")
        XCTAssertEqual(request.args.fromDate, "2024-01-01T00:00:00")
        XCTAssertEqual(request.args.toDate, "2024-12-31T23:59:59")
    }
    
    func testImportCommandParsing() throws {
        let json = """
        {
            "command": "import",
            "args": {
                "path": "/path/to/video.mov",
                "album_names": ["Album1", "Album2"]
            }
        }
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        
        XCTAssertEqual(request.command, "import")
        XCTAssertEqual(request.args.path, "/path/to/video.mov")
        XCTAssertEqual(request.args.albumNames, ["Album1", "Album2"])
    }
    
    func testDeleteCommandParsing() throws {
        let json = """
        {
            "command": "delete",
            "args": {
                "uuid": "E1F92593-2A89-44CA-B4F9-5C586A2EEE14/L0/001"
            }
        }
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        
        XCTAssertEqual(request.command, "delete")
        XCTAssertEqual(request.args.uuid, "E1F92593-2A89-44CA-B4F9-5C586A2EEE14/L0/001")
    }
    
    func testExportCommandParsing() throws {
        let json = """
        {
            "command": "export",
            "args": {
                "uuid": "TEST-UUID",
                "destination": "/path/to/export/"
            }
        }
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        
        XCTAssertEqual(request.command, "export")
        XCTAssertEqual(request.args.uuid, "TEST-UUID")
        XCTAssertEqual(request.args.destination, "/path/to/export/")
    }
    
    func testDownloadCommandParsing() throws {
        let json = """
        {
            "command": "download",
            "args": {
                "uuid": "ICLOUD-UUID"
            }
        }
        """
        
        let request = try JSONCoding.decode(json, as: CommandRequest.self)
        
        XCTAssertEqual(request.command, "download")
        XCTAssertEqual(request.args.uuid, "ICLOUD-UUID")
    }
}
