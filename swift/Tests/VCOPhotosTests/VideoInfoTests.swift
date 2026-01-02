import XCTest
@testable import VCOPhotosLib

final class VideoInfoTests: XCTestCase {
    
    func testVideoInfoEncoding() throws {
        let videoInfo = VideoInfo(
            uuid: "E1F92593-2A89-44CA-B4F9-5C586A2EEE14/L0/001",
            filename: "test_video.mov",
            path: "/Users/test/Photos/test_video.mov",
            codec: "hevc",
            resolution: [1920, 1080],
            bitrate: 15000000,
            duration: 120.5,
            frameRate: 30.0,
            fileSize: 225000000,
            captureDate: "2024-01-15T10:30:00",
            creationDate: "2024-01-15T10:30:00",
            albums: ["Vacation 2024", "Family"],
            isInIcloud: false,
            isLocal: true,
            location: [35.6762, 139.6503]
        )
        
        let jsonString = try JSONCoding.encode(videoInfo)
        
        // Verify JSON contains expected fields
        // Note: JSON encoder may escape forward slashes as \/
        XCTAssertTrue(jsonString.contains("E1F92593-2A89-44CA-B4F9-5C586A2EEE14"),
                     "Expected uuid in JSON: \(jsonString)")
        XCTAssertTrue(jsonString.contains("test_video.mov"),
                     "Expected filename in JSON: \(jsonString)")
        XCTAssertTrue(jsonString.contains("hevc"),
                     "Expected codec in JSON: \(jsonString)")
        XCTAssertTrue(jsonString.contains("frame_rate"),
                     "Expected frame_rate key in JSON: \(jsonString)")
        XCTAssertTrue(jsonString.contains("file_size"),
                     "Expected file_size key in JSON: \(jsonString)")
        XCTAssertTrue(jsonString.contains("is_in_icloud"),
                     "Expected is_in_icloud key in JSON: \(jsonString)")
        XCTAssertTrue(jsonString.contains("is_local"),
                     "Expected is_local key in JSON: \(jsonString)")
    }
    
    func testVideoInfoDecoding() throws {
        let json = """
        {
            "uuid": "E1F92593-2A89-44CA-B4F9-5C586A2EEE14/L0/001",
            "filename": "test_video.mov",
            "path": "/Users/test/Photos/test_video.mov",
            "codec": "hevc",
            "resolution": [1920, 1080],
            "bitrate": 15000000,
            "duration": 120.5,
            "frame_rate": 30.0,
            "file_size": 225000000,
            "capture_date": "2024-01-15T10:30:00",
            "creation_date": "2024-01-15T10:30:00",
            "albums": ["Vacation 2024", "Family"],
            "is_in_icloud": false,
            "is_local": true,
            "location": [35.6762, 139.6503]
        }
        """
        
        let videoInfo = try JSONCoding.decode(json, as: VideoInfo.self)
        
        XCTAssertEqual(videoInfo.uuid, "E1F92593-2A89-44CA-B4F9-5C586A2EEE14/L0/001")
        XCTAssertEqual(videoInfo.filename, "test_video.mov")
        XCTAssertEqual(videoInfo.codec, "hevc")
        XCTAssertEqual(videoInfo.resolution, [1920, 1080])
        XCTAssertEqual(videoInfo.bitrate, 15000000)
        XCTAssertEqual(videoInfo.duration, 120.5)
        XCTAssertEqual(videoInfo.frameRate, 30.0)
        XCTAssertEqual(videoInfo.fileSize, 225000000)
        XCTAssertEqual(videoInfo.albums, ["Vacation 2024", "Family"])
        XCTAssertEqual(videoInfo.isInIcloud, false)
        XCTAssertEqual(videoInfo.isLocal, true)
        XCTAssertEqual(videoInfo.location, [35.6762, 139.6503])
    }
    
    func testVideoInfoRoundTrip() throws {
        let original = VideoInfo(
            uuid: "TEST-UUID/L0/001",
            filename: "roundtrip.mov",
            path: "/path/to/video.mov",
            codec: "h264",
            resolution: [3840, 2160],
            bitrate: 50000000,
            duration: 300.0,
            frameRate: 60.0,
            fileSize: 1875000000,
            captureDate: "2024-06-01T14:00:00",
            creationDate: "2024-06-01T14:00:00",
            albums: ["Album1", "Album2", "Album3"],
            isInIcloud: true,
            isLocal: false,
            location: [40.7128, -74.0060]
        )
        
        // Encode to JSON
        let jsonString = try JSONCoding.encode(original)
        
        // Decode back
        let decoded = try JSONCoding.decode(jsonString, as: VideoInfo.self)
        
        // Verify equality
        XCTAssertEqual(original, decoded)
    }
    
    func testVideoInfoWithNullLocation() throws {
        let json = """
        {
            "uuid": "TEST-UUID",
            "filename": "no_location.mov",
            "path": "/path/to/video.mov",
            "codec": "hevc",
            "resolution": [1920, 1080],
            "bitrate": 10000000,
            "duration": 60.0,
            "frame_rate": 30.0,
            "file_size": 75000000,
            "capture_date": null,
            "creation_date": "2024-01-01T00:00:00",
            "albums": [],
            "is_in_icloud": false,
            "is_local": true,
            "location": null
        }
        """
        
        let videoInfo = try JSONCoding.decode(json, as: VideoInfo.self)
        
        XCTAssertNil(videoInfo.captureDate)
        XCTAssertNil(videoInfo.location)
        XCTAssertEqual(videoInfo.albums, [])
    }
    
    func testVideoInfoDefaultValues() {
        let videoInfo = VideoInfo(
            uuid: "MINIMAL-UUID",
            filename: "minimal.mov"
        )
        
        XCTAssertEqual(videoInfo.path, "")
        XCTAssertEqual(videoInfo.codec, "unknown")
        XCTAssertEqual(videoInfo.resolution, [0, 0])
        XCTAssertEqual(videoInfo.bitrate, 0)
        XCTAssertEqual(videoInfo.duration, 0.0)
        XCTAssertEqual(videoInfo.frameRate, 0.0)
        XCTAssertEqual(videoInfo.fileSize, 0)
        XCTAssertEqual(videoInfo.albums, [])
        XCTAssertEqual(videoInfo.isInIcloud, false)
        XCTAssertEqual(videoInfo.isLocal, true)
        XCTAssertNil(videoInfo.location)
    }
}
