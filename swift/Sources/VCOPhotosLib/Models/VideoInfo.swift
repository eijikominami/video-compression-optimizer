import Foundation

/// Video information from Apple Photos library.
/// Matches Python VideoInfo dataclass for compatibility.
public struct VideoInfo: Codable, Equatable {
    /// Photos library UUID (localIdentifier)
    public let uuid: String
    
    /// Original filename
    public let filename: String
    
    /// Path to the video file
    public let path: String
    
    /// Video codec (e.g., hevc, h264)
    public let codec: String
    
    /// Video resolution as [width, height]
    public let resolution: [Int]
    
    /// Video bitrate in bits per second
    public let bitrate: Int
    
    /// Video duration in seconds
    public let duration: Double
    
    /// Video frame rate in fps
    public let frameRate: Double
    
    /// File size in bytes
    public let fileSize: Int
    
    /// Capture date (ISO 8601 format)
    public let captureDate: String?
    
    /// Creation date (ISO 8601 format)
    public let creationDate: String
    
    /// List of album names
    public let albums: [String]
    
    /// Whether the video is stored in iCloud
    public let isInIcloud: Bool
    
    /// Whether the video is available locally
    public let isLocal: Bool
    
    /// GPS coordinates as [latitude, longitude] or nil
    public let location: [Double]?
    
    enum CodingKeys: String, CodingKey {
        case uuid
        case filename
        case path
        case codec
        case resolution
        case bitrate
        case duration
        case frameRate = "frame_rate"
        case fileSize = "file_size"
        case captureDate = "capture_date"
        case creationDate = "creation_date"
        case albums
        case isInIcloud = "is_in_icloud"
        case isLocal = "is_local"
        case location
    }
    
    /// Create VideoInfo with default values for optional fields
    public init(
        uuid: String,
        filename: String,
        path: String = "",
        codec: String = "unknown",
        resolution: [Int] = [0, 0],
        bitrate: Int = 0,
        duration: Double = 0.0,
        frameRate: Double = 0.0,
        fileSize: Int = 0,
        captureDate: String? = nil,
        creationDate: String? = nil,
        albums: [String] = [],
        isInIcloud: Bool = false,
        isLocal: Bool = true,
        location: [Double]? = nil
    ) {
        self.uuid = uuid
        self.filename = filename
        self.path = path
        self.codec = codec
        self.resolution = resolution
        self.bitrate = bitrate
        self.duration = duration
        self.frameRate = frameRate
        self.fileSize = fileSize
        self.captureDate = captureDate
        self.creationDate = creationDate ?? ISO8601DateFormatter().string(from: Date())
        self.albums = albums
        self.isInIcloud = isInIcloud
        self.isLocal = isLocal
        self.location = location
    }
}
