import Foundation

/// Request structure for commands sent via stdin.
public struct CommandRequest: Codable {
    /// Command name (scan, import, delete, export, download)
    public let command: String
    
    /// Command arguments
    public let args: CommandArgs
    
    enum CodingKeys: String, CodingKey {
        case command
        case args
    }
}

/// Command arguments container.
public struct CommandArgs: Codable {
    /// Date filter: from date (ISO 8601)
    public var fromDate: String?
    
    /// Date filter: to date (ISO 8601)
    public var toDate: String?
    
    /// Video UUID for operations
    public var uuid: String?
    
    /// File path for import/export
    public var path: String?
    
    /// Destination path for export
    public var destination: String?
    
    /// Album names for import
    public var albumNames: [String]?
    
    /// Capture date for import (ISO 8601, used as creationDate in Photos)
    public var captureDate: String?
    
    enum CodingKeys: String, CodingKey {
        case fromDate = "from_date"
        case toDate = "to_date"
        case uuid
        case path
        case destination
        case albumNames = "album_names"
        case captureDate = "capture_date"
    }
    
    public init(
        fromDate: String? = nil,
        toDate: String? = nil,
        uuid: String? = nil,
        path: String? = nil,
        destination: String? = nil,
        albumNames: [String]? = nil,
        captureDate: String? = nil
    ) {
        self.fromDate = fromDate
        self.toDate = toDate
        self.uuid = uuid
        self.path = path
        self.destination = destination
        self.albumNames = albumNames
        self.captureDate = captureDate
    }
}
