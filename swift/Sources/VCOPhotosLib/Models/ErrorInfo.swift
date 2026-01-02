import Foundation

/// Error information for command responses.
public struct ErrorInfo: Codable, Equatable {
    /// Error type identifier
    public let type: ErrorType
    
    /// Human-readable error message
    public let message: String
    
    public init(type: ErrorType, message: String) {
        self.type = type
        self.message = message
    }
    
    enum CodingKeys: String, CodingKey {
        case type
        case message
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(type.rawValue, forKey: .type)
        try container.encode(message, forKey: .message)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let typeString = try container.decode(String.self, forKey: .type)
        self.type = ErrorType(rawValue: typeString) ?? .unknown
        self.message = try container.decode(String.self, forKey: .message)
    }
}

/// Error types matching Python implementation.
public enum ErrorType: String, Codable {
    case authorizationDenied = "authorization_denied"
    case notFound = "not_found"
    case icloudDownloadFailed = "icloud_download_failed"
    case timeout = "timeout"
    case fileNotFound = "file_not_found"
    case importFailed = "import_failed"
    case deleteFailed = "delete_failed"
    case exportFailed = "export_failed"
    case userCancelled = "user_cancelled"
    case metadataExtractionFailed = "metadata_extraction_failed"
    case unknown = "unknown"
}

/// Photos-specific errors
public enum PhotosError: Error {
    case authorizationDenied(String)
    case notFound(String)
    case icloudDownloadFailed(String)
    case timeout(String)
    case fileNotFound(String)
    case importFailed(String)
    case deleteFailed(String)
    case exportFailed(String)
    case userCancelled(String)
    case metadataExtractionFailed(String)
    case unknown(String)
    
    /// Convert to ErrorInfo for JSON response
    public var errorInfo: ErrorInfo {
        switch self {
        case .authorizationDenied(let msg):
            return ErrorInfo(type: .authorizationDenied, message: msg)
        case .notFound(let msg):
            return ErrorInfo(type: .notFound, message: msg)
        case .icloudDownloadFailed(let msg):
            return ErrorInfo(type: .icloudDownloadFailed, message: msg)
        case .timeout(let msg):
            return ErrorInfo(type: .timeout, message: msg)
        case .fileNotFound(let msg):
            return ErrorInfo(type: .fileNotFound, message: msg)
        case .importFailed(let msg):
            return ErrorInfo(type: .importFailed, message: msg)
        case .deleteFailed(let msg):
            return ErrorInfo(type: .deleteFailed, message: msg)
        case .exportFailed(let msg):
            return ErrorInfo(type: .exportFailed, message: msg)
        case .userCancelled(let msg):
            return ErrorInfo(type: .userCancelled, message: msg)
        case .metadataExtractionFailed(let msg):
            return ErrorInfo(type: .metadataExtractionFailed, message: msg)
        case .unknown(let msg):
            return ErrorInfo(type: .unknown, message: msg)
        }
    }
}
