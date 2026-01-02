import Foundation

/// Response structure for command results sent via stdout.
public struct CommandResponse<T: Encodable>: Encodable {
    /// Whether the command succeeded
    public let success: Bool
    
    /// Response data (type varies by command)
    public let data: T?
    
    /// Error information (if success is false)
    public let error: ErrorInfo?
    
    /// Create a successful response with data
    public static func success(_ data: T) -> CommandResponse<T> {
        CommandResponse(success: true, data: data, error: nil)
    }
    
    /// Create a failed response with error
    public static func failure(_ error: ErrorInfo) -> CommandResponse<T> {
        CommandResponse(success: false, data: nil, error: error)
    }
}

/// Type-erased response for encoding
public struct AnyCommandResponse: Encodable {
    public let success: Bool
    public let data: AnyEncodable?
    public let error: ErrorInfo?
    
    public init<T: Encodable>(success: Bool, data: T?, error: ErrorInfo?) {
        self.success = success
        self.data = data.map { AnyEncodable($0) }
        self.error = error
    }
    
    /// Initialize from a typed CommandResponse
    public init<T: Encodable>(from response: CommandResponse<T>) {
        self.success = response.success
        self.data = response.data.map { AnyEncodable($0) }
        self.error = response.error
    }
    
    public static func success<T: Encodable>(_ data: T) -> AnyCommandResponse {
        AnyCommandResponse(success: true, data: data, error: nil)
    }
    
    public static func failure(_ error: ErrorInfo) -> AnyCommandResponse {
        AnyCommandResponse(success: false, data: Optional<String>.none, error: error)
    }
}

/// Type-erased encodable wrapper
public struct AnyEncodable: Encodable {
    private let _encode: (Encoder) throws -> Void
    
    public init<T: Encodable>(_ value: T) {
        _encode = { encoder in
            try value.encode(to: encoder)
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        try _encode(encoder)
    }
}
