import Foundation

/// JSON encoding/decoding utilities.
public enum JSONCoding {
    /// Shared encoder with sorted keys
    public static let encoder: JSONEncoder = {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return encoder
    }()
    
    /// Shared decoder
    public static let decoder: JSONDecoder = {
        let decoder = JSONDecoder()
        return decoder
    }()
    
    /// Encode value to JSON string
    public static func encode<T: Encodable>(_ value: T) throws -> String {
        let data = try encoder.encode(value)
        guard let string = String(data: data, encoding: .utf8) else {
            throw JSONCodingError.encodingFailed("Failed to convert data to UTF-8 string")
        }
        return string
    }
    
    /// Decode JSON string to value
    public static func decode<T: Decodable>(_ string: String, as type: T.Type) throws -> T {
        guard let data = string.data(using: .utf8) else {
            throw JSONCodingError.decodingFailed("Failed to convert string to UTF-8 data")
        }
        return try decoder.decode(type, from: data)
    }
    
    /// Decode JSON data to value
    public static func decode<T: Decodable>(_ data: Data, as type: T.Type) throws -> T {
        try decoder.decode(type, from: data)
    }
}

/// JSON coding errors
public enum JSONCodingError: Error {
    case encodingFailed(String)
    case decodingFailed(String)
}
