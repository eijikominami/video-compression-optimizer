import XCTest
@testable import VCOPhotosLib

final class JSONCodingTests: XCTestCase {
    
    // MARK: - Basic Encoding/Decoding Tests
    
    func testEncodeSimpleStruct() throws {
        struct Simple: Codable {
            let name: String
            let value: Int
        }
        
        let obj = Simple(name: "test", value: 42)
        let json = try JSONCoding.encode(obj)
        
        XCTAssertTrue(json.contains("\"name\":\"test\""))
        XCTAssertTrue(json.contains("\"value\":42"))
    }
    
    func testDecodeSimpleStruct() throws {
        struct Simple: Codable, Equatable {
            let name: String
            let value: Int
        }
        
        let json = """
        {"name": "test", "value": 42}
        """
        
        let obj = try JSONCoding.decode(json, as: Simple.self)
        XCTAssertEqual(obj.name, "test")
        XCTAssertEqual(obj.value, 42)
    }
    
    func testRoundTrip() throws {
        struct Data: Codable, Equatable {
            let id: String
            let count: Int
            let enabled: Bool
        }
        
        let original = Data(id: "abc123", count: 100, enabled: true)
        let json = try JSONCoding.encode(original)
        let decoded = try JSONCoding.decode(json, as: Data.self)
        
        XCTAssertEqual(original, decoded)
    }
    
    // MARK: - Edge Cases
    
    func testEncodeEmptyArray() throws {
        let arr: [String] = []
        let json = try JSONCoding.encode(arr)
        XCTAssertEqual(json, "[]")
    }
    
    func testDecodeEmptyArray() throws {
        let json = "[]"
        let arr = try JSONCoding.decode(json, as: [String].self)
        XCTAssertTrue(arr.isEmpty)
    }
    
    func testEncodeNullOptional() throws {
        struct WithOptional: Codable {
            let required: String
            let optional: String?
        }
        
        let obj = WithOptional(required: "value", optional: nil)
        let json = try JSONCoding.encode(obj)
        
        XCTAssertTrue(json.contains("\"required\":\"value\""))
        // nil values should be omitted or encoded as null
    }
    
    func testDecodeNullOptional() throws {
        struct WithOptional: Codable {
            let required: String
            let optional: String?
        }
        
        let json = """
        {"required": "value", "optional": null}
        """
        
        let obj = try JSONCoding.decode(json, as: WithOptional.self)
        XCTAssertEqual(obj.required, "value")
        XCTAssertNil(obj.optional)
    }
    
    func testDecodeMissingOptional() throws {
        struct WithOptional: Codable {
            let required: String
            let optional: String?
        }
        
        let json = """
        {"required": "value"}
        """
        
        let obj = try JSONCoding.decode(json, as: WithOptional.self)
        XCTAssertEqual(obj.required, "value")
        XCTAssertNil(obj.optional)
    }
    
    func testEncodeNestedStruct() throws {
        struct Inner: Codable {
            let value: Int
        }
        struct Outer: Codable {
            let name: String
            let inner: Inner
        }
        
        let obj = Outer(name: "outer", inner: Inner(value: 42))
        let json = try JSONCoding.encode(obj)
        
        XCTAssertTrue(json.contains("\"name\":\"outer\""))
        XCTAssertTrue(json.contains("\"inner\""))
        XCTAssertTrue(json.contains("\"value\":42"))
    }
    
    func testEncodeArrayOfStructs() throws {
        struct Item: Codable {
            let id: Int
        }
        
        let items = [Item(id: 1), Item(id: 2), Item(id: 3)]
        let json = try JSONCoding.encode(items)
        
        XCTAssertTrue(json.contains("\"id\":1"))
        XCTAssertTrue(json.contains("\"id\":2"))
        XCTAssertTrue(json.contains("\"id\":3"))
    }
    
    // MARK: - Error Handling Tests
    
    func testDecodeInvalidJSON() {
        let invalidJSON = "not valid json"
        
        XCTAssertThrowsError(try JSONCoding.decode(invalidJSON, as: [String: String].self))
    }
    
    func testDecodeTypeMismatch() {
        struct Expected: Codable {
            let value: Int
        }
        
        let json = """
        {"value": "not an int"}
        """
        
        XCTAssertThrowsError(try JSONCoding.decode(json, as: Expected.self))
    }
    
    func testDecodeMissingRequiredField() {
        struct Required: Codable {
            let required: String
        }
        
        let json = """
        {}
        """
        
        XCTAssertThrowsError(try JSONCoding.decode(json, as: Required.self))
    }
    
    // MARK: - Special Characters Tests
    
    func testEncodeSpecialCharacters() throws {
        struct WithSpecial: Codable {
            let text: String
        }
        
        let obj = WithSpecial(text: "Hello \"World\" \n\t日本語")
        let json = try JSONCoding.encode(obj)
        
        // Should properly escape special characters
        XCTAssertTrue(json.contains("\\\"World\\\""))
        XCTAssertTrue(json.contains("\\n"))
        XCTAssertTrue(json.contains("日本語"))
    }
    
    func testDecodeSpecialCharacters() throws {
        struct WithSpecial: Codable {
            let text: String
        }
        
        let json = """
        {"text": "Hello \\"World\\" \\n\\t日本語"}
        """
        
        let obj = try JSONCoding.decode(json, as: WithSpecial.self)
        XCTAssertTrue(obj.text.contains("\"World\""))
        XCTAssertTrue(obj.text.contains("\n"))
        XCTAssertTrue(obj.text.contains("日本語"))
    }
    
    // MARK: - Sorted Keys Test
    
    func testEncoderSortsKeys() throws {
        struct Unordered: Codable {
            let zebra: String
            let apple: String
            let mango: String
        }
        
        let obj = Unordered(zebra: "z", apple: "a", mango: "m")
        let json = try JSONCoding.encode(obj)
        
        // Keys should be sorted alphabetically
        let appleIndex = json.range(of: "apple")!.lowerBound
        let mangoIndex = json.range(of: "mango")!.lowerBound
        let zebraIndex = json.range(of: "zebra")!.lowerBound
        
        XCTAssertTrue(appleIndex < mangoIndex)
        XCTAssertTrue(mangoIndex < zebraIndex)
    }
}
