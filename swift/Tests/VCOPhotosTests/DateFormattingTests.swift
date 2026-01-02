import XCTest
@testable import VCOPhotosLib

final class DateFormattingTests: XCTestCase {
    
    func testFormatDate() {
        // Create a specific date
        var components = DateComponents()
        components.year = 2024
        components.month = 6
        components.day = 15
        components.hour = 14
        components.minute = 30
        components.second = 45
        
        let calendar = Calendar.current
        let date = calendar.date(from: components)!
        
        let formatted = DateFormatting.format(date)
        
        XCTAssertEqual(formatted, "2024-06-15T14:30:45")
    }
    
    func testParseDate() {
        let dateString = "2024-06-15T14:30:45"
        
        let date = DateFormatting.parse(dateString)
        
        XCTAssertNotNil(date)
        
        let calendar = Calendar.current
        let components = calendar.dateComponents([.year, .month, .day, .hour, .minute, .second], from: date!)
        
        XCTAssertEqual(components.year, 2024)
        XCTAssertEqual(components.month, 6)
        XCTAssertEqual(components.day, 15)
        XCTAssertEqual(components.hour, 14)
        XCTAssertEqual(components.minute, 30)
        XCTAssertEqual(components.second, 45)
    }
    
    func testRoundTrip() {
        let originalString = "2024-01-15T10:30:00"
        
        let date = DateFormatting.parse(originalString)
        XCTAssertNotNil(date)
        
        let formatted = DateFormatting.format(date!)
        
        XCTAssertEqual(formatted, originalString)
    }
    
    func testFormatOptionalWithValue() {
        var components = DateComponents()
        components.year = 2024
        components.month = 1
        components.day = 1
        components.hour = 0
        components.minute = 0
        components.second = 0
        
        let date = Calendar.current.date(from: components)
        
        let formatted = DateFormatting.formatOptional(date)
        
        XCTAssertNotNil(formatted)
        XCTAssertEqual(formatted, "2024-01-01T00:00:00")
    }
    
    func testFormatOptionalWithNil() {
        let formatted = DateFormatting.formatOptional(nil)
        
        XCTAssertNil(formatted)
    }
    
    func testParseInvalidDate() {
        let invalidStrings = [
            "not a date",
            "2024-13-01T00:00:00",  // Invalid month
            "2024/01/01 00:00:00",  // Wrong format
            ""
        ]
        
        for invalidString in invalidStrings {
            let date = DateFormatting.parse(invalidString)
            XCTAssertNil(date, "Expected nil for invalid date string: \(invalidString)")
        }
    }
    
    func testParseWithTimezone() {
        // ISO 8601 with timezone
        let dateString = "2024-06-15T14:30:45Z"
        
        let date = DateFormatting.parse(dateString)
        
        XCTAssertNotNil(date)
    }
}
