import Foundation

/// Date formatting utilities for ISO 8601 compatibility with Python.
public enum DateFormatting {
    /// ISO 8601 formatter without timezone (matches Python output)
    private static let iso8601Formatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone.current
        return formatter
    }()
    
    /// ISO 8601 formatter with timezone for parsing
    private static let iso8601WithTimezone: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter
    }()
    
    /// Format date to ISO 8601 string (YYYY-MM-DDTHH:MM:SS)
    public static func format(_ date: Date) -> String {
        iso8601Formatter.string(from: date)
    }
    
    /// Parse ISO 8601 string to date
    public static func parse(_ string: String) -> Date? {
        if let date = iso8601Formatter.date(from: string) {
            return date
        }
        return iso8601WithTimezone.date(from: string)
    }
    
    /// Format optional date to ISO 8601 string or nil
    public static func formatOptional(_ date: Date?) -> String? {
        guard let date = date else { return nil }
        return format(date)
    }
}
