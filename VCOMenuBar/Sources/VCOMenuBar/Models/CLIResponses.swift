import Foundation

// MARK: - vco scan --json

struct ScanResponse: Codable {
    let summary: ScanSummary
    let candidates: [ScanCandidate]
}

struct ScanSummary: Codable {
    let totalCandidates: Int
    let totalSavingsBytes: Int

    enum CodingKeys: String, CodingKey {
        case totalCandidates = "total_candidates"
        case totalSavingsBytes = "total_savings_bytes"
    }
}

struct ScanCandidate: Codable {
    let filename: String
    let fileSize: Int

    enum CodingKeys: String, CodingKey {
        case filename
        case fileSize = "file_size"
    }
}

// MARK: - vco convert --json

struct ConvertResponse: Codable {
    let taskId: String?
    let fileCount: Int
    let status: String
    let errorMessage: String?

    enum CodingKeys: String, CodingKey {
        case taskId = "task_id"
        case fileCount = "file_count"
        case status
        case errorMessage = "error_message"
    }
}

// MARK: - vco status --json

struct StatusResponse: Codable {
    let tasks: [TaskStatus]
}

struct TaskStatus: Codable {
    let taskId: String
    let status: String
    let files: [FileTaskStatus]?
    let progressPercentage: Int?

    enum CodingKeys: String, CodingKey {
        case taskId = "task_id"
        case status
        case files
        case progressPercentage = "progress_percentage"
    }
}

struct FileTaskStatus: Codable {
    let fileId: String
    let filename: String
    let status: String
    let progressPercentage: Int?
    let errorMessage: String?

    enum CodingKeys: String, CodingKey {
        case fileId = "file_id"
        case filename
        case status
        case progressPercentage = "progress_percentage"
        case errorMessage = "error_message"
    }
}

// MARK: - vco import --all --json

struct ImportResponse: Codable {
    let total: Int
    let successful: Int
    let failed: Int
    let results: [ImportResult]
}

struct ImportResult: Codable {
    let success: Bool
    let itemId: String
    let originalFilename: String
    let convertedFilename: String
    let errorMessage: String?
    let originalDeleted: Bool?
    let originalDeleteError: String?

    enum CodingKeys: String, CodingKey {
        case success
        case itemId = "item_id"
        case originalFilename = "original_filename"
        case convertedFilename = "converted_filename"
        case errorMessage = "error_message"
        case originalDeleted = "original_deleted"
        case originalDeleteError = "original_delete_error"
    }
}
