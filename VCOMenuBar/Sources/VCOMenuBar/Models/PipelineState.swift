import Foundation

enum PipelineStage: String, Codable {
    case idle, scanning, converting, polling, importing, cancelling, error
}

enum FileStatus: String, Codable {
    case pending, processing, success, failed
}

struct FileState: Codable, Identifiable {
    var id: String { filename }
    let filename: String
    var status: FileStatus
    var progressPercentage: Int?
    var errorReason: String?
}

struct PipelineState: Codable {
    var stage: PipelineStage
    var taskIds: [String]
    var files: [FileState]
    var lastUpdated: Date
    var topNUsed: Int?
    var lastPolledStatus: String?
    var lastStatusChangeTime: Date?
}
