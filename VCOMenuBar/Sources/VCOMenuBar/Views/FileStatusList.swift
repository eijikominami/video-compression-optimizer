import SwiftUI

struct FileStatusList: View {
    let files: [FileState]

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 4) {
                ForEach(files) { file in
                    FileStatusRow(file: file)
                }
            }
        }
        .frame(maxHeight: 200)
    }
}

struct FileStatusRow: View {
    let file: FileState

    var body: some View {
        HStack(spacing: 6) {
            statusIcon
            VStack(alignment: .leading, spacing: 2) {
                Text(file.filename)
                    .font(.caption)
                    .lineLimit(1)
                if let progress = file.progressPercentage, file.status == .processing {
                    ProgressView(value: Double(progress), total: 100)
                }
                if let error = file.errorReason {
                    Text(error)
                        .font(.caption2)
                        .foregroundColor(.red)
                        .lineLimit(1)
                }
            }
        }
    }

    private var statusIcon: some View {
        Group {
            switch file.status {
            case .pending: Image(systemName: "circle").foregroundColor(.gray)
            case .processing: Image(systemName: "arrow.triangle.2.circlepath").foregroundColor(.blue)
            case .success: Image(systemName: "checkmark.circle.fill").foregroundColor(.green)
            case .failed: Image(systemName: "xmark.circle.fill").foregroundColor(.red)
            }
        }
        .font(.caption)
    }
}
