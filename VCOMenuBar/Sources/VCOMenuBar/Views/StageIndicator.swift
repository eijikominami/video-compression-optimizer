import SwiftUI

struct StageIndicator: View {
    let stage: PipelineStage

    var body: some View {
        HStack(spacing: 6) {
            Circle()
                .fill(color)
                .frame(width: 8, height: 8)
            Text(label)
                .font(.headline)
        }
    }

    private var label: String {
        switch stage {
        case .idle: "Idle"
        case .scanning: "Scanning..."
        case .converting: "Converting..."
        case .polling: "Polling..."
        case .importing: "Importing..."
        case .cancelling: "Cancelling..."
        case .error: "Error"
        }
    }

    private var color: Color {
        switch stage {
        case .idle: .gray
        case .error: .red
        case .cancelling: .orange
        default: .green
        }
    }
}
