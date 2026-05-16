import SwiftUI

struct MenuBarView: View {
    @ObservedObject var controller: PipelineController

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            StageIndicator(stage: controller.stage)

            if !controller.summaryText.isEmpty {
                Text(controller.summaryText)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }

            if let error = controller.errorMessage {
                Text(error)
                    .font(.caption)
                    .foregroundColor(.red)
                    .lineLimit(2)
            }

            if !controller.files.isEmpty {
                Divider()
                FileStatusList(files: controller.files)
            }

            Divider()

            HStack {
                Button("実行") {
                    Task { await controller.startPipeline() }
                }
                .disabled(controller.stage != .idle)

                Button("停止") {
                    controller.requestStop()
                }
                .disabled(controller.stage == .idle || controller.isCancelRequested)

                Spacer()

                Button("終了") {
                    NSApplication.shared.terminate(nil)
                }
            }
        }
        .padding(12)
        .frame(width: 300)
    }
}
