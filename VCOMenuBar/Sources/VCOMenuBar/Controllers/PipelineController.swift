import Foundation
import Combine

@MainActor
class PipelineController: ObservableObject {
    @Published var stage: PipelineStage = .idle
    @Published var files: [FileState] = []
    @Published var summaryText: String = ""
    @Published var isCancelRequested: Bool = false
    @Published var errorMessage: String?

    let cliRunner: CLIRunner
    let stateStore: StateStore
    let configReader: ConfigReader
    let notificationManager: NotificationManager

    private var pollingTimer: Timer?
    private var lastPolledStatus: String?
    private var lastStatusChangeTime: Date?

    init(
        cliRunner: CLIRunner = CLIRunner(),
        stateStore: StateStore = StateStore(),
        configReader: ConfigReader = ConfigReader(),
        notificationManager: NotificationManager = NotificationManager()
    ) {
        self.cliRunner = cliRunner
        self.stateStore = stateStore
        self.configReader = configReader
        self.notificationManager = notificationManager
    }

    // MARK: - Public

    func startPipeline() async {
        guard stage == .idle else { return }
        isCancelRequested = false
        errorMessage = nil
        files = []

        let config = try? configReader.read()
        var topN = config?.conversion?.topN

        // Scan
        stage = .scanning
        persistState()

        let scanResult: CLIResult
        do {
            scanResult = try await cliRunner.execute(.scan(topN: topN))
        } catch {
            handleUnexpectedError(error.localizedDescription)
            return
        }

        if scanResult.exitCode != 0 {
            handleCLIError(scanResult)
            return
        }

        guard let scanResponse = try? cliRunner.parseJSON(ScanResponse.self, from: scanResult.stdout),
              !scanResponse.candidates.isEmpty else {
            summaryText = "候補なし"
            stage = .idle
            persistState()
            return
        }

        files = scanResponse.candidates.map { FileState(filename: $0.filename, status: .pending) }
        updateSummary()

        if isCancelRequested { finishCancel(); return }

        // Convert
        stage = .converting
        persistState()

        let convertResult: CLIResult
        do {
            convertResult = try await cliRunner.execute(.convert(topN: topN, yes: true))
        } catch {
            handleUnexpectedError(error.localizedDescription)
            return
        }

        if convertResult.exitCode != 0 {
            let cliError = cliRunner.classifyError(result: convertResult)
            if case .diskSpace = cliError {
                // Retry with reduced top-n
                topN = (topN ?? files.count) - 1
                if topN! < 1 {
                    notificationManager.sendDiskSpaceInsufficient()
                    handleUnexpectedError("ディスク容量不足（リトライ不可）")
                    return
                }
                // Restart pipeline with reduced top-n
                stage = .idle
                await startPipelineWithTopN(topN!)
                return
            }
            handleCLIError(convertResult)
            return
        }

        guard let convertResponse = try? cliRunner.parseJSON(ConvertResponse.self, from: convertResult.stdout),
              let taskId = convertResponse.taskId else {
            handleUnexpectedError("convert レスポンスのパースに失敗")
            return
        }

        if isCancelRequested { finishCancel(); return }

        // Polling
        stage = .polling
        persistState(taskIds: [taskId])
        await pollUntilComplete(taskId: taskId)

        if isCancelRequested { finishCancel(); return }

        // Import
        stage = .importing
        persistState(taskIds: [taskId])

        let importResult: CLIResult
        do {
            importResult = try await cliRunner.execute(.importAll(deleteOriginal: true))
        } catch {
            handleUnexpectedError(error.localizedDescription)
            return
        }

        if importResult.exitCode != 0 {
            handleCLIError(importResult)
            return
        }

        if let importResponse = try? cliRunner.parseJSON(ImportResponse.self, from: importResult.stdout) {
            processImportResults(importResponse)
        }

        // Complete
        let successCount = files.filter { $0.status == .success }.count
        let failCount = files.filter { $0.status == .failed }.count
        notificationManager.sendPipelineComplete(successful: successCount, failed: failCount)
        if successCount == 0 && failCount > 0 {
            notificationManager.sendAllFilesFailed()
        }

        stage = .idle
        try? stateStore.clear()
        updateSummary()
    }

    func requestStop() {
        guard stage != .idle && !isCancelRequested else { return }
        isCancelRequested = true
        stage = .cancelling
        pollingTimer?.invalidate()
        pollingTimer = nil
    }

    func restoreOnLaunch() async {
        guard let state = try? stateStore.load() else { return }

        files = state.files
        updateSummary()

        switch state.stage {
        case .polling:
            stage = .polling
            if let taskId = state.taskIds.first {
                await pollUntilComplete(taskId: taskId)
                if !isCancelRequested {
                    stage = .importing
                    let result = try? await cliRunner.execute(.importAll(deleteOriginal: true))
                    if let r = result, r.exitCode == 0,
                       let resp = try? cliRunner.parseJSON(ImportResponse.self, from: r.stdout) {
                        processImportResults(resp)
                    }
                    stage = .idle
                    try? stateStore.clear()
                }
            }
        case .importing:
            stage = .importing
            let result = try? await cliRunner.execute(.importAll(deleteOriginal: true))
            if let r = result, r.exitCode == 0,
               let resp = try? cliRunner.parseJSON(ImportResponse.self, from: r.stdout) {
                processImportResults(resp)
            }
            stage = .idle
            try? stateStore.clear()
        default:
            stage = .idle
            try? stateStore.clear()
        }
        updateSummary()
    }

    // MARK: - Private

    private func startPipelineWithTopN(_ topN: Int) async {
        // Simplified retry - restart from scan with new topN
        isCancelRequested = false
        stage = .scanning
        persistState()

        let scanResult: CLIResult
        do {
            scanResult = try await cliRunner.execute(.scan(topN: topN))
        } catch {
            handleUnexpectedError(error.localizedDescription)
            return
        }
        guard scanResult.exitCode == 0,
              let scanResponse = try? cliRunner.parseJSON(ScanResponse.self, from: scanResult.stdout),
              !scanResponse.candidates.isEmpty else {
            stage = .idle
            return
        }
        files = scanResponse.candidates.map { FileState(filename: $0.filename, status: .pending) }

        stage = .converting
        let convertResult: CLIResult
        do {
            convertResult = try await cliRunner.execute(.convert(topN: topN, yes: true))
        } catch {
            handleUnexpectedError(error.localizedDescription)
            return
        }
        guard convertResult.exitCode == 0,
              let convertResponse = try? cliRunner.parseJSON(ConvertResponse.self, from: convertResult.stdout),
              let taskId = convertResponse.taskId else {
            handleUnexpectedError("convert 失敗")
            return
        }

        stage = .polling
        persistState(taskIds: [taskId])
        await pollUntilComplete(taskId: taskId)
        if isCancelRequested { finishCancel(); return }

        stage = .importing
        let importResult = try? await cliRunner.execute(.importAll(deleteOriginal: true))
        if let r = importResult, r.exitCode == 0,
           let resp = try? cliRunner.parseJSON(ImportResponse.self, from: r.stdout) {
            processImportResults(resp)
        }
        stage = .idle
        try? stateStore.clear()
        updateSummary()
    }

    private func pollUntilComplete(taskId: String) async {
        lastStatusChangeTime = Date()
        lastPolledStatus = nil

        while !isCancelRequested {
            let result: CLIResult
            do {
                result = try await cliRunner.execute(.status(taskId: taskId))
            } catch {
                break
            }

            if result.exitCode != 0 {
                let cliError = cliRunner.classifyError(result: result)
                if case .authExpired = cliError {
                    notificationManager.sendAuthExpired()
                    handleUnexpectedError("AWS 認証期限切れ")
                    return
                }
                break
            }

            if let statusResponse = try? cliRunner.parseJSON(StatusResponse.self, from: result.stdout),
               let task = statusResponse.tasks.first {
                // Update file states from polling
                if let taskFiles = task.files {
                    for tf in taskFiles {
                        if let idx = files.firstIndex(where: { $0.filename == tf.filename }) {
                            files[idx].progressPercentage = tf.progressPercentage
                            if tf.status == "COMPLETED" { files[idx].status = .success }
                            else if tf.status == "FAILED" {
                                files[idx].status = .failed
                                files[idx].errorReason = tf.errorMessage
                            } else { files[idx].status = .processing }
                        }
                    }
                }
                updateSummary()

                // Check stuck (30 min)
                let currentStatus = task.status
                if currentStatus != lastPolledStatus {
                    lastPolledStatus = currentStatus
                    lastStatusChangeTime = Date()
                } else if let changeTime = lastStatusChangeTime,
                          Date().timeIntervalSince(changeTime) > 1800 {
                    errorMessage = "タスクが30分以上同じ状態です"
                }

                // Check if all done
                if task.status == "COMPLETED" || task.status == "FAILED" {
                    break
                }
            }

            // Wait 60 seconds
            try? await Task.sleep(nanoseconds: 60_000_000_000)
        }
    }

    private func processImportResults(_ response: ImportResponse) {
        for r in response.results {
            if let idx = files.firstIndex(where: { $0.filename == r.originalFilename || $0.filename == r.convertedFilename }) {
                files[idx].status = r.success ? .success : .failed
                files[idx].errorReason = r.errorMessage
            }
            if r.success, let deleteError = r.originalDeleteError {
                notificationManager.sendDeleteOriginalFailed(filename: r.originalFilename)
                if let idx = files.firstIndex(where: { $0.filename == r.originalFilename }) {
                    files[idx].errorReason = "import成功、削除失敗: \(deleteError)"
                }
            }
        }
        updateSummary()
    }

    private func handleCLIError(_ result: CLIResult) {
        let cliError = cliRunner.classifyError(result: result)
        switch cliError {
        case .authExpired:
            notificationManager.sendAuthExpired()
            errorMessage = "AWS 認証期限切れ"
        case .diskSpace:
            notificationManager.sendDiskSpaceInsufficient()
            errorMessage = "ディスク容量不足"
        case .networkError:
            errorMessage = "ネットワークエラー"
        case .executionFailed(_, let msg):
            errorMessage = msg.isEmpty ? "予期しないエラー" : String(msg.prefix(200))
        default:
            errorMessage = "予期しないエラー"
        }
        stage = .error
        persistState()
    }

    private func handleUnexpectedError(_ message: String) {
        errorMessage = message
        stage = .error
        persistState()
    }

    private func finishCancel() {
        isCancelRequested = false
        stage = .idle
        persistState()
    }

    private func persistState(taskIds: [String] = []) {
        let state = PipelineState(
            stage: stage,
            taskIds: taskIds,
            files: files,
            lastUpdated: Date(),
            topNUsed: nil,
            lastPolledStatus: lastPolledStatus,
            lastStatusChangeTime: lastStatusChangeTime
        )
        try? stateStore.save(state)
    }

    private func updateSummary() {
        let success = files.filter { $0.status == .success }.count
        let failed = files.filter { $0.status == .failed }.count
        let processing = files.filter { $0.status == .processing }.count
        let total = files.count
        if total == 0 {
            summaryText = ""
        } else {
            summaryText = "\(success)/\(total) 完了, \(failed) 失敗, \(processing) 処理中"
        }
    }
}
