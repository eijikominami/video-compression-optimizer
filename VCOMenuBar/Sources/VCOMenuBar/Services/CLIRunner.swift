import Foundation

enum CLICommand {
    case scan(topN: Int?)
    case convert(topN: Int?, yes: Bool)
    case status(taskId: String?)
    case importAll(deleteOriginal: Bool)
}

struct CLIResult {
    let exitCode: Int32
    let stdout: Data
    let stderr: Data
}

enum CLIError: Error {
    case vcoNotFound
    case executionFailed(Int32, String)
    case jsonParseError(String)
    case diskSpace
    case networkError
    case authExpired
}

class CLIRunner {
    let vcoPath: String

    init(vcoPath: String? = nil) {
        self.vcoPath = vcoPath ?? Self.findVCO()
    }

    static func findVCO() -> String {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/which")
        process.arguments = ["vco"]
        let pipe = Pipe()
        process.standardOutput = pipe
        try? process.run()
        process.waitUntilExit()
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        let path = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
        return path ?? "/opt/homebrew/bin/vco"
    }

    func validate() -> Bool {
        FileManager.default.isExecutableFile(atPath: vcoPath)
    }

    func buildArguments(for command: CLICommand) -> [String] {
        switch command {
        case .scan(let topN):
            var args = ["scan", "--json"]
            if let n = topN { args += ["--top-n", String(n)] }
            return args
        case .convert(let topN, let yes):
            var args = ["convert", "--json"]
            if let n = topN { args += ["--top-n", String(n)] }
            if yes { args.append("--yes") }
            return args
        case .status(let taskId):
            var args = ["status", "--json"]
            if let id = taskId { args.append(id) }
            return args
        case .importAll(let deleteOriginal):
            var args = ["import", "--all", "--yes", "--json"]
            if deleteOriginal { args.append("--delete-original") }
            return args
        }
    }

    func execute(_ command: CLICommand) async throws -> CLIResult {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: vcoPath)
        process.arguments = buildArguments(for: command)

        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe

        try process.run()

        // Read data before waitUntilExit to avoid pipe buffer deadlock
        let stdout = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
        let stderr = stderrPipe.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()

        return CLIResult(exitCode: process.terminationStatus, stdout: stdout, stderr: stderr)
    }

    func classifyError(result: CLIResult) -> CLIError {
        let stderrStr = String(data: result.stderr, encoding: .utf8) ?? ""
        if stderrStr.contains("disk space") || stderrStr.contains("No space left") {
            return .diskSpace
        }
        if stderrStr.contains("expired") || stderrStr.contains("credentials") || stderrStr.contains("UnauthorizedAccess") {
            return .authExpired
        }
        if stderrStr.contains("network") || stderrStr.contains("timeout") || stderrStr.contains("ConnectionError") {
            return .networkError
        }
        return .executionFailed(result.exitCode, stderrStr)
    }

    func parseJSON<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        do {
            return try JSONDecoder().decode(type, from: data)
        } catch {
            throw CLIError.jsonParseError(error.localizedDescription)
        }
    }
}
