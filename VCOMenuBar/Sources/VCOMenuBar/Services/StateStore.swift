import Foundation

enum StateStoreError: Error {
    case writeError(String)
}

class StateStore {
    let path: String

    init(path: String? = nil) {
        self.path = path ?? NSString("~/.config/vco/menubar_state.json").expandingTildeInPath
    }

    func save(_ state: PipelineState) throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = .prettyPrinted
        let data = try encoder.encode(state)
        let url = URL(fileURLWithPath: path)
        try FileManager.default.createDirectory(
            at: url.deletingLastPathComponent(), withIntermediateDirectories: true
        )
        try data.write(to: url)
    }

    func load() throws -> PipelineState? {
        let url = URL(fileURLWithPath: path)
        guard FileManager.default.fileExists(atPath: path) else { return nil }
        let data = try Data(contentsOf: url)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(PipelineState.self, from: data)
    }

    func clear() throws {
        let url = URL(fileURLWithPath: path)
        if FileManager.default.fileExists(atPath: path) {
            try FileManager.default.removeItem(at: url)
        }
    }
}
