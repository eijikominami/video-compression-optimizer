import Foundation
import VCOPhotosLib

/// Main entry point for vco-photos CLI.
/// Reads JSON command from stdin, executes, and outputs JSON response to stdout.
@main
struct VCOPhotos {
    static func main() async {
        // Read JSON from stdin (read all available data)
        var inputData = Data()
        while let line = readLine(strippingNewline: false) {
            inputData.append(contentsOf: line.utf8)
        }
        
        let input = String(data: inputData, encoding: .utf8) ?? ""
        guard !input.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            outputError(ErrorInfo(type: .unknown, message: "No input provided"))
            Foundation.exit(1)
        }
        
        // Parse command request
        let request: CommandRequest
        do {
            request = try JSONCoding.decode(input, as: CommandRequest.self)
        } catch {
            outputError(ErrorInfo(type: .unknown, message: "Failed to parse command: \(error)"))
            Foundation.exit(1)
        }
        
        // Execute command
        let response = await executeCommand(request)
        
        // Output response
        do {
            let jsonString = try JSONCoding.encode(response)
            print(jsonString)
        } catch {
            outputError(ErrorInfo(type: .unknown, message: "Failed to encode response: \(error)"))
            Foundation.exit(1)
        }
        
        // Exit with appropriate code
        Foundation.exit(response.success ? 0 : 1)
    }
    
    /// Execute a command and return response
    static func executeCommand(_ request: CommandRequest) async -> AnyCommandResponse {
        switch request.command {
        case "scan":
            return await executeScan(request.args)
        case "import":
            return await executeImport(request.args)
        case "delete":
            return await executeDelete(request.args)
        case "export":
            return await executeExport(request.args)
        case "download":
            return await executeDownload(request.args)
        case "add_to_albums":
            return await executeAddToAlbums(request.args)
        default:
            return AnyCommandResponse.failure(
                ErrorInfo(type: .unknown, message: "Unknown command: \(request.command)")
            )
        }
    }
    
    /// Execute scan command
    static func executeScan(_ args: CommandArgs) async -> AnyCommandResponse {
        let response = await ScanCommand.execute(args: args)
        return AnyCommandResponse(from: response)
    }
    
    /// Execute import command
    static func executeImport(_ args: CommandArgs) async -> AnyCommandResponse {
        let response = await ImportCommand.execute(args: args)
        return AnyCommandResponse(from: response)
    }
    
    /// Execute delete command
    static func executeDelete(_ args: CommandArgs) async -> AnyCommandResponse {
        let response = await DeleteCommand.execute(args: args)
        return AnyCommandResponse(from: response)
    }
    
    /// Execute export command
    static func executeExport(_ args: CommandArgs) async -> AnyCommandResponse {
        let response = await ExportCommand.execute(args: args)
        return AnyCommandResponse(from: response)
    }
    
    /// Execute download command
    static func executeDownload(_ args: CommandArgs) async -> AnyCommandResponse {
        let response = await DownloadCommand.execute(args: args)
        return AnyCommandResponse(from: response)
    }
    
    /// Execute add_to_albums command
    static func executeAddToAlbums(_ args: CommandArgs) async -> AnyCommandResponse {
        let response = await AddToAlbumsCommand.execute(args: args)
        return AnyCommandResponse(from: response)
    }
    
    /// Output error to stdout as JSON
    static func outputError(_ error: ErrorInfo) {
        let response = AnyCommandResponse.failure(error)
        if let jsonString = try? JSONCoding.encode(response) {
            print(jsonString)
        } else {
            // Fallback to raw JSON if encoding fails
            print("""
            {"success":false,"error":{"type":"\(error.type.rawValue)","message":"\(error.message)"}}
            """)
        }
    }
}
