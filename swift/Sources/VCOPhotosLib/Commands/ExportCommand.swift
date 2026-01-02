import Foundation

/// Handles the export command to export videos from Photos library.
public struct ExportCommand {
    
    /// Execute the export command.
    /// - Parameter args: Command arguments (uuid and destination required)
    /// - Returns: CommandResponse with exported file path
    public static func execute(args: CommandArgs) async -> CommandResponse<String> {
        guard let uuid = args.uuid else {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: "Missing required parameter: uuid"))
        }
        
        guard let destination = args.destination else {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: "Missing required parameter: destination"))
        }
        
        do {
            let exportedPath = try await VideoExporter.shared.exportVideo(byUUID: uuid, to: destination)
            return CommandResponse.success(exportedPath)
        } catch let error as PhotosError {
            return CommandResponse.failure(error.errorInfo)
        } catch {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: error.localizedDescription))
        }
    }
}
