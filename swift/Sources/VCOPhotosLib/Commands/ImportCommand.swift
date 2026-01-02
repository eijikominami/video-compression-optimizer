import Foundation

/// Handles the import command to import videos into Photos library.
public struct ImportCommand {
    
    /// Execute the import command.
    /// - Parameter args: Command arguments (path required, album_names optional)
    /// - Returns: CommandResponse with imported video's localIdentifier
    public static func execute(args: CommandArgs) async -> CommandResponse<String> {
        guard let path = args.path else {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: "Missing required parameter: path"))
        }
        
        let fileURL = URL(fileURLWithPath: path)
        
        do {
            let localIdentifier = try await VideoImporter.shared.importVideo(
                from: fileURL,
                albumNames: args.albumNames
            )
            return CommandResponse.success(localIdentifier)
        } catch let error as PhotosError {
            return CommandResponse.failure(error.errorInfo)
        } catch {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: error.localizedDescription))
        }
    }
}
