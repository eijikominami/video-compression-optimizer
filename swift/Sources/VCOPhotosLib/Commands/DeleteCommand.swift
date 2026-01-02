import Foundation

/// Handles the delete command to delete videos from Photos library.
public struct DeleteCommand {
    
    /// Execute the delete command.
    /// - Parameter args: Command arguments (uuid required)
    /// - Returns: CommandResponse with success boolean
    public static func execute(args: CommandArgs) async -> CommandResponse<Bool> {
        guard let uuid = args.uuid else {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: "Missing required parameter: uuid"))
        }
        
        do {
            let success = try await VideoDeleter.shared.deleteVideo(byUUID: uuid)
            return CommandResponse.success(success)
        } catch let error as PhotosError {
            return CommandResponse.failure(error.errorInfo)
        } catch {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: error.localizedDescription))
        }
    }
}
