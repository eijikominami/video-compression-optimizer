import Foundation

/// Command to add an existing video to albums.
public struct AddToAlbumsCommand {
    
    /// Execute the add_to_albums command.
    /// - Parameter args: Command arguments containing uuid and album_names
    /// - Returns: CommandResponse with success status
    public static func execute(args: CommandArgs) async -> CommandResponse<Bool> {
        // Extract uuid
        guard let uuid = args.uuid else {
            return .failure(ErrorInfo(
                type: .unknown,
                message: "Missing required parameter: uuid"
            ))
        }
        
        // Extract album_names
        guard let albumNames = args.albumNames, !albumNames.isEmpty else {
            return .failure(ErrorInfo(
                type: .unknown,
                message: "Missing or empty required parameter: album_names"
            ))
        }
        
        do {
            try await VideoImporter.shared.addToAlbums(assetIdentifier: uuid, albumNames: albumNames)
            return .success(true)
        } catch let error as PhotosError {
            return .failure(error.errorInfo)
        } catch {
            return .failure(ErrorInfo(
                type: .unknown,
                message: "Failed to add to albums: \(error.localizedDescription)"
            ))
        }
    }
}
