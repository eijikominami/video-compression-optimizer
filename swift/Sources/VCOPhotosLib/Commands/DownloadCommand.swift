import Foundation

/// Handles the download command to download videos from iCloud.
public struct DownloadCommand {
    
    /// Execute the download command.
    /// - Parameter args: Command arguments (uuid required)
    /// - Returns: CommandResponse with downloaded file path
    public static func execute(args: CommandArgs) async -> CommandResponse<String> {
        guard let uuid = args.uuid else {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: "Missing required parameter: uuid"))
        }
        
        do {
            let url = try await ICloudDownloader.shared.downloadVideo(byUUID: uuid)
            return CommandResponse.success(url.path)
        } catch let error as PhotosError {
            return CommandResponse.failure(error.errorInfo)
        } catch {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: error.localizedDescription))
        }
    }
}
