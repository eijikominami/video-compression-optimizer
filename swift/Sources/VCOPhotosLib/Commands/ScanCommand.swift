import Foundation

/// Handles the scan command to retrieve videos from Photos library.
public struct ScanCommand {
    
    /// Execute the scan command.
    /// - Parameter args: Command arguments (from_date, to_date optional)
    /// - Returns: CommandResponse with array of VideoInfo
    public static func execute(args: CommandArgs) async -> CommandResponse<[VideoInfo]> {
        do {
            let videos: [VideoInfo]
            
            // Check for date range parameters
            let fromDate = args.fromDate.flatMap { DateFormatting.parse($0) }
            let toDate = args.toDate.flatMap { DateFormatting.parse($0) }
            
            if fromDate != nil || toDate != nil {
                videos = try await PhotosManager.shared.getVideosByDateRange(from: fromDate, to: toDate)
            } else {
                videos = try await PhotosManager.shared.getAllVideos()
            }
            
            return CommandResponse.success(videos)
        } catch let error as PhotosError {
            return CommandResponse.failure(error.errorInfo)
        } catch {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: error.localizedDescription))
        }
    }
}
