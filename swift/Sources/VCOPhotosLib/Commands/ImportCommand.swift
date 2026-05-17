import Foundation

/// Handles the import command to import videos into Photos library.
public struct ImportCommand {
    
    /// Execute the import command.
    /// - Parameter args: Command arguments (path required, album_names optional, capture_date optional)
    /// - Returns: CommandResponse with imported video's localIdentifier
    public static func execute(args: CommandArgs) async -> CommandResponse<String> {
        guard let path = args.path else {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: "Missing required parameter: path"))
        }
        
        let fileURL = URL(fileURLWithPath: path)
        
        // Parse capture_date if provided (ISO 8601 format)
        var captureDate: Date? = nil
        if let captureDateStr = args.captureDate {
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            captureDate = formatter.date(from: captureDateStr)
            if captureDate == nil {
                // Try without fractional seconds
                formatter.formatOptions = [.withInternetDateTime]
                captureDate = formatter.date(from: captureDateStr)
            }
        }
        
        do {
            let localIdentifier = try await VideoImporter.shared.importVideo(
                from: fileURL,
                albumNames: args.albumNames,
                captureDate: captureDate
            )
            return CommandResponse.success(localIdentifier)
        } catch let error as PhotosError {
            return CommandResponse.failure(error.errorInfo)
        } catch {
            return CommandResponse.failure(ErrorInfo(type: .unknown, message: error.localizedDescription))
        }
    }
}
