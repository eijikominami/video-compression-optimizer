import Foundation
import Photos

/// Handles downloading videos from iCloud.
public class ICloudDownloader {
    
    // MARK: - Singleton
    
    public static let shared = ICloudDownloader()
    
    private init() {}
    
    // MARK: - Progress Tracking
    
    /// State for throttling progress output
    private var lastProgressPercent: Int = -1
    private var lastProgressTime: Date = Date.distantPast
    
    /// Output progress JSON to stdout
    private func outputProgress(uuid: String, percent: Int) {
        let progressJson = "{\"type\":\"progress\",\"uuid\":\"\(uuid)\",\"percent\":\(percent)}"
        print(progressJson)
        fflush(stdout)
    }
    
    // MARK: - Download
    
    /// Download a video from iCloud.
    /// - Parameters:
    ///   - asset: The PHAsset to download
    ///   - uuid: The UUID for progress reporting
    ///   - progressHandler: Optional handler for progress updates (0.0 to 1.0)
    ///   - timeout: Timeout in seconds (default: 300)
    /// - Returns: URL to the downloaded video file
    public func downloadVideo(
        asset: PHAsset,
        uuid: String,
        progressHandler: ((Double) -> Void)? = nil,
        timeout: TimeInterval = 300
    ) async throws -> URL {
        // Reset progress tracking state
        lastProgressPercent = -1
        lastProgressTime = Date.distantPast
        
        return try await withCheckedThrowingContinuation { continuation in
            let options = PHVideoRequestOptions()
            options.version = .current
            options.deliveryMode = .highQualityFormat
            options.isNetworkAccessAllowed = true
            
            // Set up progress handler
            options.progressHandler = { [self] progress, error, _, _ in
                if let error = error {
                    // Report progress error to stderr
                    FileHandle.standardError.write(
                        "Download error: \(error.localizedDescription)\n".data(using: .utf8)!
                    )
                } else {
                    let progressPercent = Int(progress * 100)
                    let now = Date()
                    
                    // Output progress if changed by 5% or 2 seconds elapsed
                    if progressPercent - lastProgressPercent >= 5 ||
                       now.timeIntervalSince(lastProgressTime) >= 2.0 ||
                       progressPercent == 100 {
                        outputProgress(uuid: uuid, percent: progressPercent)
                        lastProgressPercent = progressPercent
                        lastProgressTime = now
                    }
                    
                    progressHandler?(progress)
                }
            }
            
            // Create a timeout task
            let timeoutTask = Task {
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                continuation.resume(throwing: PhotosError.timeout("Download timed out after \(Int(timeout)) seconds"))
            }
            
            PHImageManager.default().requestAVAsset(forVideo: asset, options: options) { avAsset, _, info in
                // Cancel timeout task
                timeoutTask.cancel()
                
                // Check for errors
                if let error = info?[PHImageErrorKey] as? Error {
                    continuation.resume(throwing: PhotosError.icloudDownloadFailed(error.localizedDescription))
                    return
                }
                
                // Check if cancelled
                if let cancelled = info?[PHImageCancelledKey] as? Bool, cancelled {
                    continuation.resume(throwing: PhotosError.userCancelled("Download was cancelled"))
                    return
                }
                
                // Get the URL from AVURLAsset
                guard let urlAsset = avAsset as? AVURLAsset else {
                    continuation.resume(throwing: PhotosError.icloudDownloadFailed("Failed to get video URL"))
                    return
                }
                
                continuation.resume(returning: urlAsset.url)
            }
        }
    }
    
    /// Download a video by UUID.
    /// - Parameters:
    ///   - uuid: The localIdentifier of the video
    ///   - progressHandler: Optional handler for progress updates
    ///   - timeout: Timeout in seconds
    /// - Returns: URL to the downloaded video file
    public func downloadVideo(
        byUUID uuid: String,
        progressHandler: ((Double) -> Void)? = nil,
        timeout: TimeInterval = 300
    ) async throws -> URL {
        let asset = try PhotosManager.shared.findAsset(byUUID: uuid)
        return try await downloadVideo(asset: asset, uuid: uuid, progressHandler: progressHandler, timeout: timeout)
    }
}
