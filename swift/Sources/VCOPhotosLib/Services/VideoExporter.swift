import Foundation
import Photos

/// Handles exporting videos from the Photos library to a file path.
public class VideoExporter {
    
    // MARK: - Singleton
    
    public static let shared = VideoExporter()
    
    private init() {}
    
    // MARK: - Export
    
    /// Export a video to a destination path.
    /// - Parameters:
    ///   - uuid: The localIdentifier of the video to export
    ///   - destination: The destination file path
    /// - Returns: The path to the exported file
    public func exportVideo(byUUID uuid: String, to destination: String) async throws -> String {
        // Request read authorization
        _ = try await PhotosManager.shared.requestReadAuthorization()
        
        // Find the asset
        let asset = try PhotosManager.shared.findAsset(byUUID: uuid)
        
        // Get the video URL
        let videoURL = try await getVideoURL(for: asset)
        
        // Create destination URL
        let destinationURL = URL(fileURLWithPath: destination)
        
        // Create destination directory if needed
        let destinationDir = destinationURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: destinationDir, withIntermediateDirectories: true)
        
        // Remove existing file if present
        if FileManager.default.fileExists(atPath: destination) {
            try FileManager.default.removeItem(atPath: destination)
        }
        
        // Copy the file
        try FileManager.default.copyItem(at: videoURL, to: destinationURL)
        
        return destination
    }
    
    /// Get the video URL for an asset.
    private func getVideoURL(for asset: PHAsset) async throws -> URL {
        return try await withCheckedThrowingContinuation { continuation in
            let options = PHVideoRequestOptions()
            options.version = .current
            options.deliveryMode = .highQualityFormat
            options.isNetworkAccessAllowed = true
            
            PHImageManager.default().requestAVAsset(forVideo: asset, options: options) { avAsset, _, info in
                if let error = info?[PHImageErrorKey] as? Error {
                    continuation.resume(throwing: PhotosError.exportFailed(error.localizedDescription))
                    return
                }
                
                guard let urlAsset = avAsset as? AVURLAsset else {
                    continuation.resume(throwing: PhotosError.exportFailed("Failed to get video URL"))
                    return
                }
                
                continuation.resume(returning: urlAsset.url)
            }
        }
    }
}
