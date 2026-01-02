import Foundation
import Photos

/// Handles deleting videos from the Photos library.
public class VideoDeleter {
    
    // MARK: - Singleton
    
    public static let shared = VideoDeleter()
    
    private init() {}
    
    // MARK: - Delete
    
    /// Delete a video from the Photos library.
    /// - Parameter uuid: The localIdentifier of the video to delete
    /// - Returns: true if deletion was successful
    public func deleteVideo(byUUID uuid: String) async throws -> Bool {
        // Request write authorization
        _ = try await PhotosManager.shared.requestWriteAuthorization()
        
        // Find the asset
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [uuid], options: nil)
        
        guard let asset = fetchResult.firstObject else {
            // Asset not found - treat as success (idempotent)
            return true
        }
        
        // Delete the asset
        do {
            try await PHPhotoLibrary.shared().performChanges {
                PHAssetChangeRequest.deleteAssets([asset] as NSFastEnumeration)
            }
            return true
        } catch let error as NSError {
            // Check if user cancelled
            if error.domain == "PHPhotosErrorDomain" && error.code == 3072 {
                throw PhotosError.userCancelled("User cancelled the deletion")
            }
            throw PhotosError.deleteFailed(error.localizedDescription)
        }
    }
}
