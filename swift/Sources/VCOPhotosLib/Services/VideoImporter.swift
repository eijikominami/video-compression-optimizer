import Foundation
import Photos

/// Handles importing videos into the Photos library.
public class VideoImporter {
    
    // MARK: - Singleton
    
    public static let shared = VideoImporter()
    
    private init() {}
    
    // MARK: - Import
    
    /// Import a video file into the Photos library.
    /// - Parameters:
    ///   - fileURL: URL to the video file to import
    ///   - albumNames: Optional array of album names to add the video to
    /// - Returns: The localIdentifier of the imported video
    public func importVideo(from fileURL: URL, albumNames: [String]? = nil) async throws -> String {
        // Request write authorization
        _ = try await PhotosManager.shared.requestWriteAuthorization()
        
        // Check if file exists
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            throw PhotosError.fileNotFound("File not found: \(fileURL.path)")
        }
        
        // Import the video
        var localIdentifier: String?
        
        try await PHPhotoLibrary.shared().performChanges {
            let creationRequest = PHAssetCreationRequest.forAsset()
            creationRequest.addResource(with: .video, fileURL: fileURL, options: nil)
            localIdentifier = creationRequest.placeholderForCreatedAsset?.localIdentifier
        }
        
        guard let identifier = localIdentifier else {
            throw PhotosError.importFailed("Failed to get localIdentifier for imported video")
        }
        
        // Add to albums if specified
        if let albumNames = albumNames, !albumNames.isEmpty {
            try await addToAlbums(assetIdentifier: identifier, albumNames: albumNames)
        }
        
        return identifier
    }
    
    /// Add an asset to albums, creating them if necessary.
    /// - Parameters:
    ///   - assetIdentifier: The localIdentifier of the asset
    ///   - albumNames: Array of album names
    public func addToAlbums(assetIdentifier: String, albumNames: [String]) async throws {
        // Request write authorization
        _ = try await PhotosManager.shared.requestWriteAuthorization()
        // Fetch the asset
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [assetIdentifier], options: nil)
        guard let asset = fetchResult.firstObject else {
            throw PhotosError.notFound("Asset not found: \(assetIdentifier)")
        }
        
        for albumName in albumNames {
            // Find or create the album
            let album = try await findOrCreateAlbum(named: albumName)
            
            // Add asset to album
            try await PHPhotoLibrary.shared().performChanges {
                guard let albumChangeRequest = PHAssetCollectionChangeRequest(for: album) else {
                    return
                }
                albumChangeRequest.addAssets([asset] as NSFastEnumeration)
            }
        }
    }
    
    /// Find an existing album or create a new one.
    /// - Parameter name: Album name
    /// - Returns: The PHAssetCollection for the album
    private func findOrCreateAlbum(named name: String) async throws -> PHAssetCollection {
        // Search for existing album
        let fetchOptions = PHFetchOptions()
        fetchOptions.predicate = NSPredicate(format: "title == %@", name)
        let collections = PHAssetCollection.fetchAssetCollections(with: .album, subtype: .any, options: fetchOptions)
        
        if let existingAlbum = collections.firstObject {
            return existingAlbum
        }
        
        // Create new album
        var albumIdentifier: String?
        
        try await PHPhotoLibrary.shared().performChanges {
            let createRequest = PHAssetCollectionChangeRequest.creationRequestForAssetCollection(withTitle: name)
            albumIdentifier = createRequest.placeholderForCreatedAssetCollection.localIdentifier
        }
        
        guard let identifier = albumIdentifier else {
            throw PhotosError.importFailed("Failed to create album: \(name)")
        }
        
        // Fetch the created album
        let createdAlbums = PHAssetCollection.fetchAssetCollections(withLocalIdentifiers: [identifier], options: nil)
        guard let album = createdAlbums.firstObject else {
            throw PhotosError.importFailed("Failed to fetch created album: \(name)")
        }
        
        return album
    }
}
