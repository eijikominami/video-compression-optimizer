import Foundation
import Photos
import AVFoundation
import CoreLocation
import CoreMedia

/// Manages access to the Photos library using PhotoKit.
public class PhotosManager {
    
    // MARK: - Singleton
    
    public static let shared = PhotosManager()
    
    private init() {}
    
    // MARK: - Authorization
    
    /// Request read authorization for Photos library.
    /// - Returns: Current authorization status after request
    public func requestReadAuthorization() async throws -> PHAuthorizationStatus {
        let status = await PHPhotoLibrary.requestAuthorization(for: .readWrite)
        
        if status == .denied || status == .restricted {
            throw PhotosError.authorizationDenied("Photos library read access was denied")
        }
        
        return status
    }
    
    /// Request write authorization for Photos library.
    /// - Returns: Current authorization status after request
    public func requestWriteAuthorization() async throws -> PHAuthorizationStatus {
        let status = await PHPhotoLibrary.requestAuthorization(for: .readWrite)
        
        if status == .denied || status == .restricted {
            throw PhotosError.authorizationDenied("Photos library write access was denied")
        }
        
        return status
    }
    
    /// Check current authorization status without requesting.
    public func checkAuthorizationStatus() -> PHAuthorizationStatus {
        PHPhotoLibrary.authorizationStatus(for: .readWrite)
    }
    
    // MARK: - Video Fetching
    
    /// Fetch all videos from Photos library.
    /// - Returns: Array of VideoInfo objects
    public func getAllVideos() async throws -> [VideoInfo] {
        _ = try await requestReadAuthorization()
        
        let fetchOptions = PHFetchOptions()
        fetchOptions.predicate = NSPredicate(format: "mediaType == %d", PHAssetMediaType.video.rawValue)
        fetchOptions.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
        
        let assets = PHAsset.fetchAssets(with: fetchOptions)
        
        return await extractVideoInfoFromAssets(assets)
    }
    
    /// Fetch videos within a date range.
    /// - Parameters:
    ///   - fromDate: Start date (inclusive), nil for no lower bound
    ///   - toDate: End date (inclusive), nil for no upper bound
    /// - Returns: Array of VideoInfo objects
    public func getVideosByDateRange(from fromDate: Date?, to toDate: Date?) async throws -> [VideoInfo] {
        _ = try await requestReadAuthorization()
        
        let fetchOptions = PHFetchOptions()
        var predicates: [NSPredicate] = [
            NSPredicate(format: "mediaType == %d", PHAssetMediaType.video.rawValue)
        ]
        
        if let fromDate = fromDate {
            predicates.append(NSPredicate(format: "creationDate >= %@", fromDate as NSDate))
        }
        
        if let toDate = toDate {
            predicates.append(NSPredicate(format: "creationDate <= %@", toDate as NSDate))
        }
        
        fetchOptions.predicate = NSCompoundPredicate(andPredicateWithSubpredicates: predicates)
        fetchOptions.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
        
        let assets = PHAsset.fetchAssets(with: fetchOptions)
        
        return await extractVideoInfoFromAssets(assets)
    }
    
    /// Find a video by UUID (localIdentifier).
    /// - Parameter uuid: The normalized UUID (without /L0/001 suffix)
    /// - Returns: The PHAsset if found
    public func findAsset(byUUID uuid: String) throws -> PHAsset {
        // Try with the normalized UUID first, then with /L0/001 suffix
        let identifiersToTry = [uuid, "\(uuid)/L0/001"]
        
        for identifier in identifiersToTry {
            let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [identifier], options: nil)
            if let asset = fetchResult.firstObject {
                return asset
            }
        }
        
        throw PhotosError.notFound("Video not found: \(uuid)")
    }
    
    // MARK: - Private Helpers
    
    /// Extract VideoInfo from a fetch result.
    private func extractVideoInfoFromAssets(_ assets: PHFetchResult<PHAsset>) async -> [VideoInfo] {
        var videos: [VideoInfo] = []
        
        assets.enumerateObjects { asset, _, _ in
            if let videoInfo = self.extractVideoInfo(from: asset) {
                videos.append(videoInfo)
            }
        }
        
        return videos
    }
    
    /// Extract VideoInfo from a single PHAsset.
    private func extractVideoInfo(from asset: PHAsset) -> VideoInfo? {
        // Normalize UUID by removing /L0/001 suffix to match Python implementation
        let uuid = normalizeUUID(asset.localIdentifier)
        let filename = getFilename(for: asset)
        let path = getPath(for: asset)
        let albums = getAlbums(for: asset)
        let (isInIcloud, isLocal) = getICloudStatus(for: asset)
        let location = getLocation(for: asset)
        let creationDate = DateFormatting.formatOptional(asset.creationDate) ?? DateFormatting.format(Date())
        let captureDate = DateFormatting.formatOptional(asset.creationDate)
        
        // Get video-specific metadata
        let resources = PHAssetResource.assetResources(for: asset)
        let fileSize = getFileSize(from: resources)
        
        // Resolution from PHAsset
        let resolution = [asset.pixelWidth, asset.pixelHeight]
        let duration = asset.duration
        
        // Extract codec, bitrate, frame_rate synchronously using semaphore
        var codec = "unknown"
        var bitrate = 0
        var frameRate = 0.0
        
        // Only try to extract metadata if file is locally available
        if isLocal {
            let semaphore = DispatchSemaphore(value: 0)
            
            let options = PHVideoRequestOptions()
            options.version = .current
            options.deliveryMode = .highQualityFormat
            options.isNetworkAccessAllowed = false
            
            PHImageManager.default().requestAVAsset(forVideo: asset, options: options) { avAsset, _, _ in
                defer { semaphore.signal() }
                
                guard let avAsset = avAsset else { return }
                
                // Extract metadata synchronously from AVAsset
                if let videoTrack = avAsset.tracks(withMediaType: .video).first {
                    // Extract codec
                    if let formatDescription = videoTrack.formatDescriptions.first {
                        let mediaSubType = CMFormatDescriptionGetMediaSubType(formatDescription as! CMFormatDescription)
                        codec = self.codecName(from: mediaSubType)
                    }
                    
                    // Extract frame rate
                    frameRate = Double(videoTrack.nominalFrameRate)
                    
                    // Extract bitrate
                    bitrate = Int(videoTrack.estimatedDataRate)
                }
            }
            
            // Wait with timeout (100ms max per asset to avoid blocking)
            _ = semaphore.wait(timeout: .now() + 0.1)
        }
        
        return VideoInfo(
            uuid: uuid,
            filename: filename,
            path: path,
            codec: codec,
            resolution: resolution,
            bitrate: bitrate,
            duration: duration,
            frameRate: frameRate,
            fileSize: fileSize,
            captureDate: captureDate,
            creationDate: creationDate,
            albums: albums,
            isInIcloud: isInIcloud,
            isLocal: isLocal,
            location: location
        )
    }
    
    /// Convert FourCC code to codec name (matching Python osxphotos format).
    private func codecName(from fourCC: FourCharCode) -> String {
        switch fourCC {
        case kCMVideoCodecType_H264:
            return "avc1"  // Match osxphotos format
        case kCMVideoCodecType_HEVC:
            return "hvc1"  // Match osxphotos format
        case kCMVideoCodecType_MPEG4Video:
            return "mp4v"
        case kCMVideoCodecType_AppleProRes422:
            return "apcn"
        case kCMVideoCodecType_AppleProRes422HQ:
            return "apch"
        case kCMVideoCodecType_AppleProRes422LT:
            return "apcs"
        case kCMVideoCodecType_AppleProRes422Proxy:
            return "apco"
        case kCMVideoCodecType_AppleProRes4444:
            return "ap4h"
        case kCMVideoCodecType_AppleProRes4444XQ:
            return "ap4x"
        default:
            // Convert FourCC to string
            let chars = [
                Character(UnicodeScalar((fourCC >> 24) & 0xFF)!),
                Character(UnicodeScalar((fourCC >> 16) & 0xFF)!),
                Character(UnicodeScalar((fourCC >> 8) & 0xFF)!),
                Character(UnicodeScalar(fourCC & 0xFF)!)
            ]
            return String(chars).trimmingCharacters(in: .whitespaces).lowercased()
        }
    }
    
    /// Get filename for an asset.
    private func getFilename(for asset: PHAsset) -> String {
        let resources = PHAssetResource.assetResources(for: asset)
        return resources.first?.originalFilename ?? "unknown"
    }
    
    /// Get file path for an asset (if locally available).
    private func getPath(for asset: PHAsset) -> String {
        let resources = PHAssetResource.assetResources(for: asset)
        
        // Try to get the path from the primary resource
        guard resources.first(where: { $0.type == .video }) != nil || resources.first != nil else {
            return ""
        }
        
        // PHAssetResource doesn't directly expose the file path
        // We return empty string and populate it when actually accessing the file
        return ""
    }
    
    /// Get albums containing the asset.
    private func getAlbums(for asset: PHAsset) -> [String] {
        var albumNames: [String] = []
        
        let collections = PHAssetCollection.fetchAssetCollectionsContaining(
            asset,
            with: .album,
            options: nil
        )
        
        collections.enumerateObjects { collection, _, _ in
            if let title = collection.localizedTitle {
                albumNames.append(title)
            }
        }
        
        return albumNames
    }
    
    /// Get iCloud status for an asset.
    /// Matches Python osxphotos behavior:
    /// - is_in_icloud: True if asset is a cloud asset (photo.iscloudasset)
    /// - is_local: True if local file path exists and is accessible
    private func getICloudStatus(for asset: PHAsset) -> (isInIcloud: Bool, isLocal: Bool) {
        let resources = PHAssetResource.assetResources(for: asset)
        
        // Check resource types
        let hasFullSizeVideo = resources.contains { $0.type == .fullSizeVideo }
        let hasLocalVideo = resources.contains { $0.type == .video }
        
        // osxphotos.iscloudasset returns True if the asset is in an iCloud library
        // This is True for most assets when iCloud Photos is enabled
        // The key indicator is the presence of .fullSizeVideo resource type
        // which means the original is stored in iCloud
        let isInIcloud = hasFullSizeVideo
        
        // is_local: True only if the file is actually downloaded locally
        // When .fullSizeVideo exists, the local copy is NOT available
        // (the .video resource in this case is just a placeholder/thumbnail)
        // Only when .video exists WITHOUT .fullSizeVideo is the file truly local
        let isLocal = hasLocalVideo && !hasFullSizeVideo
        
        return (isInIcloud, isLocal)
    }
    
    /// Get location for an asset.
    private func getLocation(for asset: PHAsset) -> [Double]? {
        guard let location = asset.location else {
            return nil
        }
        
        return [location.coordinate.latitude, location.coordinate.longitude]
    }
    
    /// Get file size from resources.
    private func getFileSize(from resources: [PHAssetResource]) -> Int {
        guard let resource = resources.first(where: { $0.type == .video }) ?? resources.first else {
            return 0
        }
        
        // Try to get file size from resource
        if let fileSize = resource.value(forKey: "fileSize") as? Int {
            return fileSize
        }
        
        return 0
    }
    
    /// Normalize UUID by removing /L0/001 suffix to match Python implementation.
    /// PhotoKit localIdentifier format: "UUID/L0/001" -> "UUID"
    private func normalizeUUID(_ localIdentifier: String) -> String {
        if let slashIndex = localIdentifier.firstIndex(of: "/") {
            return String(localIdentifier[..<slashIndex])
        }
        return localIdentifier
    }
}
