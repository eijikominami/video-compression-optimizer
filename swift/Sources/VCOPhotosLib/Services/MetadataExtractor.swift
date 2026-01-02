import Foundation
import AVFoundation
import Photos

/// Extracts detailed metadata from video assets using AVFoundation.
public class MetadataExtractor {
    
    // MARK: - Singleton
    
    public static let shared = MetadataExtractor()
    
    private init() {}
    
    // MARK: - Metadata Extraction
    
    /// Extract detailed metadata from an AVAsset.
    /// - Parameter avAsset: The AVAsset to extract metadata from
    /// - Returns: Tuple containing codec, bitrate, and frame rate
    public func extractMetadata(from avAsset: AVAsset) async -> (codec: String, bitrate: Int, frameRate: Double) {
        var codec = "unknown"
        var bitrate = 0
        var frameRate = 0.0
        
        // Get video track
        do {
            let videoTracks = try await avAsset.loadTracks(withMediaType: .video)
            
            if let videoTrack = videoTracks.first {
                // Extract codec
                codec = await extractCodec(from: videoTrack)
                
                // Extract frame rate
                frameRate = await extractFrameRate(from: videoTrack)
                
                // Extract bitrate
                bitrate = await extractBitrate(from: avAsset, videoTrack: videoTrack)
            }
        } catch {
            // Return defaults if extraction fails
        }
        
        return (codec, bitrate, frameRate)
    }
    
    /// Extract codec information from a video track.
    private func extractCodec(from track: AVAssetTrack) async -> String {
        do {
            let formatDescriptions = try await track.load(.formatDescriptions)
            
            guard let formatDescription = formatDescriptions.first else {
                return "unknown"
            }
            
            let mediaSubType = CMFormatDescriptionGetMediaSubType(formatDescription)
            return codecName(from: mediaSubType)
        } catch {
            return "unknown"
        }
    }
    
    /// Convert FourCC code to codec name.
    private func codecName(from fourCC: FourCharCode) -> String {
        switch fourCC {
        case kCMVideoCodecType_H264:
            return "h264"
        case kCMVideoCodecType_HEVC:
            return "hevc"
        case kCMVideoCodecType_MPEG4Video:
            return "mpeg4"
        case kCMVideoCodecType_AppleProRes422:
            return "prores422"
        case kCMVideoCodecType_AppleProRes422HQ:
            return "prores422hq"
        case kCMVideoCodecType_AppleProRes422LT:
            return "prores422lt"
        case kCMVideoCodecType_AppleProRes422Proxy:
            return "prores422proxy"
        case kCMVideoCodecType_AppleProRes4444:
            return "prores4444"
        case kCMVideoCodecType_AppleProRes4444XQ:
            return "prores4444xq"
        case kCMVideoCodecType_AppleProResRAW:
            return "proresraw"
        case kCMVideoCodecType_AppleProResRAWHQ:
            return "proresrawhq"
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
    
    /// Extract frame rate from a video track.
    private func extractFrameRate(from track: AVAssetTrack) async -> Double {
        do {
            let nominalFrameRate = try await track.load(.nominalFrameRate)
            return Double(nominalFrameRate)
        } catch {
            return 0.0
        }
    }
    
    /// Extract bitrate from an AVAsset.
    private func extractBitrate(from asset: AVAsset, videoTrack: AVAssetTrack) async -> Int {
        do {
            // Try to get estimated data rate from track
            let estimatedDataRate = try await videoTrack.load(.estimatedDataRate)
            if estimatedDataRate > 0 {
                return Int(estimatedDataRate)
            }
            
            // Fallback: calculate from file size and duration
            let duration = try await asset.load(.duration)
            let durationSeconds = CMTimeGetSeconds(duration)
            
            if durationSeconds > 0 {
                // We need file size from PHAssetResource, which is handled elsewhere
                // Return 0 as fallback
                return 0
            }
        } catch {
            // Return 0 if extraction fails
        }
        
        return 0
    }
    
    // MARK: - PHAsset Integration
    
    /// Request AVAsset from PHAsset and extract metadata.
    /// - Parameter phAsset: The PHAsset to extract metadata from
    /// - Returns: Tuple containing codec, bitrate, and frame rate
    public func extractMetadata(from phAsset: PHAsset) async throws -> (codec: String, bitrate: Int, frameRate: Double) {
        return try await withCheckedThrowingContinuation { continuation in
            let options = PHVideoRequestOptions()
            options.version = .current
            options.deliveryMode = .highQualityFormat
            options.isNetworkAccessAllowed = false  // Don't download from iCloud
            
            PHImageManager.default().requestAVAsset(forVideo: phAsset, options: options) { avAsset, _, info in
                if let error = info?[PHImageErrorKey] as? Error {
                    continuation.resume(throwing: PhotosError.metadataExtractionFailed(error.localizedDescription))
                    return
                }
                
                guard let avAsset = avAsset else {
                    // Asset not locally available (might be in iCloud)
                    continuation.resume(returning: ("unknown", 0, 0.0))
                    return
                }
                
                Task {
                    let metadata = await self.extractMetadata(from: avAsset)
                    continuation.resume(returning: metadata)
                }
            }
        }
    }
}
