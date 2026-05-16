import Foundation

struct VCOConfig: Codable {
    struct AWS: Codable {
        let s3Bucket: String?
        let roleArn: String?
        let region: String?
        let profile: String?

        enum CodingKeys: String, CodingKey {
            case s3Bucket = "s3_bucket"
            case roleArn = "role_arn"
            case region
            case profile
        }
    }

    struct Conversion: Codable {
        let topN: Int?

        enum CodingKeys: String, CodingKey {
            case topN = "top_n"
        }
    }

    let aws: AWS?
    let conversion: Conversion?
}

enum ConfigError: Error {
    case fileNotFound(String)
    case parseError(String)
}

class ConfigReader {
    let configPath: String

    init(configPath: String? = nil) {
        self.configPath = configPath ?? NSString("~/.config/vco/config.json").expandingTildeInPath
    }

    func read() throws -> VCOConfig {
        let url = URL(fileURLWithPath: configPath)
        guard FileManager.default.fileExists(atPath: configPath) else {
            throw ConfigError.fileNotFound(configPath)
        }
        let data = try Data(contentsOf: url)
        return try JSONDecoder().decode(VCOConfig.self, from: data)
    }

    func validate() -> [String] {
        var missing: [String] = []
        guard let config = try? read() else {
            return ["config.json"]
        }
        if config.aws?.s3Bucket == nil { missing.append("aws.s3_bucket") }
        if config.aws?.roleArn == nil { missing.append("aws.role_arn") }
        return missing
    }
}
