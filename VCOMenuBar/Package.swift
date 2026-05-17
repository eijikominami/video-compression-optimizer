// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "VCOMenuBar",
    platforms: [.macOS(.v13)],
    products: [
        .executable(name: "VCOMenuBar", targets: ["VCOMenuBar"]),
    ],
    targets: [
        .executableTarget(
            name: "VCOMenuBar",
            path: "Sources/VCOMenuBar"
        ),
        .testTarget(
            name: "VCOMenuBarTests",
            dependencies: ["VCOMenuBar"],
            path: "Tests/VCOMenuBarTests"
        ),
    ]
)
