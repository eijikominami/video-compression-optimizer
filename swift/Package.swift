// swift-tools-version: 5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "vco-photos",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .executable(
            name: "vco-photos",
            targets: ["vco-photos"]
        ),
        .library(
            name: "VCOPhotosLib",
            targets: ["VCOPhotosLib"]
        )
    ],
    targets: [
        // Library target containing all the logic (testable)
        .target(
            name: "VCOPhotosLib",
            dependencies: [],
            path: "Sources/VCOPhotosLib"
        ),
        // Executable target (thin wrapper)
        .executableTarget(
            name: "vco-photos",
            dependencies: ["VCOPhotosLib"],
            path: "Sources/vco-photos"
        ),
        // Test target
        .testTarget(
            name: "VCOPhotosTests",
            dependencies: ["VCOPhotosLib"],
            path: "Tests/VCOPhotosTests"
        )
    ]
)
