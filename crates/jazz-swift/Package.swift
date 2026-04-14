// swift-tools-version: 6.0

import Foundation
import PackageDescription

let packageDirectory = URL(fileURLWithPath: #filePath).deletingLastPathComponent().path
let xcframeworkPath = "artifacts/JazzSwiftFFI.xcframework"
let hasXCFramework = FileManager.default.fileExists(
    atPath: packageDirectory + "/artifacts/JazzSwiftFFI.xcframework"
)

var targets: [Target] = []

if hasXCFramework {
    targets.append(
        .binaryTarget(
            name: "jazz_swiftFFI",
            path: xcframeworkPath
        )
    )
}

targets.append(
    .target(
        name: "JazzSwiftBindings",
        dependencies: hasXCFramework
            ? [
                .target(
                    name: "jazz_swiftFFI",
                    condition: .when(platforms: [.iOS, .macOS])
                ),
            ]
            : []
    )
)

targets.append(
    .testTarget(
        name: "JazzSwiftBindingsTests",
        dependencies: ["JazzSwiftBindings"]
    )
)

let package = Package(
    name: "JazzSwiftBindings",
    platforms: [
        .iOS(.v15),
        .macOS(.v13),
    ],
    products: [
        .library(
            name: "JazzSwiftBindings",
            targets: ["JazzSwiftBindings"]
        ),
    ],
    targets: targets
)
