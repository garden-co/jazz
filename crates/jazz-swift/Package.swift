// swift-tools-version: 6.0

import PackageDescription

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
    targets: [
        .target(
            name: "JazzSwiftBindings"
        ),
        .testTarget(
            name: "JazzSwiftBindingsTests",
            dependencies: ["JazzSwiftBindings"]
        ),
    ]
)
