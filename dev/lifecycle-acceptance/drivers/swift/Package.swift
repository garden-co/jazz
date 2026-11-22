// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "lifecycle-driver",
    dependencies: [.package(url: "__JAZZ_SWIFT_URL__", exact: "__VERSION__")],
    targets: [
        .executableTarget(
            name: "driver",
            dependencies: [.product(name: "Jazz", package: "jazz-swift")],
            swiftSettings: [.swiftLanguageMode(.v5)])
    ]
)
