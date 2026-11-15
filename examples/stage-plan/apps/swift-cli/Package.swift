// swift-tools-version:5.9
// StagePlan from Swift: a CLI over StagePlan's jazz.app.json and
// jazz.permissions.json (byte-identical copies of apps/react-localfirst's),
// with the bindings `jazz-tools generate` wrote in JazzApp.swift.
import PackageDescription

let package = Package(
    name: "stage-plan-swift",
    platforms: [.macOS(.v13)],
    dependencies: [
        .package(name: "jazz-swift", path: "../../../../packages/jazz-swift")
    ],
    targets: [
        .target(
            name: "StagePlan",
            dependencies: [.product(name: "Jazz", package: "jazz-swift")],
            path: ".",
            sources: ["JazzApp.swift", "Sources/StagePlan"],
            resources: [.copy("jazz.permissions.json")]
        ),
        .executableTarget(name: "stage-plan-swift", dependencies: ["StagePlan"]),
        .testTarget(name: "StagePlanTests", dependencies: ["StagePlan"]),
    ]
)
