// swift-tools-version:5.9
// The Jazz Swift runtime. The typed bindings that `jazz-tools generate` writes
// (`JazzApp.swift`) call it, and it wraps crates/jazz-ffi through UniFFI.
//
// Run `scripts/build-ffi.sh` first. It builds libjazz_ffi (a static library,
// in target/release/jazz-ffi-static of the Jazz repository, or in $JAZZ_FFI_LIB_DIR) and
// generates the UniFFI sources in Sources/JazzFFI and Sources/jazz_ffiFFI.
import Foundation
import PackageDescription

let here = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
let libDir =
    ProcessInfo.processInfo.environment["JAZZ_FFI_LIB_DIR"]
    ?? here.appendingPathComponent("../../target/release/jazz-ffi-static").standardized.path

let package = Package(
    name: "jazz-swift",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "Jazz", targets: ["Jazz"])
    ],
    targets: [
        .target(name: "Jazz", dependencies: ["JazzFFI"]),
        .target(
            name: "JazzFFI",
            dependencies: ["jazz_ffiFFI"],
            swiftSettings: [.unsafeFlags(["-suppress-warnings"])],
            linkerSettings: [
                .linkedLibrary("jazz_ffi"),
                .linkedLibrary("m"),
                .linkedLibrary("dl", .when(platforms: [.linux])),
                .linkedLibrary("pthread", .when(platforms: [.linux])),
                .unsafeFlags(["-L", libDir]),
            ]
        ),
        .target(name: "jazz_ffiFFI"),
        .testTarget(name: "JazzTests", dependencies: ["Jazz"]),
    ]
)
