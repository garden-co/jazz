// swift-tools-version:6.0
// The Jazz Swift package, at the repository root so SwiftPM can take it by
// URL at a release tag:
//
//     .package(url: "https://github.com/garden-co/jazz", exact: "2.0.0-alpha.65")
//
// Its sources live in packages/jazz-swift. The UniFFI sources over
// crates/jazz-ffi are committed (packages/jazz-swift/scripts/build-ffi.sh
// --check keeps them current); libjazz_ffi itself is a system library found
// through pkg-config (`jazz_ffi.pc`), which build-ffi.sh writes beside the
// static library it builds. See packages/jazz-swift/README.md.
import PackageDescription

let package = Package(
    name: "jazz",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "Jazz", targets: ["Jazz"])
    ],
    targets: [
        .target(name: "Jazz", dependencies: ["JazzFFI"], path: "packages/jazz-swift/Sources/Jazz"),
        .target(
            name: "JazzFFI", dependencies: ["jazz_ffiFFI"], path: "packages/jazz-swift/Sources/JazzFFI"),
        .systemLibrary(
            name: "jazz_ffiFFI", path: "packages/jazz-swift/Sources/jazz_ffiFFI", pkgConfig: "jazz_ffi"),
        .testTarget(
            name: "JazzTests", dependencies: ["Jazz"], path: "packages/jazz-swift/Tests/JazzTests"),
    ]
)
