public enum JazzSwiftBindings {
    public static let isPrepared = _jazzSwiftBindingsHasFFI

    public static func preparationStatus() -> String {
        if isPrepared {
            return "JazzSwiftFFI xcframework is linked for iOS builds."
        }

        return "JazzSwiftFFI xcframework is not linked in this build. Run bash scripts/prepare-jazz-swift-bindings.sh from the jazz2 repo before building this package for iOS."
    }
}

#if os(iOS) && !canImport(jazz_swiftFFI)
#error("JazzSwiftFFI.xcframework is missing. Run bash scripts/prepare-jazz-swift-bindings.sh in the jazz2 repo before building this package for iOS.")
#endif

#if canImport(jazz_swiftFFI)
private let _jazzSwiftBindingsHasFFI = true
#else
private let _jazzSwiftBindingsHasFFI = false
#endif
