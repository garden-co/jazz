import XCTest
@testable import JazzSwiftBindings

final class JazzSwiftBindingsTests: XCTestCase {
    func testPreparationStatusMatchesLinkedRuntime() {
        #if canImport(jazz_swiftFFI)
        XCTAssertTrue(JazzSwiftBindings.isPrepared)
        XCTAssertFalse(generateId().isEmpty)
        XCTAssertGreaterThan(currentTimestampMs(), 0)
        #else
        XCTAssertFalse(JazzSwiftBindings.isPrepared)
        XCTAssertTrue(
            JazzSwiftBindings.preparationStatus().contains(
                "prepare-jazz-swift-bindings.sh"
            )
        )
        #endif
    }
}
