import XCTest
@testable import JazzSwiftBindings

final class JazzSwiftBindingsTests: XCTestCase {
    func testScaffoldCompiles() {
        XCTAssertTrue(JazzSwiftBindings.scaffoldReady)
        XCTAssertEqual(
            JazzSwiftBindings.bindingRootDescription(),
            "Thin Swift package scaffold over the Jazz Rust core."
        )
    }
}
