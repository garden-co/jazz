import Foundation
import XCTest

@testable import Jazz

/// UTF-16 ranges, as UIKit and AppKit give them, map onto Unicode scalars:
/// one may split a character, only a surrogate pair is refused.
final class TextIndexTests: XCTestCase {
    /// `base` with UTF-16 `range` replaced by `text`, as JazzText does it.
    func replacing(_ base: String, _ location: Int, _ length: Int, with text: String) throws -> String {
        var scalars = base.unicodeScalars
        scalars.replaceSubrange(
            try JazzText<Task>.scalarRange(NSRange(location: location, length: length), in: base),
            with: text.unicodeScalars)
        return String(scalars)
    }

    func testRangesInsideACharacterAreTaken() throws {
        // A backspace removing a combining accent, a vowel sign, half a flag,
        // a zero-width joiner.
        XCTAssertEqual(try replacing("e\u{301}x", 1, 1, with: ""), "ex")
        XCTAssertEqual(try replacing("कि", 1, 1, with: ""), "क")
        XCTAssertEqual(try replacing("🇩🇪", 2, 2, with: ""), "🇩")
        XCTAssertEqual(try replacing("👩\u{200D}👧", 2, 1, with: ""), "👩👧")
    }

    func testSurrogatePairsAndBoundsAreRefused() {
        for (base, location, length) in [("a😀b", 2, 0), ("a😀b", 1, 1), ("a😀b", 2, 2), ("ab", 1, 5), ("ab", -1, 1)] {
            XCTAssertThrowsError(try replacing(base, location, length, with: "X"), "\(base) \(location) \(length)") {
                guard case .refused = $0 as? JazzError else { return XCTFail("\($0)") }
            }
        }
    }

    func testWholeCharactersAndTheEnd() throws {
        XCTAssertEqual(try replacing("a😀b", 1, 2, with: "X"), "aXb")
        XCTAssertEqual(try replacing("a😀b", 3, 0, with: "X"), "a😀Xb")
        XCTAssertEqual(try replacing("ab", 2, 0, with: "!"), "ab!")
    }
}
