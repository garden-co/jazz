import XCTest

@testable import Jazz

final class EditTests: XCTestCase {
    func testEditsEncodeAsJazzFFIsEditsJSON() throws {
        let edits: [Edit] = [
            .text(column: "notes", text: "b", editedFrom: "a"),
            .splice(column: "body", units: .utf16, from: 4, to: 9, splices: [Splice(at: 1, delete: 2, insert: "xy")]),
            .jsonSet(column: "meta", edits: [(pointer: "/color", value: .string("red"))]),
        ]
        XCTAssertEqual(
            try Coding.json(JSONValue.array(edits.map { $0.json() })),
            #"[{"column":"notes","editedFrom":"a","kind":"text","text":"b"},{"column":"body","kind":"splice","splices":[{"at":1,"delete":2,"insert":"xy"}],"within":{"from":4,"to":9,"units":"utf16"}},{"column":"meta","edits":[{"at":"\/color","value":"red"}],"kind":"jsonSet"}]"#
        )
    }
}
