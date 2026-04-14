import XCTest
@testable import JazzSwiftBindings

final class JazzSwiftBindingsTests: XCTestCase {
    private final class StringBox: @unchecked Sendable {
        var value: String?
    }

    private func makeWireRow(id: String, values: [JazzJSONValue]) -> JazzWireRow {
        JazzWireRow(
            id: id,
            values: values.map { JazzWireValue(type: "Text", value: $0) }
        )
    }

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

    func testJazzValueEncodesToTaggedWireShape() throws {
        let payload: [String: JazzValue] = [
            "title": .text("milk"),
            "done": .boolean(true),
        ]

        let encoded = try JazzJSONCodec.encode(payload)
        let decoded = try JSONSerialization.jsonObject(with: Data(encoded.utf8)) as? [String: [String: Any]]

        XCTAssertEqual(decoded?["title"]?["type"] as? String, "Text")
        XCTAssertEqual(decoded?["title"]?["value"] as? String, "milk")
        XCTAssertEqual(decoded?["done"]?["type"] as? String, "Boolean")
        XCTAssertEqual(decoded?["done"]?["value"] as? Bool, true)
    }

    func testWireRowDecodesConvenienceShape() throws {
        let row = try JazzJSONCodec.decode(
            JazzWireRow.self,
            from: #"{"id":"row-1","values":[{"type":"Text","value":"hello"},{"type":"Boolean","value":true}]}"#
        )

        XCTAssertEqual(row.id, "row-1")
        XCTAssertEqual(row.values[0].renderedDescription, "hello")
        XCTAssertEqual(row.values[1].renderedDescription, "true")
    }

    func testSubscriptionChangesDecode() throws {
        let changes = try JazzJSONCodec.decode(
            [JazzSubscriptionChange].self,
            from: #"[{"kind":0,"id":"row-1","index":0,"row":{"id":"row-1","values":[{"type":"Text","value":"hello"}]}}]"#
        )

        XCTAssertEqual(changes.count, 1)
        XCTAssertEqual(changes[0].kindDescription, "added")
        XCTAssertEqual(changes[0].row?.values.first?.renderedDescription, "hello")
    }

    func testClosureSubscriptionCallbackForwardsJSON() {
        let received = StringBox()
        let callback = ClosureSubscriptionCallback { received.value = $0 }
        callback.onUpdate(deltaJson: #"[{"kind":0}]"#)
        XCTAssertEqual(received.value, #"[{"kind":0}]"#)
    }

    func testWireValueTypedAccessors() throws {
        let row = try JazzJSONCodec.decode(
            JazzWireRow.self,
            from: #"{"id":"row-1","values":[{"type":"Text","value":"hello"},{"type":"Boolean","value":true},{"type":"Timestamp","value":1700}]}"#
        )

        XCTAssertEqual(row.values[0].stringValue, "hello")
        XCTAssertEqual(row.values[1].boolValue, true)
        XCTAssertEqual(row.values[2].uint64Value, 1700)
    }

    func testWireRowNamedColumnsMapValuesByColumnName() throws {
        let row = try JazzJSONCodec.decode(
            JazzWireRow.self,
            from: #"{"id":"row-1","values":[{"type":"Text","value":"hello"},{"type":"Boolean","value":true}]}"#
        )

        let named = try row.named(columns: ["title", "done"])

        XCTAssertEqual(named.id, "row-1")
        XCTAssertEqual(named.values["title"]?.stringValue, "hello")
        XCTAssertEqual(named.values["done"]?.boolValue, true)
    }

    func testWireRowNamedColumnsRejectMismatchedColumnCount() throws {
        let row = try JazzJSONCodec.decode(
            JazzWireRow.self,
            from: #"{"id":"row-1","values":[{"type":"Text","value":"hello"}]}"#
        )

        XCTAssertThrowsError(
            try row.named(columns: ["title", "done"])
        ) { error in
            XCTAssertEqual(
                error as? JazzWireRowColumnError,
                .columnCountMismatch(expected: 2, actual: 1)
            )
        }
    }

    func testObserveHelpersSuppressInitialSnapshotReplay() {
        let initialRow = makeWireRow(id: "row-1", values: [.string("hello")])
        let snapshotChanges = [
            JazzSubscriptionChange(kind: 0, id: "row-1", index: 0, row: initialRow),
        ]

        let currentRows = currentRowsFromSnapshotChanges(snapshotChanges)

        XCTAssertEqual(currentRows, [initialRow])
        XCTAssertEqual(diffSubscriptionChanges(from: [initialRow], to: currentRows), [])
    }

    func testObserveHelpersEmitOnlyInterveningChangesAgainstInitialRows() {
        let initialRow = makeWireRow(id: "row-1", values: [.string("before")])
        let updatedRow = makeWireRow(id: "row-1", values: [.string("after")])
        let addedRow = makeWireRow(id: "row-2", values: [.string("extra")])
        let snapshotChanges = [
            JazzSubscriptionChange(kind: 0, id: "row-1", index: 0, row: updatedRow),
            JazzSubscriptionChange(kind: 0, id: "row-2", index: 1, row: addedRow),
        ]

        let normalized = diffSubscriptionChanges(
            from: [initialRow],
            to: currentRowsFromSnapshotChanges(snapshotChanges)
        )

        XCTAssertEqual(
            normalized,
            [
                JazzSubscriptionChange(kind: 2, id: "row-1", index: 0, row: updatedRow),
                JazzSubscriptionChange(kind: 0, id: "row-2", index: 1, row: addedRow),
            ]
        )
    }

    func testObserveHelpersEmitRemovalsBeforeFollowUpAddsAndUpdates() {
        let removedRow = makeWireRow(id: "row-1", values: [.string("removed")])
        let stableRow = makeWireRow(id: "row-2", values: [.string("stable")])
        let movedRow = makeWireRow(id: "row-2", values: [.string("stable")])
        let addedRow = makeWireRow(id: "row-3", values: [.string("added")])

        let normalized = diffSubscriptionChanges(
            from: [removedRow, stableRow],
            to: [movedRow, addedRow]
        )

        XCTAssertEqual(
            normalized,
            [
                JazzSubscriptionChange(kind: 1, id: "row-1", index: 0, row: nil),
                JazzSubscriptionChange(kind: 2, id: "row-2", index: 0, row: movedRow),
                JazzSubscriptionChange(kind: 0, id: "row-3", index: 1, row: addedRow),
            ]
        )
    }
}
