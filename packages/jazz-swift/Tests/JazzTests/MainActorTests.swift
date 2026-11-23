import Foundation
import XCTest

@testable import Jazz

/// A view model, as SwiftUI apps write one: transactions are called from the
/// main actor with inline closures. That this compiles in Swift 6 is half
/// the test.
@MainActor
private final class Model {
    struct Failed: Error {}

    let jazz: Client
    var committed = 0

    init(jazz: Client) { self.jazz = jazz }

    func mergeable() async throws {
        try await jazz.transaction { tx in
            MainActor.assertIsolated("the body runs on the caller's actor")
            _ = try await tx.insert(NewNote(text: "mergeable", rank: 1))
        }
        committed += 1
    }

    func serializable() async throws {
        try await jazz.serializableTransaction { tx in
            MainActor.assertIsolated("the body runs on the caller's actor")
            _ = try await tx.insert(NewNote(text: "serializable", rank: 2))
        }
        committed += 1
    }

    func rolledBack() async throws {
        try await jazz.transaction { tx in
            _ = try await tx.insert(NewNote(text: "rolled back", rank: 3))
            throw Failed()
        }
    }
}

final class MainActorTests: XCTestCase {
    func testTransactionsFromTheMainActor() async throws {
        let jazz = try await Client.open(app: notesApp, config: .local())
        let model = await Model(jazz: jazz)
        try await model.mergeable()
        try await model.serializable()
        let committed = await model.committed
        XCTAssertEqual(committed, 2)
        do {
            try await model.rolledBack()
            XCTFail("the body's error is rethrown")
        } catch is Model.Failed {}
        let stored = try await jazz.all(Query<Note>().orderBy(Notes.rank))
        XCTAssertEqual(stored.map(\.text), ["mergeable", "serializable"])
        try await jazz.close()
    }
}
