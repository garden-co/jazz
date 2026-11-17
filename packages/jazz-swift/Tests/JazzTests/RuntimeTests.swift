import Foundation
import XCTest

@testable import Jazz

/// A note: the table these tests write, in an app of its own.
struct Note: Row {
    static let table = "notes"
    let id: Jazz.ID
    var text: String
    var rank: Double
}

struct NewNote: Insert {
    typealias Row = Note
    var text: String
    var rank: Double
}

enum Notes {
    static let text = Column<Note, String>("text")
    static let rank = Column<Note, Double>("rank")
}

let notesApp = #"""
    { "format": 1, "tables": { "notes": { "columns": { "text": "string", "rank": "float" } } } }
    """#

/// The Jazz core through the package, on whatever platform runs the tests:
/// Linux in the repository's gate, macOS and the iOS simulator against the
/// release's XCFramework in the mirror's check.
final class RuntimeTests: XCTestCase {
    func local() async throws -> Client {
        try await Client.open(app: notesApp, config: .local())
    }

    func testWritesAndReadsBack() async throws {
        let jazz = try await local()
        let b = try await jazz.insert(NewNote(text: "b", rank: 2))
        let a = try await jazz.insert(NewNote(text: "a", rank: 1))
        let all = try await jazz.all(Query<Note>().orderBy(Notes.rank))
        XCTAssertEqual(all.map(\.id), [a.id, b.id])
        let one = try await jazz.one(Query<Note>().where(Notes.text.eq("b")))
        XCTAssertEqual(one?.id, b.id)
        try await jazz.delete(a)
        let left = try await jazz.all(Query<Note>())
        XCTAssertEqual(left.map(\.id), [b.id])
        try await jazz.close()
    }

    func testWatchFollowsWrites() async throws {
        let jazz = try await local()
        var results = jazz.watch(Query<Note>().orderBy(Notes.rank)).makeAsyncIterator()
        let first = try await results.next()
        XCTAssertEqual(first?.count, 0)
        let note = try await jazz.insert(NewNote(text: "seen", rank: 1))
        let second = try await results.next()
        XCTAssertEqual(second?.map(\.id), [note.id])
        try await jazz.close()
    }

    func testTransactionsCommitTogether() async throws {
        let jazz = try await local()
        try await jazz.transaction { tx in
            try await tx.insert(NewNote(text: "one", rank: 1))
            try await tx.insert(NewNote(text: "two", rank: 2))
        }
        let all = try await jazz.all(Query<Note>().orderBy(Notes.rank))
        XCTAssertEqual(all.map(\.text), ["one", "two"])
        try await jazz.close()
    }

    func testAPersistentStoreSurvivesReopening() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("jazz-runtime-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: dir) }
        let config = Config.local(appId: UUID().uuidString.lowercased(), storage: .persistent, dataDir: dir.path)
        let first = try await Client.open(app: notesApp, config: config)
        let note = try await first.insert(NewNote(text: "kept", rank: 1))
        try await first.close()
        let again = try await Client.open(app: notesApp, config: config)
        let all = try await again.all(Query<Note>())
        XCTAssertEqual(all.map(\.id), [note.id])
        try await again.close()
    }
}
