// JazzQuery is for SwiftUI on Apple platforms: these run on macOS and the iOS
// simulator (the mirror's check), not on Linux.
#if canImport(Observation) && !os(Linux)
    import Foundation
    import Observation
    import XCTest

    @testable import Jazz

    @available(macOS 14, iOS 17, *)
    @MainActor
    final class JazzQueryTests: XCTestCase {
        struct TimedOut: Error {}

        /// Waits until `done` holds, as a view would see it.
        func until(_ what: String, _ done: @escaping () -> Bool) async throws {
            for _ in 0..<200 {
                if done() { return }
                try await _Concurrency.Task.sleep(nanoseconds: 25_000_000)
            }
            XCTFail("timed out waiting for \(what)")
            throw TimedOut()
        }

        func testRowsFollowWritesInOrder() async throws {
            let jazz = try await Client.open(app: notesApp, config: .local())
            let query = JazzQuery(jazz, Query<Note>().orderBy(Notes.rank))
            XCTAssertTrue(query.isLoading)
            try await until("the first result") { !query.isLoading }
            XCTAssertEqual(query.rows.count, 0)
            XCTAssertNil(query.error)
            let b = try await jazz.insert(NewNote(text: "b", rank: 2))
            let a = try await jazz.insert(NewNote(text: "a", rank: 1))
            try await until("both rows") { query.rows.count == 2 }
            XCTAssertEqual(query.rows.map(\.id), [a.id, b.id])
            try await jazz.close()
        }

        func testAChangeNotifiesObservers() async throws {
            let jazz = try await Client.open(app: notesApp, config: .local())
            let query = JazzQuery(jazz, Query<Note>())
            try await until("the first result") { !query.isLoading }
            final class Flag: @unchecked Sendable {
                private let lock = NSLock()
                private var value = false
                var set: Bool {
                    get { lock.withLock { value } }
                    set { lock.withLock { value = newValue } }
                }
            }
            let changed = Flag()
            // What SwiftUI does for a view reading `rows`.
            withObservationTracking {
                _ = query.rows
            } onChange: {
                changed.set = true
            }
            _ = try await jazz.insert(NewNote(text: "new", rank: 1))
            try await until("the observer is told") { changed.set }
            try await jazz.close()
        }

        func testCancelStopsFollowing() async throws {
            let jazz = try await Client.open(app: notesApp, config: .local())
            let query = JazzQuery(jazz, Query<Note>())
            try await until("the first result") { !query.isLoading }
            query.cancel()
            _ = try await jazz.insert(NewNote(text: "unseen", rank: 1))
            try await _Concurrency.Task.sleep(nanoseconds: 300_000_000)
            XCTAssertEqual(query.rows.count, 0)
            try await jazz.close()
        }

        func testClosingTheClientEndsTheQueryWithClosed() async throws {
            let jazz = try await Client.open(app: notesApp, config: .local())
            let query = JazzQuery(jazz, Query<Note>())
            try await until("the first result") { !query.isLoading }
            try await jazz.close()
            try await until("the error") { query.error != nil }
            XCTAssertEqual(query.error as? JazzError, .closed)
        }
    }
#endif
