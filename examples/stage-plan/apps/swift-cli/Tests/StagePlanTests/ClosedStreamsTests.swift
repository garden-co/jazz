import Foundation
import Jazz
import StagePlan
import XCTest

/// When the app closes, its session and sync streams end with
/// `JazzError.closed`, their watches' last state.
final class ClosedStreamsTests: XCTestCase {
    struct TimedOut: Error {}

    func testSessionAndSyncStreamsFailClosedWhenTheAppCloses() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let sessions = app.jazz.session
        let syncs = app.jazz.syncChanges
        try await app.jazz.close()
        try await endsClosed(sessions)
        try await endsClosed(syncs)
    }

    func endsClosed<T: Sendable>(_ stream: AsyncThrowingStream<T, any Error>) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                do {
                    for try await _ in stream {}
                    XCTFail("the stream should fail with .closed")
                } catch let error as JazzError {
                    XCTAssertEqual(error, .closed)
                }
            }
            group.addTask {
                try await Task.sleep(nanoseconds: 10_000_000_000)
                throw TimedOut()
            }
            defer { group.cancelAll() }
            try await group.next()!
        }
    }
}
