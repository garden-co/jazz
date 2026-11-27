import Foundation
import Jazz
import StagePlan
import XCTest

/// Cancelling the calling task ends a wait on the server at once, as in TS and
/// Kotlin, with the server unreachable (stopped, not refusing).
final class CancellableWaitTests: XCTestCase {
    typealias Async = _Concurrency.Task

    /// Stops the relay and its server (they don't answer, nor refuse) until `resume`.
    final class Pause {
        let pids: [Int32]
        init(_ relay: TestRelay) {
            pids = [relay.process.processIdentifier, relay.server.process.processIdentifier]
            for pid in pids { kill(pid, SIGSTOP) }
        }
        func resume() { for pid in pids { kill(pid, SIGCONT) } }
        deinit { resume() }
    }

    struct NotEnded: Error {}

    /// Runs `call`, cancels it after `after` ms, and returns how it ended and how
    /// long after the cancel: `NotEnded` if it hadn't 3 s later.
    func cancelled(after: Int = 300, _ call: @escaping @Sendable () async throws -> Void) async
        -> (error: (any Error)?, ms: Int)
    {
        let task = Async { try await call() }
        try? await Async.sleep(for: .milliseconds(after))
        let clock = ContinuousClock()
        let start = clock.now
        task.cancel()
        let ended: (any Error)?? = await withTaskGroup(of: Optional<(any Error)?>.self) { group in
            group.addTask {
                if case .failure(let error) = await task.result { return .some(error) }
                return .some(nil)
            }
            group.addTask {
                try? await Async.sleep(for: .seconds(3))
                return nil
            }
            let first = await group.next()!
            group.cancelAll()
            return first
        }
        let took = (clock.now - start).components
        let ms = Int(took.seconds) * 1000 + Int(took.attoseconds / 1_000_000_000_000_000)
        guard let ended else { return (NotEnded(), ms) }
        return (ended, ms)
    }

    func start(_ call: @escaping @Sendable () async throws -> Void) -> Async<Void, any Error> {
        Async { try await call() }
    }

    func failure(of task: Async<Void, any Error>) async -> (any Error)? {
        do {
            try await task.value
            return nil
        } catch { return error }
    }

    static func newTask(_ show: JazzApp.Show, _ title: String) -> JazzApp.NewTask {
        JazzApp.NewTask(showId: show.id, title: title, status: .todo, rank: 1)
    }

    func testCancellingEndsWaitsOnTheServer() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let jazz = app.jazz
        let show = try await app.show(named: "Cancellable")
        let pause = Pause(relay)
        _ = try await jazz.insert(Self.newTask(show, "Pending"))

        let waited = await cancelled { try await jazz.waitForSync(.global) }
        XCTAssertEqual(waited.error as? JazzError, .cancelled, "\(String(describing: waited.error))")
        XCTAssertLessThan(waited.ms, 100, "waitForSync took \(waited.ms) ms after the cancel")

        let read = await cancelled { _ = try await jazz.all(JazzApp.tasks.query(), tier: .remote) }
        XCTAssertEqual(read.error as? JazzError, .cancelled, "\(String(describing: read.error))")
        XCTAssertLessThan(read.ms, 100, "a remote read took \(read.ms) ms after the cancel")

        // A global write saved locally stays queued, and says so.
        let written = await cancelled { _ = try await jazz.insert(Self.newTask(show, "Queued"), tier: .global) }
        guard case .stillQueued? = written.error as? JazzError else {
            return XCTFail("a global insert cut short: \(String(describing: written.error))")
        }
        XCTAssertLessThan(written.ms, 100, "a global insert took \(written.ms) ms after the cancel")

        // A global commit too: it stands.
        let committed = await cancelled {
            try await jazz.transaction(commit: .global) { tx in
                _ = try await tx.insert(Self.newTask(show, "Committed"))
            }
        }
        guard case .stillQueued? = committed.error as? JazzError else {
            return XCTFail("a global commit cut short: \(String(describing: committed.error))")
        }
        XCTAssertLessThan(committed.ms, 100, "a global commit took \(committed.ms) ms after the cancel")

        // Once the server is back, the queued writes reach it.
        pause.resume()
        try await within(seconds: 15) { try await jazz.waitForSync(.global) }
        let titles = Set(try await jazz.all(JazzApp.tasks.query(), tier: .remote).map(\.title))
        XCTAssertEqual(titles, ["Pending", "Queued", "Committed"])
        try await jazz.close()
    }

    func testACancelledTaskStartsNothingAndRollsBack() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let jazz = app.jazz
        let show = try await app.show(named: "Nothing")
        // Cancelled while it sleeps: the insert then runs in a cancelled task.
        let insert = start {
            try? await Async.sleep(for: .seconds(5))
            _ = try await jazz.insert(Self.newTask(show, "Never"))
        }
        insert.cancel()
        let inserted = await failure(of: insert)
        XCTAssertEqual(inserted as? JazzError, .cancelled, "an insert from a cancelled task: \(String(describing: inserted))")
        let body = start {
            try await jazz.transaction { tx in
                _ = try await tx.insert(Self.newTask(show, "Rolled back"))
                try? await Async.sleep(for: .seconds(5))
            }
        }
        try? await Async.sleep(for: .milliseconds(300))
        body.cancel()
        let rolled = await failure(of: body)
        XCTAssertEqual(rolled as? JazzError, .cancelled, "a transaction cancelled before its commit: \(String(describing: rolled))")
        let stored = try await jazz.all(JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(show.id)))
        XCTAssertEqual(stored.map(\.title), [])
        try await jazz.close()
    }

    func testARefusalAfterTheCancelStillReachesMutationErrors() async throws {
        let relay = try await TestRelay()
        let other = try await relay.user("other")
        let someoneElse = try await other.jazz.account()
        try await other.jazz.close()
        let app = try await relay.user()
        let jazz = app.jazz
        let errors = jazz.mutationErrors
        let pause = Pause(relay)
        // A show chiefed by someone else: the permissions refuse it, later.
        let written = await cancelled {
            _ = try await jazz.insert(
                JazzApp.NewShow(name: "Not mine", venue: "TBA", date: "TBA", doors: "TBA", chiefAccount: someoneElse),
                tier: .global)
        }
        guard case .stillQueued? = written.error as? JazzError else {
            return XCTFail("a global insert cut short: \(String(describing: written.error))")
        }
        pause.resume()
        try await within(seconds: 15) {
            for try await error in errors {
                XCTAssertTrue(error.denied)
                break
            }
        }
        try await jazz.close()
    }
}
