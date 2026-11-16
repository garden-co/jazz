import Foundation
import Jazz
import StagePlan
import XCTest

/// TS's lifecycle: mergeable and serializable transactions, disconnect and
/// reconnect, refused writes nobody waited on, and deleting the local store.
final class LifecycleTests: XCTestCase {
    func testSerializableTransactionsCommit() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Serializable")
        let task = try await app.jazz.serializableTransaction(commit: .global) { tx in
            try await tx.insert(JazzApp.NewTask(showId: show.id, title: "Load in", status: .todo, rank: 1))
        }
        let committed = try await app.tasks(of: show)
        XCTAssertEqual(committed.map(\.id), [task.id])
        try await app.jazz.close()
    }

    /// Both bodies have read before either commits.
    actor Barrier {
        var waiting: [CheckedContinuation<Void, Never>] = []
        let count: Int
        init(count: Int) { self.count = count }
        func arrive() async {
            if waiting.count + 1 == count {
                waiting.forEach { $0.resume() }
                waiting = []
                return
            }
            await withCheckedContinuation { waiting.append($0) }
        }
    }

    /// Two clients of the same user each move the same task in a
    /// transaction, both reading it before either commits.
    func concurrentMoves(serializable: Bool) async throws -> [Result<Void, any Error>] {
        let relay = try await TestRelay()
        let first = try await relay.user()
        let show = try await first.show(named: "Concurrent")
        let task = try await first.addTask("Rig lights", to: show)
        var second = try await relay.user()
        second.readTier = .remote
        _ = try await second.tasks(of: show)
        let barrier = Barrier(count: 2)
        func move(_ jazz: Jazz.Client, to rank: Double) async -> Result<Void, any Error> {
            do {
                let body: (Jazz.Transaction) async throws -> Void = { tx in
                    let read = try await tx.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)))
                    XCTAssertNotNil(read)
                    try await tx.update(task.id, JazzApp.TaskPatch(rank: .set(rank)))
                    await barrier.arrive()
                }
                if serializable {
                    try await jazz.serializableTransaction(commit: .global, body)
                } else {
                    try await jazz.transaction(commit: .global, body)
                }
                return .success(())
            } catch {
                return .failure(error)
            }
        }
        async let a = move(first.jazz, to: 5)
        async let b = move(second.jazz, to: 7)
        let results = await [a, b]
        try await first.jazz.close()
        try await second.jazz.close()
        return results
    }

    func testConcurrentSerializableTransactionsConflict() async throws {
        let results = try await concurrentMoves(serializable: true)
        let conflicts = results.filter {
            if case .failure(let error) = $0, case .conflict? = error as? JazzError { return true }
            return false
        }
        XCTAssertEqual(results.filter { if case .success = $0 { return true } else { return false } }.count, 1, "\(results)")
        XCTAssertEqual(conflicts.count, 1, "\(results)")
    }

    func testDisconnectedWritesSyncAfterReconnect() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Offline")
        // Another client of the same user, reading from the server.
        var other = try await relay.user()
        other.readTier = .remote
        try await app.jazz.disconnect()
        let task = try await app.jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Written offline", status: .todo, rank: 1))
        // Time it would take to sync, were the app connected.
        try await Task.sleep(nanoseconds: 1_000_000_000)
        let whileDisconnected = try await other.tasks(of: show)
        XCTAssertEqual(whileDisconnected.map(\.id), [], "the write reached the server while disconnected")
        try await app.jazz.reconnect()
        try await app.jazz.waitForSync(.global)
        let afterReconnect = try await other.tasks(of: show)
        XCTAssertEqual(afterReconnect.map(\.id), [task.id])
        try await other.jazz.close()
        try await app.jazz.close()
    }

    func testRefusedWritesReachMutationErrors() async throws {
        let relay = try await TestRelay()
        let other = try await relay.user("other")
        let someoneElse = try await other.jazz.account()
        try await other.jazz.close()
        let app = try await relay.user()
        let errors = app.jazz.mutationErrors
        // A show chiefed by someone else: the permissions refuse it, and
        // nothing waited on it, so the watch reports it.
        _ = try await app.jazz.insert(
            JazzApp.NewShow(name: "Not mine", venue: "TBA", date: "TBA", doors: "TBA", chiefAccount: someoneElse))
        let error = try await first(of: errors, within: 10)
        XCTAssertTrue(error.denied)
        XCTAssertFalse(error.serializable)
        XCTAssertFalse(error.transaction.isEmpty)
        try await app.jazz.close()
    }

    func testDeleteClientStorageEmptiesTheStore() async throws {
        let relay = try await TestRelay()
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("stage-plan-delete-\(UUID().uuidString)").path
        defer { try? FileManager.default.removeItem(atPath: dir) }
        let key = try Jazz.generateDeviceKey()
        let app = try await StagePlanApp.open(
            endpoint: relay.endpoint, appId: relay.appId, auth: .deviceKey(key), dataDir: dir)
        let me = try await app.jazz.account()
        // Kept local: the server never gets it, so only the store has it.
        try await app.jazz.disconnect()
        _ = try await app.jazz.insert(
            JazzApp.NewShow(name: "Gone", venue: "TBA", date: "TBA", doors: "TBA", chiefAccount: me))
        let named = JazzApp.shows.query().where(JazzApp.shows.name.eq("Gone"))
        let before = try await app.jazz.all(named, tier: .local)
        XCTAssertEqual(before.count, 1)
        try await app.jazz.deleteClientStorage()
        do {
            _ = try await app.jazz.all(named)
            XCTFail("the app should be closed")
        } catch let error as JazzError {
            XCTAssertEqual(error, .closed)
        }
        let reopened = try await StagePlanApp.open(
            endpoint: relay.endpoint, appId: relay.appId, auth: .deviceKey(key), dataDir: dir)
        let after = try await reopened.jazz.all(named, tier: .local)
        XCTAssertEqual(after.count, 0)
        try await reopened.jazz.close()
    }

    func testDeleteClientStorageIsRefusedWhileAnotherAppHoldsTheStore() async throws {
        let relay = try await TestRelay()
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("stage-plan-held-\(UUID().uuidString)").path
        defer { try? FileManager.default.removeItem(atPath: dir) }
        let key = try Jazz.generateDeviceKey()
        let app = try await StagePlanApp.open(
            endpoint: relay.endpoint, appId: relay.appId, auth: .deviceKey(key), dataDir: dir)
        let holder = try await StagePlanApp.open(
            endpoint: relay.endpoint, appId: relay.appId, auth: .deviceKey(key), dataDir: dir)
        do {
            try await app.jazz.deleteClientStorage()
            XCTFail("deleting a store another app holds should be refused")
        } catch let error as JazzError {
            guard case .refused = error else { return XCTFail("\(error)") }
        }
        // The app stays open.
        let show = try await app.show(named: "Still open")
        XCTAssertEqual(show.name, "Still open")
        try await holder.jazz.close()
        try await app.jazz.close()
    }

    func testMutationErrorsFailClosedWhenTheAppCloses() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let errors = app.jazz.mutationErrors
        try await app.jazz.close()
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                do {
                    for try await _ in errors {}
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

    struct TimedOut: Error {}

    func first<T: Sendable>(of stream: AsyncThrowingStream<T, any Error>, within seconds: UInt64)
        async throws -> T
    {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask {
                for try await value in stream { return value }
                throw TimedOut()
            }
            group.addTask {
                try await Task.sleep(nanoseconds: seconds * 1_000_000_000)
                throw TimedOut()
            }
            defer { group.cancelAll() }
            return try await group.next()!
        }
    }
}
