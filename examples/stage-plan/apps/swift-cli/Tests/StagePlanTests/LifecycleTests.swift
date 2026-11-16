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

    func testDisconnectedWritesSyncAfterReconnect() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Offline")
        try await app.jazz.disconnect()
        let task = try await app.jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Written offline", status: .todo, rank: 1))
        try await app.jazz.reconnect()
        try await app.jazz.waitForSync(.global)
        // A fresh client of the same user reads it from the server.
        var again = try await relay.user()
        again.readTier = .remote
        let synced = try await again.tasks(of: show)
        XCTAssertEqual(synced.map(\.id), [task.id])
        try await again.jazz.close()
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
