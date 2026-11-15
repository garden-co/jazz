import Foundation
import Jazz
import StagePlan
import XCTest

final class StagePlanTests: XCTestCase {
    let user = "00000000-0000-4000-8000-0000000000aa"

    func testTheCliSlice() async throws {
        let server = try await TestServer()
        let app = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await app.show(named: "Opening night")
        let me = try await app.jazz.account()
        XCTAssertEqual(show.chiefAccount, me)
        let v1 = try await app.show(named: "Opening night").id
        XCTAssertEqual(v1, show.id)
        let load = try await app.addTask("Load in", to: show)
        let sound = try await app.addTask("Sound check", to: show)
        let v2 = try await app.tasks(of: show).map(\.id)
        XCTAssertEqual(v2, [load.id, sound.id])
        try await app.move(load.id, to: .doing)
        let tasks = try await app.tasks(of: show)
        XCTAssertEqual(tasks.map(\.status), [.doing, .todo])
        XCTAssertLessThan(tasks[0].rank, tasks[1].rank)
        try await app.jazz.close()
    }

    func testTheDocsSnippets() async throws {
        let server = try await TestServer()
        let jazz = try await DocsSnippets.openBackend(server: server.server, userId: user)
        let app = StagePlanApp(jazz: jazz)
        let show = try await app.show(named: "Docs")
        let load = try await app.addTask("Load in", to: show)
        try await app.addTask("Sound check", to: show)
        let v3 = try await DocsSnippets.oneShot(jazz, show: show).count
        XCTAssertEqual(v3, 2)
        let v4 = try await DocsSnippets.filters(jazz, show: show).count
        XCTAssertEqual(v4, 2)
        let v5 = try await DocsSnippets.sorting(jazz, show: show).map(\.title)
        XCTAssertEqual(v5, ["Load in", "Sound check"])
        let v6 = try await DocsSnippets.pagination(jazz, show: show).count
        XCTAssertEqual(v6, 0)
        let updated = try await DocsSnippets.crud(jazz, show: show)
        XCTAssertEqual(updated?.title, "Focus the lights")
        XCTAssertEqual(updated?.status, .doing)
        let v7 = try await DocsSnippets.oneShot(jazz, show: show).count
        XCTAssertEqual(v7, 2)
        try await DocsSnippets.nullableUpdate(jazz, task: load)
        let results = try await DocsSnippets.whereOperators(jazz)
        XCTAssertEqual(results.map(\.count), [0, 2, 2, 1, 2, 0])
        let seen = try await DocsSnippets.subscriptions(jazz, show: show)
        XCTAssertEqual(seen.first?.map(\.title), ["Load in", "Sound check"])
        XCTAssertEqual(seen.last?.map(\.title), ["Load in", "Sound check", "Strike"])
        try await jazz.close()
    }

    func testCloseEndsLiveWatches() async throws {
        let server = try await TestServer()
        let app = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await app.show(named: "Closing")
        let watching = _Concurrency.Task { () -> Swift.Error? in
            do {
                for try await _ in app.jazz.watch(JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(show.id))) {}
                return nil
            } catch {
                return error
            }
        }
        try await _Concurrency.Task.sleep(nanoseconds: 200_000_000)
        try await app.jazz.close()
        let ended = try await withThrowingTaskGroup(of: Swift.Error??.self) { group in
            group.addTask { await watching.value }
            group.addTask {
                try await _Concurrency.Task.sleep(nanoseconds: 3_000_000_000)
                return nil  // timed out
            }
            let first = try await group.next()!
            group.cancelAll()
            return first
        }
        guard case .some(let error) = ended else { return XCTFail("the watch didn't end within 3 s of close()") }
        XCTAssertEqual(error as? JazzError, .closed, "\(String(describing: error))")
    }

    func testIncludesAndSelect() async throws {
        let server = try await TestServer()
        let app = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await app.show(named: "Includes")
        for title in ["Load in", "Sound check", "Doors", "Strike"] {
            try await app.addTask(title, to: show)
        }
        let (withTasks, withShow) = try await DocsSnippets.includes(app.jazz, show: show)
        XCTAssertEqual(withTasks?.tasks?.map(\.title), ["Load in", "Sound check", "Doors"])
        XCTAssertEqual(withShow.count, 4)
        XCTAssertTrue(withShow.allSatisfy { $0.show?.id == show.id })
        // Without include, relations are nil.
        let plain = try await app.tasks(of: show)
        XCTAssertNil(plain.first?.show)
        let plainShow = try await app.jazz.one(JazzApp.shows.query().where(JazzApp.shows.id.eq(show.id)))
        XCTAssertNil(plainShow?.tasks)
        // The same row compares equal with and without its include.
        XCTAssertEqual(plain.first, withShow.first(where: { $0.id == plain.first?.id }))
        let titles = try await DocsSnippets.select(app.jazz, show: show)
        XCTAssertEqual(titles.map(\.title), ["Load in", "Sound check", "Doors", "Strike"])
        try await app.jazz.close()
    }

    struct Oops: Error {}

    func testTransactionsCommitAndRollBack() async throws {
        let server = try await TestServer()
        let app = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await app.show(named: "Transactions")
        let task = try await DocsSnippets.transaction(app.jazz, show: show)
        let committed = try await app.tasks(of: show)
        XCTAssertEqual(committed.map(\.id), [task.id])
        XCTAssertEqual(committed.first?.rank, 1)
        do {
            _ = try await app.jazz.transaction { tx in
                try await tx.insert(JazzApp.NewTask(showId: show.id, title: "Never", status: .todo, rank: 0))
                throw Oops()
            }
            XCTFail("the transaction should rethrow")
        } catch is Oops {}
        let afterRollback = try await app.tasks(of: show)
        XCTAssertEqual(afterRollback.map(\.title), ["Rig lights"])
        try await app.jazz.close()
    }

    func testDurabilityTiers() async throws {
        let server = try await TestServer()
        let app = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await app.show(named: "Tiers")
        try await app.jazz.waitForSync(.global)
        let task = try await DocsSnippets.writeTier(app.jazz, show: show)
        let remote = try await DocsSnippets.readTier(app.jazz, show: show)
        XCTAssertEqual(remote.map(\.id), [task.id])
        XCTAssertEqual(remote.first?.status, .doing)
        try await app.jazz.close()
    }

    func testAWriteThePermissionsRefuseFailsAtTheGlobalTier() async throws {
        let server = try await TestServer()
        let chief = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await chief.show(named: "Private")
        try await chief.jazz.waitForSync(.global)
        let stranger = try await StagePlanApp.open(
            user: "00000000-0000-4000-8000-0000000000bb", server: server.server)
        do {
            _ = try await stranger.jazz.insert(
                JazzApp.NewTask(showId: show.id, title: "Sneaky", status: .todo, rank: 1), tier: .global)
            XCTFail("a stranger's task in someone else's show must be refused")
        } catch {
            guard case .permissionDenied = error else { return XCTFail("\(error)") }
        }
        try await stranger.jazz.close()
        try await chief.jazz.close()
    }

    func testTextEditsMergeWithOthers() async throws {
        let server = try await TestServer()
        let alice = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await alice.show(named: "Text")
        let task = try await alice.jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Notes", status: .todo, notes: "Hello world", rank: 1),
            tier: .global)
        // A second store of the same user edits the same notes concurrently.
        let bob = try await StagePlanApp.open(user: user, server: server.server)
        let seen = try await bob.jazz.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)), tier: .remote)
        XCTAssertEqual(seen?.notes, "Hello world")

        let aliceNotes = try await alice.jazz.text(task, JazzApp.tasks.notes)
        try await aliceNotes.insert("brave ", at: 6)
        try await bob.jazz.editText(JazzApp.Task.self, id: task.id, column: "notes", from: "Hello world", to: "Hello world!")
        try await alice.jazz.waitForSync(.global)
        try await bob.jazz.waitForSync(.global)

        let merged = try await alice.jazz.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)), tier: .remote)
        XCTAssertEqual(merged?.notes, "Hello brave world!")
        try await bob.jazz.close()
        try await alice.jazz.close()
    }

    func testTheTextSnippet() async throws {
        let server = try await TestServer()
        let app = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await app.show(named: "Snippet")
        let task = try await app.jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Notes", status: .todo, notes: "", rank: 1))
        let text = try await DocsSnippets.mergeableText(app.jazz, task: task)
        XCTAssertEqual(text, "Pack gaffer tape. ")
        let stored = try await app.jazz.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)))
        XCTAssertEqual(stored?.notes, "Pack gaffer tape. ")
        try await app.jazz.close()
    }

    func testPermissionAdvice() async throws {
        let server = try await TestServer()
        let chief = try await StagePlanApp.open(user: user, server: server.server)
        let show = try await chief.show(named: "Advice")
        let task = try await chief.addTask("Load in", to: show)
        try await chief.jazz.waitForSync(.global)
        let stranger = try await StagePlanApp.open(
            user: "00000000-0000-4000-8000-0000000000dd", server: server.server)
        let newTask = JazzApp.NewTask(showId: show.id, title: "Sneaky", status: .todo, rank: 2)
        let chiefMay = try await chief.jazz.canInsert(newTask)
        let strangerMay = try await stranger.jazz.canInsert(newTask)
        XCTAssertEqual(chiefMay, true)
        XCTAssertEqual(strangerMay, false)
        let chiefDeletes = try await chief.jazz.canDelete(JazzApp.Task.self, id: task.id)
        let strangerUpdates = try await stranger.jazz.canUpdate(task.id, JazzApp.TaskPatch(status: .set(.done)))
        XCTAssertEqual(chiefDeletes, true)
        XCTAssertEqual(strangerUpdates, false)
        try await stranger.jazz.close()
        try await chief.jazz.close()
    }
}
