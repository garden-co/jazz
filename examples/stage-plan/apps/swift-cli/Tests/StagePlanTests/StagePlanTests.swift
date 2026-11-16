import Foundation
import Jazz
import StagePlan
import XCTest

final class StagePlanTests: XCTestCase {
    let user = "00000000-0000-4000-8000-0000000000aa"

    func testTheCliSlice() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
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

    func testCloseEndsLiveWatches() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
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
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Includes")
        for title in ["Load in", "Sound check", "Doors", "Strike"] {
            try await app.addTask(title, to: show)
        }
        let shows = JazzApp.shows.self
        let tasks = JazzApp.tasks.self
        // A reverse relation, narrowed, and a forward one.
        let withTasks = try await app.jazz.one(
            shows.query().where(shows.id.eq(show.id))
                .include(shows.tasks.orderBy(tasks.rank).limit(3)))
        let withShow = try await app.jazz.all(
            tasks.query().where(tasks.showId.eq(show.id)).include(tasks.show))
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
        let titles = try await app.jazz.all(
            tasks.query().where(tasks.showId.eq(show.id)).orderBy(tasks.rank).select(tasks.title),
            as: TaskTitle.self)
        XCTAssertEqual(titles.map(\.title), ["Load in", "Sound check", "Doors", "Strike"])
        try await app.jazz.close()
    }

    struct TaskTitle: Decodable, Sendable, Equatable {
        var id: String
        var title: String
    }

    struct Oops: Error {}

    func testTransactionsCommitAndRollBack() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Transactions")
        // Reads inside see the transaction's own writes.
        let task = try await app.jazz.transaction(commit: .global) { tx in
            let task = try await tx.insert(
                JazzApp.NewTask(showId: show.id, title: "Rig lights", status: .todo, rank: 10))
            let open = try await tx.all(JazzApp.tasks.query().where(JazzApp.tasks.status.eq(.todo)))
            try await tx.update(task.id, JazzApp.TaskPatch(rank: .set(Double(open.count))))
            return task
        }
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
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Tiers")
        try await app.jazz.waitForSync(.global)
        let task = try await app.jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Doors", status: .todo, rank: 20), tier: .global)
        try await app.jazz.update(task.id, JazzApp.TaskPatch(status: .set(.doing)))
        try await app.jazz.waitForSync(.global)
        let remote = try await app.jazz.all(
            JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(show.id)), tier: .remote)
        XCTAssertEqual(remote.map(\.id), [task.id])
        XCTAssertEqual(remote.first?.status, .doing)
        try await app.jazz.close()
    }

    func testAWriteThePermissionsRefuseFailsAtTheGlobalTier() async throws {
        let relay = try await TestRelay()
        let chief = try await relay.user()
        let show = try await chief.show(named: "Private")
        try await chief.jazz.waitForSync(.global)
        let stranger = try await relay.user("00000000-0000-4000-8000-0000000000bb")
        do {
            _ = try await stranger.jazz.insert(
                JazzApp.NewTask(showId: show.id, title: "Sneaky", status: .todo, rank: 1), tier: .global)
            XCTFail("a stranger's task in someone else's show must be refused")
        } catch let error as JazzError {
            guard case .permissionDenied = error else { return XCTFail("\(error)") }
        }
        try await stranger.jazz.close()
        try await chief.jazz.close()
    }

    func testTextEditsMergeWithOthers() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Text")
        let task = try await app.jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Notes", status: .todo, notes: "hello world", rank: 1))
        // Someone else's change lands first...
        try await app.jazz.update(task.id, JazzApp.TaskPatch(notes: .set("hello brave world")))
        // ...then an edit made from the text before it: a plain write would
        // give "hello world!", editText keeps both.
        try await app.jazz.editText(JazzApp.Task.self, id: task.id, column: "notes", from: "hello world", to: "hello world!")
        let merged = try await app.jazz.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)))
        XCTAssertEqual(merged?.notes, "hello brave world!")

        // The same inside a transaction.
        try await app.jazz.update(task.id, JazzApp.TaskPatch(notes: .set("one two")))
        try await app.jazz.update(task.id, JazzApp.TaskPatch(notes: .set("one and two")))
        try await app.jazz.transaction { tx in
            try await tx.editText(JazzApp.Task.self, id: task.id, column: "notes", from: "one two", to: "one two three")
        }
        let inTx = try await app.jazz.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)))
        XCTAssertEqual(inTx?.notes, "one and two three")

        // Client.edit without a patch.
        try await app.jazz.edit(
            JazzApp.Task.self, id: task.id,
            edits: [.text(column: "notes", text: "one and two three!", editedFrom: "one and two three")])
        let edited = try await app.jazz.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)))
        XCTAssertEqual(edited?.notes, "one and two three!")
        try await app.jazz.close()
    }

    func testJazzTextEditsAndChanges() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Handle")
        let task = try await app.jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Notes", status: .todo, notes: "a😀b", rank: 1))
        let notes = try await app.jazz.text(task, JazzApp.tasks.notes)
        // Inside the emoji's surrogate pair: refused, nothing written.
        do {
            try await notes.insert("X", at: 2)
            XCTFail("an offset inside a surrogate pair must be refused")
        } catch let error as JazzError {
            guard case .refused = error else { return XCTFail("\(error)") }
        }
        try await notes.insert("X", at: 3)  // after the emoji
        try await notes.delete(0..<1)
        XCTAssertEqual(notes.current, "😀Xb")

        var seen: [String] = []
        for try await text in notes.changes {
            seen.append(text)
            if text == "😀Xb!" { break }
            if seen.count == 1 {
                // Someone else appends; `changes` delivers the merged text.
                try await app.jazz.editText(JazzApp.Task.self, id: task.id, column: "notes", from: "😀Xb", to: "😀Xb!")
            }
        }
        XCTAssertEqual(seen.first, "😀Xb")
        XCTAssertEqual(notes.current, "😀Xb!")
        let stored = try await app.jazz.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)))
        XCTAssertEqual(stored?.notes, "😀Xb!")
        try await app.jazz.close()
    }

    func testTheTextSnippet() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Snippet")
        let task = try await app.jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Notes", status: .todo, notes: "", rank: 1))
        let text = try await DocsSnippets.mergeableText(app.jazz, task: task)
        XCTAssertEqual(text, "Pack gaffer tape. ")
        let stored = try await app.jazz.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)))
        XCTAssertEqual(stored?.notes, "Pack gaffer tape. ")
        try await app.jazz.close()
    }

    /// An edit merged with someone else's change comes back as the merged
    /// text, never equal to what the handle wrote: it must become current.
    func testAMergedEchoBecomesCurrent() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Echo")
        let task = try await app.jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Notes", status: .todo, notes: "Hello world", rank: 1))
        let notes = try await app.jazz.text(task, JazzApp.tasks.notes)
        // Someone else's change is in the store before the handle edits.
        try await app.jazz.editText(JazzApp.Task.self, id: task.id, column: "notes", from: "Hello world", to: "Hello world!")
        try await notes.insert("brave ", at: 6)
        for try await text in notes.changes where text == "Hello brave world!" { break }
        XCTAssertEqual(notes.current, "Hello brave world!")
        // An append at the delivered end works.
        try await notes.insert("?", at: notes.current.utf16.count)
        let stored = try await app.jazz.one(JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)))
        XCTAssertEqual(stored?.notes, "Hello brave world!?")
        try await app.jazz.close()
    }

    func testPermissionAdvice() async throws {
        let relay = try await TestRelay()
        let chief = try await relay.user()
        let show = try await chief.show(named: "Advice")
        let task = try await chief.addTask("Load in", to: show)
        try await chief.jazz.waitForSync(.global)
        let stranger = try await relay.user("00000000-0000-4000-8000-0000000000dd")
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

    func testUpsertRestoreBundleAndSyncStatus() async throws {
        let relay = try await TestRelay()
        let app = try await relay.user()
        let show = try await app.show(named: "Upsert")
        let id = UUID().uuidString.lowercased()
        let first = try await app.jazz.upsert(id, JazzApp.NewTask(showId: show.id, title: "One", status: .todo, rank: 1))
        XCTAssertEqual(first.id, id)
        let second = try await app.jazz.upsert(id, JazzApp.NewTask(showId: show.id, title: "Two", status: .doing, rank: 1))
        XCTAssertEqual(second.title, "Two")
        try await app.jazz.delete(second)
        let gone = try await app.tasks(of: show)
        XCTAssertEqual(gone.count, 0)
        let back = try await app.jazz.restore(JazzApp.Task.self, id: id)
        XCTAssertEqual(back.title, "Two")

        try await app.jazz.waitForSync(.global)
        let status = try await app.jazz.syncStatus()
        XCTAssertEqual(status.link, .connected)
        let session = try await app.jazz.sessionState()
        guard case .signedIn(let user)? = session else { return XCTFail("\(String(describing: session))") }
        XCTAssertEqual(user.kind, .device)
        XCTAssertTrue(try Jazz.generateDeviceKey().hasPrefix("jazz-auth-v1:"))
        try await app.jazz.close()
    }

    /// The adopter gate's privacy check: B targets A's show by id, reads no
    /// tasks and can't write into it.
    func testAnotherUserCantReadOrWriteAShowById() async throws {
        let relay = try await TestRelay()
        let alice = try await relay.user("alice")
        let show = try await alice.show(named: "Private")
        try await alice.addTask("Secret", toShow: show.id)
        let bob = try await relay.user("bob")
        let seen = try await bob.tasks(ofShow: show.id)
        XCTAssertEqual(seen.count, 0)
        do {
            try await bob.addTask("Sneaky", toShow: show.id)
            XCTFail("bob must not write into alice's show")
        } catch let error as JazzError {
            guard case .permissionDenied = error else { return XCTFail("\(error)") }
        }
        let aliceSees = try await alice.tasks(ofShow: show.id)
        XCTAssertEqual(aliceSees.map(\.title), ["Secret"])
        try await bob.jazz.close()
        try await alice.jazz.close()
    }
}
