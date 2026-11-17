import Foundation
import Jazz
import TodoDocs
import XCTest

/// Runs every docs snippet against an end user of the docs todo app, signed
/// in through a test relay (the `global` tier needs a server).
final class DocsSnippetsTests: XCTestCase {
    let todos = JazzApp.todos.self

    @discardableResult
    func add(
        _ jazz: Jazz.Client, _ title: String, done: Bool = false, priority: Int? = nil,
        parentId: Jazz.ID? = nil, projectId: Jazz.ID? = nil
    ) async throws -> JazzApp.Todo {
        try await jazz.insert(
            JazzApp.NewTodo(
                title: title, done: done, priority: priority, parentId: parentId, projectId: projectId))
    }

    func testReading() async throws {
        let relay = try await TestRelay()
        let jazz = try await relay.user()
        let project = try await jazz.insert(JazzApp.NewProject(name: "Docs site"))
        let parent = try await add(jazz, "Plan docs", projectId: project.id)
        let child = try await add(jazz, "Write docs", parentId: parent.id)
        try await add(jazz, "Buy milk", done: true, projectId: project.id)

        let open = try await readOpenTodos(jazz)
        XCTAssertEqual(Set(open.map(\.title)), ["Plan docs", "Write docs"])
        let one = try await readTodo(jazz, id: child.id)
        XCTAssertEqual(one?.title, "Write docs")
        let none = try await readTodo(jazz, id: UUID().uuidString.lowercased())
        XCTAssertNil(none)
        let filtered = try await readOpenDocsTodos(jazz)
        XCTAssertEqual(filtered.map(\.title).sorted(), ["Plan docs", "Write docs"])
        let sorted = try await readOpenTodosByTitle(jazz)
        XCTAssertEqual(sorted.map(\.title), ["Plan docs", "Write docs"])
        let page1 = try await readTodoPage(jazz, page: 1, pageSize: 1)
        XCTAssertEqual(page1.map(\.title), ["Plan docs"])
        let page2 = try await readTodoPage(jazz, page: 2, pageSize: 1)
        XCTAssertEqual(page2.map(\.title), ["Write docs"])
        try await jazz.waitForSync(.global)
        let remote = try await readOpenTodosAtRemoteTier(jazz)
        XCTAssertEqual(remote.count, 2)

        let included = try await readOpenTodosWithProjects(jazz)
        let byTitle = Dictionary(uniqueKeysWithValues: included.map { ($0.title, $0) })
        XCTAssertEqual(byTitle["Plan docs"]?.project?.name, "Docs site")
        XCTAssertEqual(byTitle["Write docs"]?.parent?.project?.name, "Docs site")

        let projects = try await readProjectsWithOpenTodos(jazz)
        XCTAssertEqual(projects.map(\.name), ["Docs site"])
        XCTAssertEqual(projects.first?.todos?.map(\.title), ["Plan docs"])

        let titles = try await readOpenTodoTitles(jazz)
        let titleOf = Dictionary(uniqueKeysWithValues: titles.map { ($0.title, $0) })
        XCTAssertEqual(titleOf["Plan docs"]?.project?.name, "Docs site")
        XCTAssertNil(titleOf["Write docs"]?.project)

        let lineage = try await jazz.all(todoLineageQuery())
        XCTAssertEqual(Set(lineage.map(\.title)), ["Plan docs", "Write docs"])
        try await jazz.close()
    }

    func testSubscriptions() async throws {
        let relay = try await TestRelay()
        let jazz = try await relay.user()
        try await add(jazz, "First")
        let seen = AsyncStream<[String]>.makeStream()
        let subscription = watchOpenTodos(jazz) { todos in
            seen.continuation.yield(todos.map(\.title).sorted())
        }
        var results = seen.stream.makeAsyncIterator()
        let first = await results.next()
        XCTAssertEqual(first, ["First"])
        try await add(jazz, "Second")
        var latest = first
        while latest != ["First", "Second"] { latest = await results.next() }
        subscription.cancel()
        try await jazz.close()
    }

    func testWhereOperators() async throws {
        let relay = try await TestRelay()
        let jazz = try await relay.user()
        let project = try await jazz.insert(JazzApp.NewProject(name: "Home"))
        let milk = try await add(jazz, "Buy milk", priority: 5)
        // `where-order-limit` orders by `$createdAt` (milliseconds): rows
        // inserted back to back can share one, so make Draft's later.
        try await _Concurrency.Task.sleep(nanoseconds: 20_000_000)
        let draft = try await add(jazz, "Draft", priority: 12, parentId: milk.id)
        try await add(jazz, "Done", done: true, priority: 1, projectId: project.id)

        let r = try await whereOperatorExamples(
            jazz, todoIdA: milk.id, todoIdB: draft.id, searchTerm: "milk")
        XCTAssertEqual(Set(r.incomplete.map(\.title)), ["Buy milk", "Draft"])
        XCTAssertEqual(Set(r.nonDraft.map(\.title)), ["Buy milk", "Done"])
        XCTAssertEqual(Set(r.selected.map(\.title)), ["Buy milk", "Draft"])
        XCTAssertEqual(r.recent.count, 3)
        XCTAssertEqual(Set(r.highPriority.map(\.title)), ["Buy milk", "Draft"])
        XCTAssertEqual(Set(r.lowPriority.map(\.title)), ["Buy milk", "Done"])
        XCTAssertEqual(r.matches.map(\.title), ["Buy milk"])
        XCTAssertEqual(Set(r.unlinked.map(\.title)), ["Buy milk", "Done"])
        XCTAssertEqual(r.linked.map(\.title), ["Draft"])
        XCTAssertEqual(r.doneWithProject.map(\.title), ["Done"])
        XCTAssertEqual(r.recentIncomplete.map(\.title), ["Buy milk", "Draft"])
        var updates = r.subscription.makeAsyncIterator()
        let current = try await updates.next()
        XCTAssertEqual(Set(current?.map(\.title) ?? []), ["Buy milk", "Draft"])
        try await jazz.close()
    }

    func testWriting() async throws {
        let relay = try await TestRelay()
        let jazz = try await relay.user()

        let doomed = try await add(jazz, "Old")
        try await writeTodoCrud(jazz, todoId: doomed.id)
        let afterCrud = try await jazz.all(todos.query())
        XCTAssertEqual(afterCrud.map(\.title), ["Write docs"])
        XCTAssertEqual(afterCrud.first?.ownerId, exampleOwnerId)
        XCTAssertEqual(afterCrud.first?.projectId, exampleProjectId)

        let owned = try await jazz.insert(
            JazzApp.NewTodo(
                title: "Owned", done: false, description: "keep me", ownerId: exampleOwnerId))
        try await clearTodoOwner(jazz, todoId: owned.id)
        let cleared = try await readTodo(jazz, id: owned.id)
        XCTAssertNil(cleared?.ownerId)
        XCTAssertEqual(cleared?.description, "keep me")

        let importedId = UUID().uuidString.lowercased()
        try await upsertTodo(jazz, importedTodoId: importedId)
        try await upsertTodo(jazz, importedTodoId: importedId)
        let imported = try await readTodo(jazz, id: importedId)
        XCTAssertEqual(imported?.title, "Imported task")

        let restored = try await restoreDeletedTodo(jazz, todoId: owned.id)
        XCTAssertEqual(restored.id, owned.id)
        XCTAssertEqual(restored.title, "Restored task")

        let before = try await jazz.all(todos.query()).count
        try await writeTodoWithDurabilityTiers(jazz)
        let after = try await jazz.all(todos.query()).count
        XCTAssertEqual(after, before)

        try await insertTodoAndWait(jazz)
        let shipped = try await jazz.all(todos.query().where(todos.title.eq("Ship review fixes")))
        XCTAssertEqual(shipped.count, 1)
        try await jazz.close()
    }

    func testTransactions() async throws {
        let relay = try await TestRelay()
        let jazz = try await relay.user()
        let existing = try await add(jazz, "Existing")

        let createdId = try await groupTodoWrites(jazz, existingTodoId: existing.id)
        let created = try await readTodo(jazz, id: createdId)
        XCTAssertEqual(created?.title, "Write transaction docs")
        let done = try await readTodo(jazz, id: existing.id)
        XCTAssertEqual(done?.done, true)

        let other = try await add(jazz, "Other")
        let finished = try await finishTodoSerializably(jazz, todoId: other.id)
        XCTAssertEqual(finished, other.id)
        let otherDone = try await readTodo(jazz, id: other.id)
        XCTAssertEqual(otherDone?.done, true)
        do {
            _ = try await finishTodoSerializably(jazz, todoId: UUID().uuidString.lowercased())
            XCTFail("a missing todo should throw")
        } catch is TodoNotFound {}

        let third = try await add(jazz, "Third")
        try await completeTodoInTransaction(jazz, todoId: third.id)
        let thirdDone = try await readTodo(jazz, id: third.id)
        XCTAssertEqual(thirdDone?.done, true)
        try await jazz.close()
    }

    /// With todos read-only, the error snippets catch the server's rejection.
    func testRejections() async throws {
        let permissions = """
            {"$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
             "tables": {"projects": {"read": "true", "insert": "true"}, "todos": {"read": "true"}}}
            """
        let relay = try await TestRelay(permissions: permissions)
        let jazz = try await relay.user()
        try await insertTodoAndWait(jazz)
        let canCreate = try await canCreateTodo(jazz, title: "Nope")
        XCTAssertEqual(canCreate, false)
        try await jazz.close()
    }

    func testDryRunPermissions() async throws {
        let relay = try await TestRelay()
        let jazz = try await relay.user()
        let todo = try await add(jazz, "Readable")
        try await jazz.waitForSync(.global)
        let canRead = try await canReadTodo(jazz, todoId: todo.id)
        XCTAssertEqual(canRead, true)
        let deletable = try await readTodosWithDeletePermission(jazz)
        XCTAssertEqual(deletable.map(\.title), ["Readable"])
        let editable = try await readEditableTodos(jazz)
        XCTAssertEqual(editable.map(\.title), ["Readable"])
        let canCreate = try await canCreateTodo(jazz, title: "New")
        XCTAssertEqual(canCreate, true)
        try await jazz.close()
    }
}
