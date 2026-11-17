// The Swift snippets the docs show, by region (see docs/lib/languages/SNIPPETS.md):
// one function per region over the docs todo app. Tests/TodoDocsTests runs them.
import Foundation
import Jazz

/// The example ids the inserts use for `owner_id` and `projectId`.
public let exampleOwnerId = "00000000-0000-4000-8000-00000000a11c"
public let exampleProjectId = "00000000-0000-4000-8000-000000000001"

// #region reading-oneshot-swift
public func readOpenTodos(_ jazz: Jazz.Client) async throws -> [JazzApp.Todo] {
    try await jazz.all(JazzApp.todos.query().where(JazzApp.todos.done.eq(false)))
}
// #endregion reading-oneshot-swift

// #region reading-one-swift
public func readTodo(_ jazz: Jazz.Client, id: Jazz.ID) async throws -> JazzApp.Todo? {
    // The first matching row, or nil if no row matches or you can't read it.
    try await jazz.one(JazzApp.todos.query().where(JazzApp.todos.id.eq(id)))
}
// #endregion reading-one-swift

// #region reading-subscriptions-swift
public func watchOpenTodos(
    _ jazz: Jazz.Client, onChange: @escaping @Sendable ([JazzApp.Todo]) -> Void
) -> Task<Void, any Error> {
    // `watch` yields the whole result each time it changes (`subscribe`
    // yields deltas instead). Cancel the task to unsubscribe.
    Task {
        for try await todos in jazz.watch(JazzApp.todos.query().where(JazzApp.todos.done.eq(false))) {
            onChange(todos)
        }
    }
}
// #endregion reading-subscriptions-swift

// #region reading-filters-swift
public func readOpenDocsTodos(_ jazz: Jazz.Client) async throws -> [JazzApp.Todo] {
    let todos = JazzApp.todos.self
    // Conditions passed to `where` are ANDed.
    return try await jazz.all(todos.query().where(todos.done.eq(false), todos.title.contains("docs")))
}
// #endregion reading-filters-swift

// #region reading-sorting-swift
public func readOpenTodosByTitle(_ jazz: Jazz.Client) async throws -> [JazzApp.Todo] {
    let todos = JazzApp.todos.self
    return try await jazz.all(todos.query().where(todos.done.eq(false)).orderBy(todos.title.asc))
}
// #endregion reading-sorting-swift

// #region reading-pagination-swift
public func readTodoPage(_ jazz: Jazz.Client, page: Int, pageSize: Int = 20) async throws
    -> [JazzApp.Todo]
{
    let todos = JazzApp.todos.self
    return try await jazz.all(
        todos.query()
            .where(todos.done.eq(false))
            .orderBy(todos.title.asc, todos.id.asc)
            .limit(pageSize)
            .offset(max(0, (page - 1) * pageSize)))
}
// #endregion reading-pagination-swift

// #region reading-durability-tier-swift
public func readOpenTodosAtRemoteTier(_ jazz: Jazz.Client) async throws -> [JazzApp.Todo] {
    // `.local` (this store), `.localFirst` (the default) or `.remote` (the server).
    try await jazz.all(JazzApp.todos.query().where(JazzApp.todos.done.eq(false)), tier: .remote)
}
// #endregion reading-durability-tier-swift

// #region reading-includes-swift
public func readOpenTodosWithProjects(_ jazz: Jazz.Client) async throws -> [JazzApp.Todo] {
    let todos = JazzApp.todos.self
    // Included rows land in the relation properties (`todo.project`,
    // `todo.parent?.project`), which are nil when not included.
    return try await jazz.all(
        todos.query()
            .where(todos.done.eq(false))
            .include(todos.project, todos.parent.include(todos.project)))
}
// #endregion reading-includes-swift

// #region reading-reverse-relation-swift
public func readProjectsWithOpenTodos(_ jazz: Jazz.Client) async throws -> [JazzApp.Project] {
    try await jazz.all(
        JazzApp.projects.query().include(JazzApp.projects.todos.where(JazzApp.todos.done.eq(false))))
}
// #endregion reading-reverse-relation-swift

// #region reading-select-swift
/// The columns `select` reads (and `id`), with the included project's.
public struct TodoTitle: Decodable, Sendable, Equatable {
    public struct ProjectName: Decodable, Sendable, Equatable {
        public var name: String
    }
    public var id: Jazz.ID
    public var title: String
    public var project: ProjectName?
}

public func readOpenTodoTitles(_ jazz: Jazz.Client) async throws -> [TodoTitle] {
    let todos = JazzApp.todos.self
    return try await jazz.all(
        todos.query().where(todos.done.eq(false)).include(todos.project).select(todos.title),
        as: TodoTitle.self)
}
// #endregion reading-select-swift

// #region reading-recursive-swift
public func todoLineageQuery() -> Gathered<JazzApp.Todo> {
    let todos = JazzApp.todos.self
    // Each open todo, and its parent, its parent's parent and so on.
    return todos.query().where(todos.done.eq(false)).gather(todos.parent, maxDepth: 10)
}
// #endregion reading-recursive-swift

public struct WhereResults: Sendable {
    public var incomplete, nonDraft, selected, recent, highPriority, lowPriority: [JazzApp.Todo]
    public var matches, unlinked, linked, doneWithProject, recentIncomplete: [JazzApp.Todo]
    public var subscription: AsyncThrowingStream<[JazzApp.Todo], any Error>
}

public func whereOperatorExamples(
    _ jazz: Jazz.Client, todoIdA: Jazz.ID, todoIdB: Jazz.ID, searchTerm: String
) async throws -> WhereResults {
    let todos = JazzApp.todos.self

    // #region where-eq-ne-swift
    let incomplete = try await jazz.all(todos.query().where(todos.done.eq(false)))
    let nonDraft = try await jazz.all(todos.query().where(todos.title.ne("Draft")))
    let selected = try await jazz.all(todos.query().where(todos.id.in([todoIdA, todoIdB])))
    // #endregion where-eq-ne-swift

    // #region where-numeric-swift
    // Magic columns such as `$createdAt` (a `Date`) have no binding: name them.
    let createdAt = Jazz.Column<JazzApp.Todo, Date>("$createdAt")
    let oneWeekAgo = Date().addingTimeInterval(-7 * 24 * 60 * 60)
    let recent = try await jazz.all(todos.query().where(createdAt.gt(oneWeekAgo)))
    let highPriority = try await jazz.all(todos.query().where(todos.priority.gte(3)))
    let lowPriority = try await jazz.all(todos.query().where(todos.priority.lt(10)))
    // #endregion where-numeric-swift

    // #region where-contains-swift
    // Substring match (case-sensitive).
    let matches = try await jazz.all(todos.query().where(todos.title.contains(searchTerm)))
    // #endregion where-contains-swift

    // #region where-null-swift
    // Rows where the optional reference is not set, and where it is.
    let unlinked = try await jazz.all(todos.query().where(todos.parentId.isNull()))
    let linked = try await jazz.all(todos.query().where(todos.parentId.isNotNull()))
    // #endregion where-null-swift

    // #region where-and-swift
    // Conditions passed to `where` are ANDed.
    let doneWithProject = try await jazz.all(
        todos.query().where(todos.done.eq(true), todos.projectId.isNotNull()))
    // #endregion where-and-swift

    // #region where-order-limit-swift
    let recentIncomplete = try await jazz.all(
        todos.query().where(todos.done.eq(false))
            .orderBy(Jazz.Column<JazzApp.Todo, Date>("$createdAt").asc).limit(50))
    // #endregion where-order-limit-swift

    // #region where-subscription-swift
    // Iterate it (`for try await todos in subscription`) to get each result.
    let subscription = jazz.watch(todos.query().where(todos.done.eq(false)))
    // #endregion where-subscription-swift

    return WhereResults(
        incomplete: incomplete, nonDraft: nonDraft, selected: selected, recent: recent,
        highPriority: highPriority, lowPriority: lowPriority, matches: matches,
        unlinked: unlinked, linked: linked, doneWithProject: doneWithProject,
        recentIncomplete: recentIncomplete, subscription: subscription)
}

// #region writing-crud-swift
public func writeTodoCrud(_ jazz: Jazz.Client, todoId: Jazz.ID) async throws {
    try await jazz.insert(
        JazzApp.NewTodo(
            title: "Write docs", done: false, ownerId: exampleOwnerId, projectId: exampleProjectId))
    try await jazz.update(todoId, JazzApp.TodoPatch(done: .set(true)))
    try await jazz.delete(JazzApp.Todo.self, id: todoId)
}
// #endregion writing-crud-swift

// #region writing-nullable-update-swift
public func clearTodoOwner(_ jazz: Jazz.Client, todoId: Jazz.ID) async throws {
    // `ownerId` is column `owner_id`. `.set(nil)` stores null; fields left
    // out (`.unchanged`) keep their value, so `description` stays as it is.
    try await jazz.update(todoId, JazzApp.TodoPatch(ownerId: .set(nil)))
}
// #endregion writing-nullable-update-swift

// #region writing-upsert-swift
public func upsertTodo(_ jazz: Jazz.Client, importedTodoId: Jazz.ID) async throws {
    try await jazz.upsert(importedTodoId, JazzApp.NewTodo(title: "Imported task", done: false))
    try await jazz.waitForSync(.global)
}
// #endregion writing-upsert-swift

// #region writing-restore-swift
public func restoreDeletedTodo(_ jazz: Jazz.Client, todoId: Jazz.ID) async throws -> JazzApp.Todo {
    let todos = JazzApp.todos.self
    try await jazz.delete(JazzApp.Todo.self, id: todoId)

    guard try await jazz.one(todos.query().where(todos.id.eq(todoId)).includeDeleted()) != nil
    else { throw TodoNotFound(id: todoId) }

    return try await jazz.restore(
        todoId,
        as: JazzApp.NewTodo(
            title: "Restored task", done: false, ownerId: exampleOwnerId,
            projectId: exampleProjectId))
}
// #endregion writing-restore-swift

// #region writing-durability-tier-swift
public func writeTodoWithDurabilityTiers(_ jazz: Jazz.Client) async throws {
    // Each call returns once the server accepted the write (and its
    // permissions did).
    let todo = try await jazz.insert(
        JazzApp.NewTodo(
            title: "Write docs with durability tier", done: false, ownerId: exampleOwnerId,
            projectId: exampleProjectId),
        tier: .global)
    try await jazz.update(todo.id, JazzApp.TodoPatch(done: .set(true)), tier: .global)
    try await jazz.delete(todo, tier: .global)
}
// #endregion writing-durability-tier-swift

// #region writing-mutation-errors-swift
public func insertTodoAndWait(_ jazz: Jazz.Client) async throws {
    do {
        let todo = try await jazz.insert(
            JazzApp.NewTodo(
                title: "Ship review fixes", done: false, ownerId: exampleOwnerId,
                projectId: exampleProjectId),
            tier: .global)
        print(todo.id)
    } catch JazzError.permissionDenied(let reason) {
        // The server rejected the write.
        print("permissionDenied", reason)
    }
}
// #endregion writing-mutation-errors-swift

// #region writing-transaction-swift
public func groupTodoWrites(_ jazz: Jazz.Client, existingTodoId: Jazz.ID) async throws -> Jazz.ID {
    // Commits when the closure returns, waiting for `.global`; rolls back
    // when it throws.
    try await jazz.transaction(commit: .global) { tx in
        let created = try await tx.insert(
            JazzApp.NewTodo(
                title: "Write transaction docs", done: false, ownerId: exampleOwnerId,
                projectId: exampleProjectId))
        try await tx.update(existingTodoId, JazzApp.TodoPatch(done: .set(true)))

        // Reads inside see the transaction's own writes.
        guard let staged = try await tx.one(JazzApp.todos.query().where(JazzApp.todos.id.eq(created.id)))
        else { throw TodoNotFound(id: created.id) }
        return staged.id
    }
}
// #endregion writing-transaction-swift

// #region writing-transaction-errors-swift
public func completeTodoInTransaction(_ jazz: Jazz.Client, todoId: Jazz.ID) async throws {
    do {
        try await jazz.transaction(commit: .global) { tx in
            try await tx.update(todoId, JazzApp.TodoPatch(done: .set(true)))
        }
    } catch JazzError.permissionDenied(let reason) {
        // The server rejected the transaction.
        print("permissionDenied", reason)
    }
}
// #endregion writing-transaction-errors-swift

// #region writing-serializable-transaction-swift
public func finishTodoSerializably(_ jazz: Jazz.Client, todoId: Jazz.ID) async throws -> Jazz.ID {
    // The server accepts it only if nothing it read changed meanwhile: a
    // `JazzError.conflict` means another write won, and retrying may succeed.
    try await jazz.serializableTransaction { tx in
        guard let todo = try await tx.one(JazzApp.todos.query().where(JazzApp.todos.id.eq(todoId)))
        else { throw TodoNotFound(id: todoId) }
        try await tx.update(todo.id, JazzApp.TodoPatch(done: .set(true)))
        return todo.id
    }
}
// #endregion writing-serializable-transaction-swift

// #region dry-run-permissions-swift
// Each answer is `true`, `false` or `nil` (unknown: no server to ask, say).
public func canReadTodo(_ jazz: Jazz.Client, todoId: Jazz.ID) async throws -> Bool? {
    try await jazz.canRead(JazzApp.Todo.self, id: todoId)
}

public struct TodoSummary: Decodable, Sendable, Equatable {
    public var id: Jazz.ID
    public var title: String
}

public func readTodosWithDeletePermission(_ jazz: Jazz.Client) async throws -> [TodoSummary] {
    let todos = JazzApp.todos.self
    var deletable: [TodoSummary] = []
    let all = try await jazz.all(
        todos.query().orderBy(todos.title.asc).select(todos.title), as: TodoSummary.self)
    for todo in all {
        if try await jazz.canDelete(JazzApp.Todo.self, id: todo.id) == true { deletable.append(todo) }
    }
    return deletable
}

public func readEditableTodos(_ jazz: Jazz.Client) async throws -> [TodoSummary] {
    let todos = JazzApp.todos.self
    var editable: [TodoSummary] = []
    let all = try await jazz.all(
        todos.query().orderBy(todos.title.asc).select(todos.title), as: TodoSummary.self)
    for todo in all {
        let patch = JazzApp.TodoPatch(title: .set(todo.title))
        if try await jazz.canUpdate(todo.id, patch) == true { editable.append(todo) }
    }
    return editable
}

public func canCreateTodo(_ jazz: Jazz.Client, title: String) async throws -> Bool? {
    try await jazz.canInsert(JazzApp.NewTodo(title: title, done: false))
}
// #endregion dry-run-permissions-swift

/// What the snippets throw when a todo they need isn't there.
public struct TodoNotFound: Error, Equatable {
    public var id: Jazz.ID
}
