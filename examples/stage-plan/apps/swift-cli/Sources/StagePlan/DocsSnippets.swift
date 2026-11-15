// The Swift snippets the docs show, by region; Tests/StagePlanTests runs them.
import Foundation
@_spi(Backend) import Jazz

public enum DocsSnippets {
    public static func openBackend(server: Server, userId: String) async throws -> Jazz.Client {
        // #region context-setup-swift-backend
        let permissions = try StagePlanApp.permissions() // jazz.permissions.json's text
        let jazz = try await JazzApp.open(
            .backend(
                appId: server.appId,
                permissions: permissions,
                serverUrl: server.url,
                backendSecret: server.backendSecret, // vouches for the user below
                adminSecret: server.adminSecret, // publishes the permissions bundle
                user: userId, // permissions see it as `session.user`
                storage: .memory
            ))
        // #endregion context-setup-swift-backend
        return jazz
    }

    public static func openClient(endpoint: String, appId: String, deviceKey: String?) async throws
        -> Jazz.Client
    {
        // #region context-setup-swift-client
        // An end user's client syncs through your app's Jazz endpoint (its
        // backend's handler). Sign in with a device account, whose key you
        // keep in the keychain, or with your own sign-in's bearer token.
        let auth: Jazz.Auth =
            if let deviceKey {
                .deviceKey(deviceKey)  // from Jazz.generateDeviceKey()
            } else {
                .bearer { _ in try await MySignIn.currentToken() }
            }
        let jazz = try await JazzApp.open(
            .client(appId: appId, serverUrl: endpoint, auth: auth, storage: .memory))
        for try await state in jazz.session {
            if case .signedIn(let user) = state {
                print("signed in as", user.account)
                break
            }
        }
        // #endregion context-setup-swift-client
        return jazz
    }

    /// The app's own sign-in, in the docs snippet: the relay used in tests
    /// accepts `token-<user>`.
    public enum MySignIn {
        public nonisolated(unsafe) static var user = "alice"
        public static func currentToken() async throws -> String? { "token-\(user)" }
    }

    public static func oneShot(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> [JazzApp.Task]
    {
        // #region reading-oneshot-swift
        let tasks = JazzApp.tasks.self
        let all: [JazzApp.Task] = try await jazz.all(
            tasks.query().where(tasks.showId.eq(show.id)))
        let first: JazzApp.Task? = try await jazz.one(
            tasks.query().where(tasks.title.eq("Load in")))
        print(all.count, first?.title ?? "none")
        // #endregion reading-oneshot-swift
        return all
    }

    public static func subscriptions(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> [[JazzApp.Task]]
    {
        var seen: [[JazzApp.Task]] = []
        // #region reading-subscriptions-swift
        let tasks = JazzApp.tasks.self
        let query = tasks.query().where(tasks.showId.eq(show.id)).orderBy(tasks.rank)
        // `watch` yields the whole result each time it changes; `subscribe`
        // yields the deltas (added, updated, removed) instead.
        for try await current in jazz.watch(query) {
            seen.append(current)
            if current.contains(where: { $0.title == "Strike" }) {
                break // leaving the loop unsubscribes
            }
            if seen.count == 1 {
                try await jazz.insert(
                    JazzApp.NewTask(showId: show.id, title: "Strike", status: .todo, rank: 99))
            }
        }
        // #endregion reading-subscriptions-swift
        return seen
    }

    public static func filters(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> [JazzApp.Task]
    {
        // #region reading-filters-swift
        let tasks = JazzApp.tasks.self
        // Conditions passed to `where` are ANDed.
        let open = try await jazz.all(
            tasks.query().where(
                tasks.showId.eq(show.id),
                tasks.status.in([.todo, .doing, .blocked])
            ))
        // #endregion reading-filters-swift
        return open
    }

    public static func sorting(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> [JazzApp.Task]
    {
        // #region reading-sorting-swift
        let tasks = JazzApp.tasks.self
        let ofShow = tasks.query().where(tasks.showId.eq(show.id))
        let byRank = try await jazz.all(ofShow.orderBy(tasks.rank))
        let lastFirst = try await jazz.all(ofShow.orderBy(tasks.rank.desc, tasks.title.asc))
        // #endregion reading-sorting-swift
        _ = lastFirst
        return byRank
    }

    public static func pagination(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> [JazzApp.Task]
    {
        // #region reading-pagination-swift
        let tasks = JazzApp.tasks.self
        let page = 2
        let pageSize = 20
        let rows = try await jazz.all(
            tasks.query()
                .where(tasks.showId.eq(show.id))
                .orderBy(tasks.rank)
                .limit(pageSize)
                .offset((page - 1) * pageSize))
        // #endregion reading-pagination-swift
        return rows
    }

    public static func crud(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> JazzApp.Task?
    {
        // #region writing-crud-swift
        let task = try await jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Focus lights", status: .todo, rank: 3))
        try await jazz.update(
            task.id, JazzApp.TaskPatch(title: .set("Focus the lights"), status: .set(.doing)))
        let updated = try await jazz.one(
            JazzApp.tasks.query().where(JazzApp.tasks.id.eq(task.id)))
        try await jazz.delete(task)
        // #endregion writing-crud-swift
        return updated
    }

    public static func nullableUpdate(_ jazz: Jazz.Client, task: JazzApp.Task) async throws {
        // #region writing-nullable-update-swift
        // `.set(nil)` stores `null`; columns left `.unchanged` keep their value.
        try await jazz.update(task.id, JazzApp.TaskPatch(assigneeId: .set(nil)))
        // #endregion writing-nullable-update-swift
    }

    public static func whereOperators(_ jazz: Jazz.Client) async throws -> [[JazzApp.Task]] {
        let tasks = JazzApp.tasks.self
        // #region where-eq-ne-swift
        let blocked = try await jazz.all(tasks.query().where(tasks.status.eq(.blocked)))
        let notDone = try await jazz.all(tasks.query().where(tasks.status.ne(.done)))
        // #endregion where-eq-ne-swift
        // #region where-numeric-swift
        let topTen = try await jazz.all(
            tasks.query().where(tasks.rank.gte(1), tasks.rank.lt(11)).orderBy(tasks.rank))
        // #endregion where-numeric-swift
        // #region where-contains-swift
        let sound = try await jazz.all(tasks.query().where(tasks.title.contains("Sound")))
        // #endregion where-contains-swift
        // #region where-null-swift
        let unassigned = try await jazz.all(tasks.query().where(tasks.assigneeId.isNull()))
        let assigned = try await jazz.all(tasks.query().where(tasks.assigneeId.isNotNull()))
        // #endregion where-null-swift
        return [blocked, notDone, topTen, sound, unassigned, assigned]
    }

    public static func includes(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> (JazzApp.Show?, [JazzApp.Task])
    {
        // #region reading-reverse-relation-swift
        let shows = JazzApp.shows.self
        // A show with its first three tasks by rank (a reverse relation).
        let withTasks = try await jazz.one(
            shows.query()
                .where(shows.id.eq(show.id))
                .include(shows.tasks.orderBy(JazzApp.tasks.rank).limit(3)))
        print(withTasks?.tasks?.map(\.title) ?? [])
        // #endregion reading-reverse-relation-swift
        // #region reading-includes-swift
        let tasks = JazzApp.tasks.self
        // Each task with its show (a forward relation): `task.show` is `nil`
        // when not included.
        let withShow = try await jazz.all(
            tasks.query().where(tasks.showId.eq(show.id)).include(tasks.show))
        print(withShow.map { "\($0.title) @ \($0.show?.venue ?? "?")" })
        // #endregion reading-includes-swift
        return (withTasks, withShow)
    }

    public struct TaskTitle: Decodable, Sendable, Equatable {
        public var id: String
        public var title: String
    }

    public static func select(_ jazz: Jazz.Client, show: JazzApp.Show) async throws -> [TaskTitle] {
        // #region reading-select-swift
        let tasks = JazzApp.tasks.self
        // Only some columns (and `id`), decoded into a type of your own.
        let titles = try await jazz.all(
            tasks.query().where(tasks.showId.eq(show.id)).orderBy(tasks.rank)
                .select(tasks.title),
            as: TaskTitle.self)
        // #endregion reading-select-swift
        return titles
    }

    public static func transaction(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> JazzApp.Task
    {
        // #region writing-transaction-swift
        // Commits when the closure returns, rolls back when it throws.
        let task = try await jazz.transaction(commit: .global) { tx in
            let task = try await tx.insert(
                JazzApp.NewTask(showId: show.id, title: "Rig lights", status: .todo, rank: 10))
            // Reads inside see the transaction's own writes.
            let open = try await tx.all(
                JazzApp.tasks.query().where(JazzApp.tasks.status.eq(.todo)))
            try await tx.update(task.id, JazzApp.TaskPatch(rank: .set(Double(open.count))))
            return task
        }
        // #endregion writing-transaction-swift
        return task
    }

    public static func readTier(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> [JazzApp.Task]
    {
        // #region reading-durability-tier-swift
        // `.local` (this store), `.localFirst` (the default) or `.remote` (the server).
        let fromServer = try await jazz.all(
            JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(show.id)), tier: .remote)
        // #endregion reading-durability-tier-swift
        return fromServer
    }

    public static func writeTier(_ jazz: Jazz.Client, show: JazzApp.Show) async throws
        -> JazzApp.Task
    {
        // #region writing-durability-tier-swift
        // Waits until the server accepted the write (and its permissions did).
        let task = try await jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Doors", status: .todo, rank: 20),
            tier: .global)
        // Or write locally and wait for everything so far to reach the server.
        try await jazz.update(task.id, JazzApp.TaskPatch(status: .set(.doing)))
        try await jazz.waitForSync(.global)
        // #endregion writing-durability-tier-swift
        return task
    }

    public static func mergeableText(_ jazz: Jazz.Client, task: JazzApp.Task) async throws -> String {
        // #region writing-mergeable-text-swift
        // Edits of a text-merge column (`"merge": "text"`) merge with others'
        // changes instead of replacing the whole value.
        let notes = try await jazz.text(task, JazzApp.tasks.notes)
        try await notes.insert("Bring gaffer tape. ", at: 0) // UTF-16 offsets
        try await notes.replace(0..<5, with: "Pack")
        // #endregion writing-mergeable-text-swift
        return notes.current
    }
}
