// The Swift snippets the docs show, by region; Tests/StagePlanTests runs them.
import Foundation
import Jazz

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
                user: userId, // the session's user: permissions see it as `session.user`
                storage: .memory
            ))
        // #endregion context-setup-swift-backend
        return jazz
    }

    public static func openClient() async throws -> Jazz.Client {
        // #region context-setup-swift-client
        let jazz = try await JazzApp.open(
            .client(
                appId: "<your app id>",
                serverUrl: "https://<your sync server>",
                storage: .persistent,
                dataDir: FileManager.default.temporaryDirectory.path
            ))
        // #endregion context-setup-swift-client
        return jazz
    }

    public static func oneShot(_ jazz: Jazz.Client, show: Show) async throws -> [Task] {
        // #region reading-oneshot-swift
        let tasks: [Task] = try await jazz.all(Tasks.query().where(Tasks.showId.eq(show.id)))
        let first: Task? = try await jazz.one(Tasks.query().where(Tasks.title.eq("Load in")))
        print(tasks.count, first?.title ?? "none")
        // #endregion reading-oneshot-swift
        return tasks
    }

    public static func subscriptions(_ jazz: Jazz.Client, show: Show) async throws -> [[Task]] {
        var seen: [[Task]] = []
        // #region reading-subscriptions-swift
        let query = Tasks.query().where(Tasks.showId.eq(show.id)).orderBy(Tasks.rank)
        // `watch` yields the whole result each time it changes; `subscribe`
        // yields the deltas (added, updated, removed) instead.
        for try await tasks in jazz.watch(query) {
            seen.append(tasks)
            if tasks.contains(where: { $0.title == "Strike" }) {
                break // leaving the loop unsubscribes
            }
            if seen.count == 1 {
                try await jazz.insert(NewTask(showId: show.id, title: "Strike", status: .todo, rank: 99))
            }
        }
        // #endregion reading-subscriptions-swift
        return seen
    }

    public static func filters(_ jazz: Jazz.Client, show: Show) async throws -> [Task] {
        // #region reading-filters-swift
        // Conditions passed to `where` are ANDed.
        let open = try await jazz.all(
            Tasks.query().where(
                Tasks.showId.eq(show.id),
                Tasks.status.in([.todo, .doing, .blocked])
            ))
        // #endregion reading-filters-swift
        return open
    }

    public static func sorting(_ jazz: Jazz.Client, show: Show) async throws -> [Task] {
        // #region reading-sorting-swift
        let byRank = try await jazz.all(
            Tasks.query().where(Tasks.showId.eq(show.id)).orderBy(Tasks.rank))
        let newestFirst = try await jazz.all(
            Tasks.query().where(Tasks.showId.eq(show.id)).orderBy(Tasks.rank.desc, Tasks.title.asc))
        // #endregion reading-sorting-swift
        _ = newestFirst
        return byRank
    }

    public static func pagination(_ jazz: Jazz.Client, show: Show) async throws -> [Task] {
        // #region reading-pagination-swift
        let page = 2
        let pageSize = 20
        let tasks = try await jazz.all(
            Tasks.query()
                .where(Tasks.showId.eq(show.id))
                .orderBy(Tasks.rank)
                .limit(pageSize)
                .offset((page - 1) * pageSize))
        // #endregion reading-pagination-swift
        return tasks
    }

    public static func crud(_ jazz: Jazz.Client, show: Show) async throws -> Task? {
        // #region writing-crud-swift
        let task = try await jazz.insert(
            NewTask(showId: show.id, title: "Focus lights", status: .todo, rank: 3))
        try await jazz.update(task.id, TaskPatch(title: .set("Focus the lights"), status: .set(.doing)))
        let updated = try await jazz.one(Tasks.query().where(Tasks.id.eq(task.id)))
        try await jazz.delete(task)
        // #endregion writing-crud-swift
        return updated
    }

    public static func nullableUpdate(_ jazz: Jazz.Client, task: Task) async throws {
        // #region writing-nullable-update-swift
        // `.set(nil)` stores `null`; columns left `.unchanged` keep their value.
        try await jazz.update(task.id, TaskPatch(assigneeId: .set(nil)))
        // #endregion writing-nullable-update-swift
    }

    public static func whereOperators(_ jazz: Jazz.Client, show: Show) async throws -> [[Task]] {
        // #region where-eq-ne-swift
        let blocked = try await jazz.all(Tasks.query().where(Tasks.status.eq(.blocked)))
        let notDone = try await jazz.all(Tasks.query().where(Tasks.status.ne(.done)))
        // #endregion where-eq-ne-swift
        // #region where-numeric-swift
        let topTen = try await jazz.all(
            Tasks.query().where(Tasks.rank.gte(1), Tasks.rank.lt(11)).orderBy(Tasks.rank))
        // #endregion where-numeric-swift
        // #region where-contains-swift
        let sound = try await jazz.all(Tasks.query().where(Tasks.title.contains("Sound")))
        // #endregion where-contains-swift
        // #region where-null-swift
        let unassigned = try await jazz.all(Tasks.query().where(Tasks.assigneeId.isNull()))
        let assigned = try await jazz.all(Tasks.query().where(Tasks.assigneeId.isNotNull()))
        // #endregion where-null-swift
        return [blocked, notDone, topTen, sound, unassigned, assigned]
    }
}
