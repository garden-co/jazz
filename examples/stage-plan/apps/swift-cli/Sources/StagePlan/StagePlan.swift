import Foundation
@_spi(Backend) import Jazz

/// StagePlan's slice the Swift CLI covers: as the signed-in user, make sure
/// a show exists (they're its chief), list its tasks by `rank`, add tasks
/// and move them between stages.
public struct StagePlanApp: Sendable {
    public let jazz: Jazz.Client

    public init(jazz: Jazz.Client) { self.jazz = jazz }

    /// The app's `jazz.permissions.json`, as shipped beside `jazz.app.json`.
    public static func permissions() throws -> String {
        guard let url = Bundle.module.url(forResource: "jazz", withExtension: "permissions.json")
        else { throw JazzError.io("jazz.permissions.json is missing from the bundle") }
        return try String(contentsOf: url, encoding: .utf8)
    }

    /// Opens StagePlan as a backend of `server` acting for `user` (in
    /// memory unless `dataDir` is given).
    public static func open(user: String, server: Server, dataDir: String? = nil) async throws
        -> StagePlanApp
    {
        let config = Jazz.Config.backend(
            appId: server.appId, permissions: try permissions(), serverUrl: server.url,
            backendSecret: server.backendSecret, adminSecret: server.adminSecret, user: user,
            storage: dataDir == nil ? .memory : .persistent, dataDir: dataDir)
        return StagePlanApp(jazz: try await JazzApp.open(config))
    }

    /// The user's show named `name`, created (with them as chief) if they have none.
    public func show(named name: String) async throws -> JazzApp.Show {
        let me = try await jazz.account()
        if let show = try await jazz.one(
            JazzApp.shows.query().where(JazzApp.shows.chiefAccount.eq(me), JazzApp.shows.name.eq(name)))
        {
            return show
        }
        return try await jazz.insert(
            JazzApp.NewShow(name: name, venue: "TBA", date: "TBA", doors: "TBA", chiefAccount: me))
    }

    /// The show's tasks, in `rank` order.
    public func tasks(of show: JazzApp.Show) async throws -> [JazzApp.Task] {
        try await jazz.all(JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(show.id)).orderBy(JazzApp.tasks.rank))
    }

    /// Adds a task to the bottom of the show's `todo` stage.
    @discardableResult
    public func addTask(_ title: String, to show: JazzApp.Show) async throws -> JazzApp.Task {
        let last = try await jazz.one(
            JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(show.id)).orderBy(JazzApp.tasks.rank.desc).limit(1))
        return try await jazz.insert(
            JazzApp.NewTask(showId: show.id, title: title, status: .todo, rank: (last?.rank ?? 0) + 1))
    }

    /// Moves the task to `status`.
    public func move(_ task: Jazz.ID, to status: JazzApp.TaskStatus) async throws {
        try await jazz.update(task, JazzApp.TaskPatch(status: .set(status)))
    }
}

/// The sync server a backend publishes to and syncs with.
public struct Server: Sendable {
    public var url: String
    public var appId: String
    public var backendSecret: String
    public var adminSecret: String

    public init(url: String, appId: String, backendSecret: String, adminSecret: String) {
        self.url = url
        self.appId = appId
        self.backendSecret = backendSecret
        self.adminSecret = adminSecret
    }

    /// From `JAZZ_SERVER_URL`, `JAZZ_APP_ID`, `JAZZ_BACKEND_SECRET` and `JAZZ_ADMIN_SECRET`.
    public static func fromEnvironment() -> Server? {
        let env = ProcessInfo.processInfo.environment
        guard let url = env["JAZZ_SERVER_URL"], let appId = env["JAZZ_APP_ID"],
            let backendSecret = env["JAZZ_BACKEND_SECRET"], let adminSecret = env["JAZZ_ADMIN_SECRET"]
        else { return nil }
        return Server(url: url, appId: appId, backendSecret: backendSecret, adminSecret: adminSecret)
    }
}
