import Foundation
import Jazz

/// StagePlan's slice the Swift CLI covers: as the signed-in user, make sure
/// a show exists (they're its chief), list its tasks by `rank`, add tasks
/// and move them between stages.
public struct StagePlanApp: Sendable {
    public let jazz: Jazz.Client
    /// Where reads go: `nil` (local first) in a long-lived app; `.remote`
    /// for a one-shot process (the CLI), whose fresh store hasn't synced yet.
    public var readTier: ReadTier?

    public init(jazz: Jazz.Client, readTier: ReadTier? = nil) {
        self.jazz = jazz
        self.readTier = readTier
    }

    /// Opens StagePlan as an end user of the app's Jazz endpoint (its
    /// backend's handler, in TS, Rust or Go), signed in as `auth` says (in
    /// memory unless `dataDir` is given).
    public static func open(endpoint: String, appId: String, auth: Jazz.Auth, dataDir: String? = nil)
        async throws -> StagePlanApp
    {
        let jazz = try await JazzApp.open(
            .client(
                appId: appId, serverUrl: endpoint, auth: auth,
                storage: dataDir == nil ? .memory : .persistent, dataDir: dataDir))
        // Signed in before anything else.
        for try await state in jazz.session {
            if case .signedIn = state { break }
            if case .signedOut = state { throw JazzError.refused("signed out") }
        }
        return StagePlanApp(jazz: jazz)
    }

    /// The user's show named `name`, created (with them as chief) if they have none.
    public func show(named name: String) async throws -> JazzApp.Show {
        let me = try await jazz.account()
        if let show = try await jazz.one(
            JazzApp.shows.query().where(JazzApp.shows.chiefAccount.eq(me), JazzApp.shows.name.eq(name)),
            tier: readTier)
        {
            return show
        }
        return try await jazz.insert(
            JazzApp.NewShow(name: name, venue: "TBA", date: "TBA", doors: "TBA", chiefAccount: me),
            tier: .global)
    }

    /// The show's tasks, in `rank` order.
    public func tasks(of show: JazzApp.Show) async throws -> [JazzApp.Task] {
        try await tasks(ofShow: show.id)
    }

    /// The tasks of the show with this id the user may read, in `rank` order
    /// (none of a show the app's permissions don't let them see).
    public func tasks(ofShow showId: Jazz.ID) async throws -> [JazzApp.Task] {
        try await jazz.all(
            JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(showId)).orderBy(JazzApp.tasks.rank), tier: readTier)
    }

    /// Adds a task to the bottom of the show's `todo` stage.
    @discardableResult
    public func addTask(_ title: String, to show: JazzApp.Show) async throws -> JazzApp.Task {
        try await addTask(title, toShow: show.id)
    }

    /// Adds a task to the show with this id, waiting until the server
    /// accepted it: a show the user may not write into fails with
    /// `JazzError.permissionDenied`.
    @discardableResult
    public func addTask(_ title: String, toShow showId: Jazz.ID) async throws -> JazzApp.Task {
        let last = try await jazz.one(
            JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(showId)).orderBy(JazzApp.tasks.rank.desc).limit(1),
            tier: readTier)
        return try await jazz.insert(
            JazzApp.NewTask(showId: showId, title: title, status: .todo, rank: (last?.rank ?? 0) + 1),
            tier: .global)
    }

    /// Moves the task to `status`.
    public func move(_ task: Jazz.ID, to status: JazzApp.TaskStatus) async throws {
        try await jazz.update(task, JazzApp.TaskPatch(status: .set(status)), tier: .global)
    }
}

