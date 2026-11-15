import Foundation
import Jazz

/// StagePlan's slice the Swift CLI covers: as the signed-in user, make sure
/// a show exists (they're its chief), list its tasks by `rank`, add tasks
/// and move them between stages.
public struct StagePlanApp: Sendable {
    public let jazz: Jazz.Client

    public init(jazz: Jazz.Client) { self.jazz = jazz }

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

