// The Swift snippets the setup page shows, by region; Tests/StagePlanTests runs
// them. The other pages' Swift snippets are examples/docs/todo-client-swift's.
import Foundation
import Jazz

public enum DocsSnippets {
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

    public static func openSession(endpoint: String, appId: String, dataDir: String) async throws
        -> (JazzSession, Jazz.Client)
    {
        // #region context-setup-swift-session
        // A session follows who is signed in: a device account (a guest)
        // until your sign-in names a user, who takes the guest's account
        // over if they have none yet. Call `session.refresh()` when your
        // sign-in changes.
        let session = JazzApp.session(
            appId: appId, serverUrl: endpoint,
            bearer: { _ in try await MySignIn.currentToken() },  // nil while signed out
            start: .device, dataDir: dataDir)
        // The signed-in user's client, once the session is ready (it throws
        // .noUser if no one is). `session.clients` gives the next one each
        // time the user changes, and nil on sign-out.
        let jazz = try await session.client()
        // #endregion context-setup-swift-session
        return (session, jazz)
    }

    /// The app's own sign-in, in the docs snippet: the relay used in tests
    /// accepts `token-<user>`.
    public enum MySignIn {
        public nonisolated(unsafe) static var user = "alice"
        public static func currentToken() async throws -> String? { "token-\(user)" }
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
