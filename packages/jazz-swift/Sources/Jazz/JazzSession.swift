import Foundation
import JazzFFI

/// Who an app is signed in as, followed over time, as TS's `JazzSession`:
/// it signs in and out (through the app's backend or as a device account)
/// and hands out a `Client` for the user signed in now, replacing it when
/// the user changes.
///
/// ```swift
/// let session = JazzApp.session(appId: appId, serverUrl: endpoint, start: .device)
/// let jazz = try await session.client()        // once someone is signed in
/// for try await jazz in session.clients { … }  // each time the user changes
/// ```
///
/// In SwiftUI, `JazzRoot(session) { … } signedOut: { … }` does this for you.
/// A client kept past a change of user throws `JazzError.replaced`.
public final class JazzSession: Sendable {
    /// How the session starts.
    public enum Start: String, Sendable {
        /// Follow the app's backend: whoever `bearer` names is signed in,
        /// and no one without a token. Needs `bearer`.
        case backend
        /// As the device account this device keeps (a new one if none): a
        /// guest who needs no sign-in.
        case device
        case signedOut = "signed-out"
    }

    /// The session, once opened (it opens in the background).
    private let opening: _Concurrency.Task<JazzFFI.Session, any Error>
    let implementations: (any Sendable)?
    private let current = Current()

    /// A session of `app` (the text of `jazz.app.json`: `JazzApp.app`;
    /// the bindings' `JazzApp.session(…)` passes it) syncing through the
    /// app's Jazz endpoint `serverUrl`. It opens in the background: calls
    /// wait for it, and fail as it does. A session that failed to open
    /// stays failed: make a new one to try again.
    ///
    /// - `bearer`: the app's own sign-in, as `Auth.bearer`: the current
    ///   token, or `nil` when signed out. `refresh()` asks it again.
    /// - `start`: `.backend` (the default with `bearer`), `.device` (the
    ///   default without) or `.signedOut`.
    /// - `linkGuest`: a user `bearer` names who has no account yet takes
    ///   over the device account signed in before (as TS); `false` leaves
    ///   it until `linkDeviceAccount()`.
    /// - Each account's store is under `dataDir/<account>/`, and the device
    ///   account's key in `dataDir/device.key`.
    public init(
        app: String, appId: String, serverUrl: String,
        bearer: (@Sendable (TokenReason) async throws -> String?)? = nil,
        start: Start? = nil, linkGuest: Bool = true,
        storage: Config.Storage = .persistent, dataDir: String? = nil,
        implementations: (any Sendable)? = nil
    ) {
        self.implementations = implementations
        opening = _Concurrency.Task {
            try await Client.typed {
                var config = Config(storage: storage, appId: appId, serverUrl: serverUrl, dataDir: dataDir)
                if storage == .persistent, dataDir == nil {
                    config.dataDir = try Config.defaultDataDir(appId: appId)
                }
                var object = try Coding.decode([String: JSONValue].self, try config.json(app: app))
                if bearer != nil { object["auth"] = .object(["bearer": .bool(true)]) }
                object["initial"] = .string((start ?? (bearer == nil ? .device : .backend)).rawValue)
                object["linkGuest"] = .bool(linkGuest)
                let json = try Coding.json(JSONValue.object(object))
                let tokens = bearer.map(Tokens.init)
                return try await Client.run { try JazzFFI.Session.open(configJson: json, tokens: tokens) }
            }
        }
    }

    /// `init(…)`, returning once the session is open (or failed to).
    public static func open(
        app: String, appId: String, serverUrl: String,
        bearer: (@Sendable (TokenReason) async throws -> String?)? = nil,
        start: Start? = nil, linkGuest: Bool = true,
        storage: Config.Storage = .persistent, dataDir: String? = nil,
        implementations: (any Sendable)? = nil
    ) async throws -> JazzSession {
        let session = JazzSession(
            app: app, appId: appId, serverUrl: serverUrl, bearer: bearer, start: start, linkGuest: linkGuest,
            storage: storage, dataDir: dataDir, implementations: implementations)
        _ = try await session.ffi()
        return session
    }

    func ffi() async throws -> JazzFFI.Session {
        do {
            return try await opening.value
        } catch {
            throw JazzError.wrap(error)
        }
    }

    /// Where the session is now.
    public func snapshot() async throws -> SessionSnapshot {
        try await Client.typed {
            let ffi = try await self.ffi()
            let json = try await Client.run { try ffi.snapshot() }
            return try Coding.decode(SessionSnapshot.self, json)
        }
    }

    /// The session's snapshot each time it changes, the current one first.
    /// It fails with `JazzError.closed` once the session closes.
    public var snapshots: AsyncThrowingStream<SessionSnapshot, any Error> {
        AsyncThrowingStream { continuation in
            let task = _Concurrency.Task {
                do {
                    let ffi = try await self.ffi()
                    let changes = Client.watch { listener in try ffi.watch(listener: listener) } decode: {
                        try Coding.decode(SessionSnapshot.self, $0)
                    }
                    for try await snapshot in changes { continuation.yield(snapshot) }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: JazzError.wrap(error))
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// The client of the user signed in, each time that changes, as TS's
    /// `followDb`: a client when someone is signed in (a sign-in, a switch
    /// of user), `nil` when that client stops being usable (a sign-out, or
    /// a switch under way). It fails with `JazzError.closed` once the
    /// session closes.
    public var clients: AsyncThrowingStream<Client?, any Error> {
        AsyncThrowingStream { continuation in
            let task = _Concurrency.Task {
                do {
                    var last: Client?
                    var gave = false
                    for try await snapshot in self.snapshots {
                        var next: Client?
                        if snapshot.status == .ready {
                            do {
                                next = try await self.client()
                            } catch JazzError.noUser {
                                // Signed out since: the snapshot that says so comes next.
                                next = nil
                            }
                        }
                        // Each client once (client() may already give a
                        // newer epoch's), and nil only after a client.
                        guard next !== last || !gave else { continue }
                        if next == nil && !gave { continue }
                        last = next
                        gave = true
                        continuation.yield(next)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: JazzError.wrap(error))
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// The client for the user signed in now, the same one until the user
    /// changes. While the session starts or switches users it waits; it
    /// throws `JazzError.noUser` when no one is signed in (follow `clients`
    /// to wait for a sign-in), the session's error when it failed, and
    /// `JazzError.closed` once it's closed.
    public func client() async throws -> Client {
        try await Client.typed {
            let ffi = try await self.ffi()
            while true {
                try _Concurrency.Task.checkCancellation()
                let snapshot = try await self.snapshot()
                switch snapshot.status {
                case .starting, .transitioning:
                    // Until it settles.
                    for try await next in self.snapshots
                    where next.status != .starting && next.status != .transitioning {
                        break
                    }
                    continue
                case .signedOut:
                    throw JazzError.noUser(snapshot.error?.message ?? "no one is signed in")
                case .error:
                    throw JazzError.refused(snapshot.error?.message ?? "the session failed")
                case .closed:
                    throw JazzError.closed
                case .ready:
                    break
                }
                let before = snapshot.clientEpoch
                if let client = current.client(at: before) { return client }
                let handle = try await Client.run { try ffi.jazz() }
                // The user may have changed while it was fetched: fetch
                // again then, so the client cached is the epoch's.
                guard try await self.snapshot().clientEpoch == before else { continue }
                let client = Client(handle: handle, implementations: implementations)
                current.set(client, at: before)
                return client
            }
        }
    }

    /// The app's auth changed (a sign-in or sign-out in your app): asks
    /// `bearer` for the token now and follows who it names.
    public func refresh() async throws { try await call { try $0.refresh() } }

    /// Signs in as a new device account.
    public func createDeviceAccount() async throws { try await call { try $0.createDeviceAccount() } }

    /// Signs in as the device account `key` (`jazz-auth-v1:…`, from
    /// `exportDeviceKey()`).
    public func restoreDeviceAccount(key: String) async throws {
        try await call { try $0.restoreDeviceAccount(deviceKey: key) }
    }

    /// Signs in again as the device account this device keeps.
    public func reopenDeviceAccount() async throws { try await call { try $0.reopenDeviceAccount() } }

    /// Links the signed-in device account to the user `bearer` names.
    public func linkDeviceAccount() async throws { try await call { try $0.linkDeviceAccount() } }

    /// The device account's key (`jazz-auth-v1:…`): keep it as a password.
    public func exportDeviceKey() async throws -> String {
        try await Client.typed {
            let ffi = try await self.ffi()
            return try await Client.run { try ffi.exportDeviceKey() }
        }
    }

    /// Signs out. The session stays open, `.signedOut`.
    public func signOut() async throws { try await call { try $0.signOut() } }

    /// Repeats what failed, as the snapshot's `recovery` says.
    public func retry() async throws { try await call { try $0.retry() } }

    /// Closes the session and its client: clients it gave throw
    /// `JazzError.closed`, and `snapshots` ends.
    public func close() async throws { try await call { try $0.shutdown() } }

    private func call(_ body: @escaping @Sendable (JazzFFI.Session) throws -> Void) async throws {
        try await Client.typed {
            let ffi = try await self.ffi()
            try await Client.run { try body(ffi) }
        }
    }

    /// The client of the latest epoch fetched.
    final class Current: @unchecked Sendable {
        private let lock = NSLock()
        private var epoch: UInt64?
        private var cached: Client?

        func client(at epoch: UInt64) -> Client? {
            lock.withLock { self.epoch == epoch ? cached : nil }
        }

        func set(_ client: Client, at epoch: UInt64) {
            lock.withLock {
                self.epoch = epoch
                self.cached = client
            }
        }
    }
}

/// Where a `JazzSession` is, as TS's `JazzSessionSnapshot`.
public struct SessionSnapshot: Decodable, Hashable, Sendable {
    public enum Status: String, Decodable, Hashable, Sendable {
        /// Opening; nothing to show yet.
        case starting
        /// `client()` is usable. `error` may still be set: an operation
        /// failed and left the client as it was.
        case ready
        /// No one is signed in. `error` may say why.
        case signedOut = "signed-out"
        /// Signing in, out, or switching users.
        case transitioning
        /// No usable client: opening, a sign-in or a reopen failed. `retry()`
        /// tries again.
        case error
        case closed
    }

    /// What `retry()` repeats.
    public enum Recovery: String, Decodable, Hashable, Sendable {
        /// The operation that failed; the client is as it was.
        case action
        /// Reopening the signed-in account (or following the backend again).
        case session
    }

    /// A failed operation.
    public struct Failure: Decodable, Hashable, Sendable {
        /// `signedOut`, `server`, `refused`, …
        public var kind: String
        public var message: String
    }

    public var status: Status
    /// Who is signed in; `nil` when no one is.
    public var user: User?
    /// Changes each time `client()` would give another client.
    public var clientEpoch: UInt64
    public var error: Failure?
    public var recovery: Recovery?
}
