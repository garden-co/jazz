import Foundation
import JazzFFI

/// Who an app is signed in as, followed over time, as TS's `JazzSession`:
/// it signs in and out (through the app's backend or as a device account)
/// and hands out a `Client` for the user signed in now, replacing it when
/// the user changes.
///
/// ```swift
/// let session = try await JazzSession.open(
///     app: JazzApp.app, appId: appId, serverUrl: endpoint, start: .device)
/// for try await snapshot in session.snapshots where snapshot.status == .ready {
///     let jazz = try await session.client()
///     …
/// }
/// ```
///
/// Get the client again whenever `clientEpoch` changes: one kept from
/// before throws `JazzError.replaced`.
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

    let ffi: JazzFFI.Session
    let implementations: (any Sendable)?
    private let current = Current()

    init(ffi: JazzFFI.Session, implementations: (any Sendable)?) {
        self.ffi = ffi
        self.implementations = implementations
    }

    /// Opens a session of `app` (the text of `jazz.app.json`:
    /// `JazzApp.app`) syncing through the app's Jazz endpoint `serverUrl`.
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
    public static func open(
        app: String, appId: String, serverUrl: String,
        bearer: (@Sendable (TokenReason) async throws -> String?)? = nil,
        start: Start? = nil, linkGuest: Bool = true,
        storage: Config.Storage = .persistent, dataDir: String? = nil,
        implementations: (any Sendable)? = nil
    ) async throws -> JazzSession {
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
            let ffi = try await Client.run { try JazzFFI.Session.open(configJson: json, tokens: tokens) }
            return JazzSession(ffi: ffi, implementations: implementations)
        }
    }

    /// Where the session is now.
    public func snapshot() async throws -> SessionSnapshot {
        try await Client.typed {
            let json = try await Client.run { try self.ffi.snapshot() }
            return try Coding.decode(SessionSnapshot.self, json)
        }
    }

    /// The session's snapshot each time it changes, the current one first.
    /// It fails with `JazzError.closed` once the session closes.
    public var snapshots: AsyncThrowingStream<SessionSnapshot, any Error> {
        Client.watch { listener in try self.ffi.watch(listener: listener) } decode: {
            try Coding.decode(SessionSnapshot.self, $0)
        }
    }

    /// The client for the user signed in now, the same one until
    /// `clientEpoch` changes. Throws `JazzError.noUser` while no one is
    /// signed in.
    public func client() async throws -> Client {
        try await Client.typed {
            while true {
                let before = try await self.snapshot().clientEpoch
                if let client = current.client(at: before) { return client }
                let handle = try await Client.run { try self.ffi.jazz() }
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
        try await Client.typed { try await Client.run { try self.ffi.exportDeviceKey() } }
    }

    /// Signs out. The session stays open, `.signedOut`.
    public func signOut() async throws { try await call { try $0.signOut() } }

    /// Repeats what failed, as the snapshot's `recovery` says.
    public func retry() async throws { try await call { try $0.retry() } }

    /// Closes the session and its client: clients it gave throw
    /// `JazzError.closed`, and `snapshots` ends.
    public func close() async throws { try await call { try $0.shutdown() } }

    private func call(_ body: @escaping @Sendable (JazzFFI.Session) throws -> Void) async throws {
        let ffi = ffi
        try await Client.typed { try await Client.run { try body(ffi) } }
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
