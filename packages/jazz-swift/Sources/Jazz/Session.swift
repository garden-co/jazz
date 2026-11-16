import Foundation
import JazzFFI

/// How an end user's client signs in to the app's Jazz endpoint.
public enum Auth: Sendable {
    /// A local-first device account: keep the key (from `Jazz.generateDeviceKey()`)
    /// as a password in the platform's keychain.
    case deviceKey(String)
    /// The app's own sign-in: `tokens` gives the current bearer token, or
    /// `nil` when the user is signed out (which stops the app). Throwing
    /// (or taking over 30 s) is a failed fetch instead: the session stays
    /// signed in and is retried. It's asked again on renewal, expiry and
    /// `refresh()`.
    case bearer(tokens: @Sendable (TokenReason) async throws -> String?)
}

/// Why `Auth.bearer` is asked for a token.
public enum TokenReason: String, Sendable { case connect, renew, expired, refresh }

/// A new device account key (`jazz-auth-v1:…`).
public func generateDeviceKey() throws -> String {
    do {
        return try JazzFFI.generateDeviceKey()
    } catch {
        throw JazzError.wrap(error)
    }
}

/// Who a session acts for.
public struct User: Decodable, Hashable, Sendable {
    public var account: Jazz.ID
    public var issuer: String?
    public var subject: String?
    public var claims: JSONValue?
    /// A local-first device account, or a user the app's backend signed in.
    public var kind: Kind

    public enum Kind: String, Decodable, Hashable, Sendable { case device, backend }
}

/// An end user's session.
public enum SessionState: Hashable, Sendable {
    case signedIn(User)
    /// The token expired; `Auth.bearer` is asked for a new one.
    case expired(User?)
    case signedOut

    struct Raw: Decodable {
        var status: String
        var user: User?
    }

    init(json: String) throws {
        let raw = try Coding.decode(Raw.self, json)
        switch raw.status {
        case "signedIn":
            guard let user = raw.user else { throw JazzError.decoding("signedIn without a user") }
            self = .signedIn(user)
        case "expired": self = .expired(raw.user)
        default: self = .signedOut
        }
    }
}

/// How the client syncs, as TS's `SyncStatus`.
public struct SyncStatus: Decodable, Hashable, Sendable {
    /// The upstream connection.
    public var link: Link
    /// Transactions committed here that the server hasn't accepted or
    /// rejected yet (including ones recovered after a restart).
    public var pendingUploads: Int
    /// When the oldest of them was committed.
    public var oldestPendingAt: Date?
    /// This client's transactions the server rejected since it opened.
    public var rejected: Int
    /// The latest thing that went wrong with sync since the client opened.
    public var lastError: SyncError?
    /// Nothing waits for upload, and the link is connected (or there's no server).
    public var synced: Bool
}

/// The upstream connection, as TS's `SyncStatus.link`.
public enum Link: Decodable, Hashable, Sendable {
    /// No server: the app runs locally.
    case localOnly
    case connecting
    case connected
    /// Not syncing; `retrying` when the client reconnects on its own. With
    /// `reason == .auth`, `auth` says why the server refused the credential.
    case offline(reason: OfflineReason, retrying: Bool, auth: JSONValue?)

    private enum Keys: String, CodingKey { case state, reason, retrying, auth }

    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: Keys.self)
        switch try c.decode(String.self, forKey: .state) {
        case "localOnly": self = .localOnly
        case "connecting": self = .connecting
        case "connected": self = .connected
        default:
            self = .offline(
                reason: (try? c.decode(OfflineReason.self, forKey: .reason)) ?? .disconnected,
                retrying: (try? c.decode(Bool.self, forKey: .retrying)) ?? false,
                auth: try? c.decodeIfPresent(JSONValue.self, forKey: .auth))
        }
    }
}

/// Why the link is offline, as TS's `OfflineReason`.
public enum OfflineReason: String, Decodable, Hashable, Sendable {
    case network, auth, stopped, disconnected
}

/// Something that went wrong with sync, as TS's `SyncError`.
public struct SyncError: Decodable, Hashable, Sendable {
    /// `rejected`, `network`, `auth` or `stopped`.
    public var kind: String
    /// The rejection's code, for `rejected`.
    public var code: String?
    public var message: String
    public var at: Date
}

final class Tokens: TokenProvider, @unchecked Sendable {
    let provide: @Sendable (TokenReason) async throws -> String?
    /// How long a fetch may take before it counts as failed (not signed out).
    static let timeout: TimeInterval = 30
    init(_ provide: @escaping @Sendable (TokenReason) async throws -> String?) { self.provide = provide }

    // Called by jazz-ffi on a thread of its own, which may block. Returning
    // nil signs the user out; throwing is a failed fetch: the session stays
    // signed in and jazz-app retries on its own backoff.
    func token(reason: String) throws -> String? {
        let reason = TokenReason(rawValue: reason) ?? .connect
        let result = Box()
        let done = DispatchSemaphore(value: 0)
        let provide = provide
        let task = _Concurrency.Task {
            do {
                result.set(.success(try await provide(reason)))
            } catch {
                result.set(.failure(error))
            }
            done.signal()
        }
        if done.wait(timeout: .now() + Self.timeout) == .timedOut {
            task.cancel()
            throw JazzFFI.JazzError.Io(reason: "the token provider took longer than \(Int(Self.timeout)) s")
        }
        switch result.get() {
        case .success(let token)?: return token
        case .failure(let error)?:
            throw JazzFFI.JazzError.Io(reason: "the token provider failed: \(error)")
        case nil: throw JazzFFI.JazzError.Io(reason: "the token provider gave no answer")
        }
    }

    final class Box: @unchecked Sendable {
        private let lock = NSLock()
        private var value: Result<String?, any Error>?
        func set(_ value: Result<String?, any Error>) { lock.withLock { self.value = value } }
        func get() -> Result<String?, any Error>? { lock.withLock { value } }
    }
}

final class States: StateListener, @unchecked Sendable {
    let changed: @Sendable (String) -> Void
    init(_ changed: @escaping @Sendable (String) -> Void) { self.changed = changed }
    func onChange(stateJson: String) { changed(stateJson) }
}

extension Client {
    /// The session's current state (`nil` for a backend or a local client).
    public func sessionState() async throws -> SessionState? {
        try await Self.typed {
            guard let json = try await Self.run({ try self.handle.sessionState() }) else { return nil }
            return try SessionState(json: json)
        }
    }

    /// The session's state each time it changes, the current one first. It
    /// fails with `JazzError.closed` once the app closes.
    public var session: AsyncThrowingStream<SessionState, any Error> {
        watch { listener in try self.handle.watchSession(listener: listener) } decode: { try SessionState(json: $0) }
    }

    public func syncStatus() async throws -> SyncStatus {
        try await Self.typed {
            let json = try await Self.run { try self.handle.syncStatus() }
            return try Coding.decode(SyncStatus.self, json)
        }
    }

    /// The sync status each time it changes, the current one first. It fails
    /// with `JazzError.closed` once the app closes.
    public var syncChanges: AsyncThrowingStream<SyncStatus, any Error> {
        watch { listener in try self.handle.watchSync(listener: listener) } decode: {
            try Coding.decode(SyncStatus.self, $0)
        }
    }

    /// Asks `Auth.bearer` for a fresh token now.
    public func refresh() async throws {
        try await Self.typed { try await Self.run { try self.handle.refresh() } }
    }

    /// Signs out and closes the app.
    public func signOut() async throws {
        try await Self.typed { try await Self.run { try self.handle.signOut() } }
    }

    /// Links this device account to the app's sign-in (`bearer` is its
    /// token), returning the user it now acts as.
    public func link(bearer: String) async throws -> User {
        try await Self.typed {
            let json = try await Self.run { try self.handle.link(bearer: bearer) }
            return try Coding.decode(User.self, json)
        }
    }

    func watch<T: Sendable>(
        _ start: @escaping @Sendable (StateListener) throws -> Watch,
        decode: @escaping @Sendable (String) throws -> T
    ) -> AsyncThrowingStream<T, any Error> {
        AsyncThrowingStream { continuation in
            // Set before starting: a state delivered during `start` may end
            // the stream, and the watch must still be cancelled then.
            let holder = WatchHolder()
            continuation.onTermination = { _ in holder.cancel() }
            let listener = States { json in
                // The app closed (`close`, `signOut`): the watch's last state.
                if (try? Coding.decode(Closed.self, json))?.kind == "closed" {
                    continuation.finish(throwing: JazzError.closed)
                    return
                }
                do {
                    continuation.yield(try decode(json))
                } catch {
                    continuation.finish(throwing: JazzError.wrap(error))
                }
            }
            do {
                holder.set(try start(listener))
            } catch {
                continuation.finish(throwing: JazzError.wrap(error))
            }
        }
    }
}

extension Client {
    /// Writes the row with this id: an insert if there's none, else all its
    /// columns replaced by `row`'s.
    @discardableResult
    public func upsert<I: Insert>(_ id: Jazz.ID, _ row: I) async throws -> I.Row {
        try await Self.typed {
            let json = try Coding.json(row, userInfo)
            let stored = try await Self.run { try self.handle.upsert(table: I.Row.table, id: id, rowJson: json) }
            return try Coding.decode(I.Row.self, stored, userInfo)
        }
    }

    /// Brings back deleted row `id` as it was, or with `row` as its content.
    @discardableResult
    public func restore<R: Row>(_ type: R.Type, id: Jazz.ID) async throws -> R {
        try await Self.typed {
            let stored = try await Self.run { try self.handle.restore(table: R.table, id: id, patchJson: nil) }
            return try Coding.decode(R.self, stored, userInfo)
        }
    }

    @discardableResult
    public func restore<I: Insert>(_ id: Jazz.ID, as row: I) async throws -> I.Row {
        try await Self.typed {
            let json = try Coding.json(row, userInfo)
            let stored = try await Self.run { try self.handle.restore(table: I.Row.table, id: id, patchJson: json) }
            return try Coding.decode(I.Row.self, stored, userInfo)
        }
    }
}

extension Transaction {
    @discardableResult
    public func upsert<I: Insert>(_ id: Jazz.ID, _ row: I) async throws -> I.Row {
        try await Client.typed {
            let json = try Coding.json(row, userInfo)
            let stored = try await Client.run { try self.ffi.upsert(table: I.Row.table, id: id, rowJson: json) }
            return try Coding.decode(I.Row.self, stored, userInfo)
        }
    }

    @discardableResult
    public func restore<R: Row>(_ type: R.Type, id: Jazz.ID) async throws -> R {
        try await Client.typed {
            let stored = try await Client.run { try self.ffi.restore(table: R.table, id: id, patchJson: nil) }
            return try Coding.decode(R.self, stored, userInfo)
        }
    }
}

/// The last state a watch gets when the app closes.
struct Closed: Decodable {
    var kind: String?
}

/// A `Watch` cancelled once, whether the stream ended before or after it started.
final class WatchHolder: @unchecked Sendable {
    private let lock = NSLock()
    private var watch: Watch?
    private var cancelled = false

    func set(_ watch: Watch) {
        let cancelNow = lock.withLock { () -> Bool in
            if cancelled { return true }
            self.watch = watch
            return false
        }
        if cancelNow { watch.cancel() }
    }

    func cancel() {
        let watch = lock.withLock { () -> Watch? in
            cancelled = true
            defer { self.watch = nil }
            return self.watch
        }
        watch?.cancel()
    }
}

extension Auth: CustomStringConvertible, CustomDebugStringConvertible {
    /// Never the key: logs and crash reports mustn't carry it.
    public var description: String {
        switch self {
        case .deviceKey: return "Auth.deviceKey(<redacted>)"
        case .bearer: return "Auth.bearer(<token provider>)"
        }
    }
    public var debugDescription: String { description }
}
