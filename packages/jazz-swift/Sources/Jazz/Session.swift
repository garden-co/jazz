import Foundation
import JazzFFI

/// How an end user's client signs in to the app's Jazz endpoint.
public enum Auth: Sendable {
    /// A local-first device account: keep the key (from `Jazz.generateDeviceKey()`)
    /// as a password in the platform's keychain.
    case deviceKey(String)
    /// The app's own sign-in: `tokens` gives the current bearer token, or
    /// `nil` when the user is signed out. It's asked again on renewal,
    /// expiry and `refresh()`.
    case bearer(tokens: @Sendable (TokenReason) async throws -> String?)
}

/// Why `Auth.bearer` is asked for a token.
public enum TokenReason: String, Sendable { case connect, renew, expired, refresh }

/// A new device account key (`jazz-auth-v1:…`).
public func generateDeviceKey() throws(JazzError) -> String {
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
    public var kind: String?
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

/// How the client is syncing, as TS's `SyncStatus`.
public struct SyncStatus: Decodable, Hashable, Sendable {
    /// The connection: e.g. "online", "connecting", "offline".
    public var link: String
    /// Local writes the server hasn't acknowledged yet.
    public var pendingUploads: Int?
    /// When the oldest of them was made (milliseconds since the epoch).
    public var oldestPendingAt: Double?
    /// Writes the server rejected.
    public var rejected: Int?
    public var lastError: String?
    /// Whether everything written so far has synced.
    public var synced: Bool?
}

final class Tokens: TokenProvider, @unchecked Sendable {
    let provide: @Sendable (TokenReason) async throws -> String?
    init(_ provide: @escaping @Sendable (TokenReason) async throws -> String?) { self.provide = provide }

    // Called by jazz-ffi on a thread of its own, which may block.
    func token(reason: String) -> String? {
        let reason = TokenReason(rawValue: reason) ?? .connect
        let result = Box()
        let done = DispatchSemaphore(value: 0)
        let provide = provide
        _Concurrency.Task {
            result.value = try? await provide(reason)
            done.signal()
        }
        done.wait()
        return result.value
    }

    final class Box: @unchecked Sendable { var value: String? }
}

final class States: StateListener, @unchecked Sendable {
    let changed: @Sendable (String) -> Void
    init(_ changed: @escaping @Sendable (String) -> Void) { self.changed = changed }
    func onChange(stateJson: String) { changed(stateJson) }
}

extension Client {
    /// The session's current state (`nil` for a backend or a local client).
    public func sessionState() async throws(JazzError) -> SessionState? {
        try await Self.typed {
            guard let json = try await Self.run({ try self.handle.sessionState() }) else { return nil }
            return try SessionState(json: json)
        }
    }

    /// The session's state each time it changes, the current one first.
    public var session: AsyncThrowingStream<SessionState, any Error> {
        watch { listener in try self.handle.watchSession(listener: listener) } decode: { try SessionState(json: $0) }
    }

    public func syncStatus() async throws(JazzError) -> SyncStatus {
        try await Self.typed {
            let json = try await Self.run { try self.handle.syncStatus() }
            return try Coding.decode(SyncStatus.self, json)
        }
    }

    /// The sync status each time it changes, the current one first.
    public var syncChanges: AsyncThrowingStream<SyncStatus, any Error> {
        watch { listener in try self.handle.watchSync(listener: listener) } decode: {
            try Coding.decode(SyncStatus.self, $0)
        }
    }

    /// Asks `Auth.bearer` for a fresh token now.
    public func refresh() async throws(JazzError) {
        try await Self.typed { try await Self.run { try self.handle.refresh() } }
    }

    /// Signs out and closes the app.
    public func signOut() async throws(JazzError) {
        try await Self.typed { try await Self.run { try self.handle.signOut() } }
    }

    /// Links this device account to the app's sign-in (`bearer` is its
    /// token), returning the user it now acts as.
    public func link(bearer: String) async throws(JazzError) -> User {
        try await Self.typed {
            let json = try await Self.run { try self.handle.link(bearer: bearer) }
            return try Coding.decode(User.self, json)
        }
    }

    /// The permissions bundle a backend published.
    public func bundle() async throws(JazzError) -> String? {
        try await Self.typed { try await Self.run { try self.handle.bundle() } }
    }

    func watch<T: Sendable>(
        _ start: @escaping @Sendable (StateListener) throws -> Watch,
        decode: @escaping @Sendable (String) throws -> T
    ) -> AsyncThrowingStream<T, any Error> {
        AsyncThrowingStream { continuation in
            let listener = States { json in
                do {
                    continuation.yield(try decode(json))
                } catch {
                    continuation.finish(throwing: JazzError.wrap(error))
                }
            }
            do {
                let watch = try start(listener)
                continuation.onTermination = { _ in watch.cancel() }
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
    public func upsert<I: Insert>(_ id: Jazz.ID, _ row: I) async throws(JazzError) -> I.Row {
        try await Self.typed {
            let json = try Coding.json(row, userInfo)
            let stored = try await Self.run { try self.handle.upsert(table: I.Row.table, id: id, rowJson: json) }
            return try Coding.decode(I.Row.self, stored, userInfo)
        }
    }

    /// Brings back deleted row `id` as it was, or with `row` as its content.
    @discardableResult
    public func restore<R: Row>(_ type: R.Type, id: Jazz.ID) async throws(JazzError) -> R {
        try await Self.typed {
            let stored = try await Self.run { try self.handle.restore(table: R.table, id: id, patchJson: nil) }
            return try Coding.decode(R.self, stored, userInfo)
        }
    }

    @discardableResult
    public func restore<I: Insert>(_ id: Jazz.ID, as row: I) async throws(JazzError) -> I.Row {
        try await Self.typed {
            let json = try Coding.json(row, userInfo)
            let stored = try await Self.run { try self.handle.restore(table: I.Row.table, id: id, patchJson: json) }
            return try Coding.decode(I.Row.self, stored, userInfo)
        }
    }
}

extension Transaction {
    @discardableResult
    public func upsert<I: Insert>(_ id: Jazz.ID, _ row: I) async throws(JazzError) -> I.Row {
        try await Client.typed {
            let json = try Coding.json(row, userInfo)
            let stored = try await Client.run { try self.ffi.upsert(table: I.Row.table, id: id, rowJson: json) }
            return try Coding.decode(I.Row.self, stored, userInfo)
        }
    }

    @discardableResult
    public func restore<R: Row>(_ type: R.Type, id: Jazz.ID) async throws(JazzError) -> R {
        try await Client.typed {
            let stored = try await Client.run { try self.ffi.restore(table: R.table, id: id, patchJson: nil) }
            return try Coding.decode(R.self, stored, userInfo)
        }
    }
}
