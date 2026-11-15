import Foundation
import JazzFFI

/// Why a Jazz call failed.
public enum JazzError: Error, Equatable, Sendable, CustomStringConvertible {
    /// Refused: validation, permissions, or not supported yet.
    case refused(String)
    /// Storage or network.
    case io(String)
    /// The client was closed.
    case closed
    /// A row or delta didn't decode into the bindings' types (stale bindings?).
    case decoding(String)

    public var description: String {
        switch self {
        case .refused(let message): return "refused: \(message)"
        case .io(let message): return "io: \(message)"
        case .closed: return "closed"
        case .decoding(let message): return "decoding: \(message)"
        }
    }

    static func from(_ error: any Error) -> any Error {
        guard let error = error as? JazzFFI.JazzError else { return error }
        switch error {
        case .Refused(let message): return JazzError.refused(message)
        case .Io(let message): return JazzError.io(message)
        case .Closed: return JazzError.closed
        }
    }
}

/// How to open an app.
public struct Config: Sendable {
    public enum Storage: String, Sendable { case memory, persistent }

    /// The session a backend acts in: `subject` is the user's id.
    public struct Session: Sendable {
        public var issuer: String
        public var subject: String
        public init(issuer: String = "jazz:backend", subject: String) {
            self.issuer = issuer
            self.subject = subject
        }
    }

    public var storage: Storage
    public var appId: String?
    public var serverUrl: String?
    public var dataDir: String?
    /// The text of `jazz.permissions.json`: set, the app opens as a backend.
    public var permissions: String?
    public var backendSecret: String?
    public var adminSecret: String?
    public var session: Session?

    public init(
        storage: Storage = .memory, appId: String? = nil, serverUrl: String? = nil,
        dataDir: String? = nil, permissions: String? = nil, backendSecret: String? = nil,
        adminSecret: String? = nil, session: Session? = nil
    ) {
        self.storage = storage
        self.appId = appId
        self.serverUrl = serverUrl
        self.dataDir = dataDir
        self.permissions = permissions
        self.backendSecret = backendSecret
        self.adminSecret = adminSecret
        self.session = session
    }

    /// A client of the server at `serverUrl`.
    public static func client(
        appId: String, serverUrl: String, storage: Storage = .memory, dataDir: String? = nil
    ) -> Config {
        Config(storage: storage, appId: appId, serverUrl: serverUrl, dataDir: dataDir)
    }

    /// A backend with its `jazz.permissions.json`, acting for `user`.
    public static func backend(
        appId: String, permissions: String, serverUrl: String? = nil,
        backendSecret: String? = nil, adminSecret: String? = nil, user: String? = nil,
        storage: Storage = .memory, dataDir: String? = nil
    ) -> Config {
        Config(
            storage: storage, appId: appId, serverUrl: serverUrl, dataDir: dataDir,
            permissions: permissions, backendSecret: backendSecret, adminSecret: adminSecret,
            session: user.map { Session(subject: $0) })
    }

    func json(app: String) throws -> String {
        var object: [String: JSONValue] = [
            "app": .string(app), "storage": .string(storage.rawValue),
        ]
        object["permissions"] = permissions.map(JSONValue.string)
        object["appId"] = appId.map(JSONValue.string)
        object["serverUrl"] = serverUrl.map(JSONValue.string)
        object["dataDir"] = dataDir.map(JSONValue.string)
        object["backendSecret"] = backendSecret.map(JSONValue.string)
        object["adminSecret"] = adminSecret.map(JSONValue.string)
        if let session {
            object["session"] = .object([
                "issuer": .string(session.issuer), "subject": .string(session.subject),
            ])
        }
        return try Coding.json(JSONValue.object(object))
    }
}

/// What changed in a subscription's result.
public struct Delta<R: Row>: Sendable {
    public var added: [R]
    public var updated: [R]
    public var removed: [Jazz.ID]
    /// The full result, replacing what came before: the first delta is one.
    public var reset: Bool
    public var coalesced: Bool
}

struct RawDelta: Decodable {
    var added: [[String: JSONValue]]
    var updated: [[String: JSONValue]]
    var removed: [String]
    var reset: Bool
    var coalesced: Bool?
}

/// An open app. Every call runs jazz-ffi off the caller's thread.
public final class Client: Sendable {
    let handle: JazzHandle

    init(handle: JazzHandle) { self.handle = handle }

    /// Opens `app` (the text of `jazz.app.json`; the bindings' `JazzApp.open` passes it).
    public static func open(app: String, config: Config) async throws -> Client {
        let json = try config.json(app: app)
        return Client(handle: try await run { try JazzHandle.open(configJson: json) })
    }

    /// The account id of the user a backend acts for (`session.user` in
    /// permissions). A client has none: this throws `refused`.
    public func account() async throws -> Jazz.ID {
        guard let account = try await Self.run({ try self.handle.account() }) else {
            throw JazzError.refused("account() needs a backend acting for a user (`Config.backend(…, user:)`)")
        }
        return account
    }

    /// Every row `query` matches.
    public func all<R>(_ query: Query<R>) async throws -> [R] {
        let json = try query.json()
        let rows = try await Self.run { try self.handle.all(queryJson: json) }
        return try Coding.decode([R].self, rows)
    }

    /// The first row `query` matches, if any.
    public func one<R>(_ query: Query<R>) async throws -> R? {
        let json = try query.json()
        guard let row = try await Self.run({ try self.handle.one(queryJson: json) }) else {
            return nil
        }
        return try Coding.decode(R.self, row)
    }

    /// Inserts `row`, returning it as stored (with its `id`).
    @discardableResult
    public func insert<I: Insert>(_ row: I) async throws -> I.Row {
        let json = try Coding.json(row)
        let inserted = try await Self.run {
            try self.handle.insert(table: I.Row.table, rowJson: json)
        }
        return try Coding.decode(I.Row.self, inserted)
    }

    /// Changes the columns `patch` sets on row `id`.
    public func update<P: Patch>(_ id: Jazz.ID, _ patch: P) async throws {
        let json = try Coding.json(patch)
        try await Self.run { try self.handle.update(table: P.Row.table, id: id, patchJson: json) }
    }

    /// Changes the columns `patch` sets on `row`.
    public func update<P: Patch>(_ row: P.Row, _ patch: P) async throws {
        try await update(row.id, patch)
    }

    public func delete<R: Row>(_ type: R.Type, id: Jazz.ID) async throws {
        try await Self.run { try self.handle.delete(table: R.table, id: id) }
    }

    public func delete<R: Row>(_ row: R) async throws {
        try await delete(R.self, id: row.id)
    }

    /// The deltas of `query`'s result, the first a `reset` with all of it.
    /// Stop iterating (or cancel the task) to unsubscribe.
    public func subscribe<R>(_ query: Query<R>) -> AsyncThrowingStream<Delta<R>, any Error> {
        raw(query).map { raw in
            Delta(
                added: try raw.added.map(Self.row), updated: try raw.updated.map(Self.row),
                removed: raw.removed, reset: raw.reset, coalesced: raw.coalesced ?? false)
        }
    }

    /// `query`'s whole result each time it changes, in its `orderBy` order.
    /// Stop iterating (or cancel the task) to unsubscribe.
    public func watch<R>(_ query: Query<R>) -> AsyncThrowingStream<[R], any Error> {
        var rows: [String: [String: JSONValue]] = [:]
        return raw(query).map { delta in
            if delta.reset { rows = [:] }
            for row in delta.added + delta.updated {
                if case .string(let id)? = row["id"] { rows[id] = row }
            }
            for id in delta.removed { rows[id] = nil }
            return try query.sort(Array(rows.values)).map(Self.row)
        }
    }

    public func close() async throws {
        try await Self.run { try self.handle.shutdown() }
    }

    static func row<R: Row>(_ object: [String: JSONValue]) throws -> R {
        try Coding.decode(R.self, Coding.json(JSONValue.object(object)))
    }

    func raw<R>(_ query: Query<R>) -> AsyncThrowingStream<RawDelta, any Error> {
        AsyncThrowingStream { continuation in
            let listener = Listener(continuation: continuation)
            do {
                let json = try query.json()
                let subscription = try handle.subscribe(queryJson: json, listener: listener)
                continuation.onTermination = { _ in subscription.cancel() }
            } catch {
                continuation.finish(throwing: JazzError.from(error))
            }
        }
    }

    /// Runs a blocking jazz-ffi call on a background thread.
    static func run<T: Sendable>(_ call: @escaping @Sendable () throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global().async {
                do {
                    continuation.resume(returning: try call())
                } catch {
                    continuation.resume(throwing: JazzError.from(error))
                }
            }
        }
    }
}

final class Listener: SubscriptionListener, @unchecked Sendable {
    let continuation: AsyncThrowingStream<RawDelta, any Error>.Continuation

    init(continuation: AsyncThrowingStream<RawDelta, any Error>.Continuation) {
        self.continuation = continuation
    }

    func onDelta(deltaJson: String) {
        do {
            continuation.yield(try Coding.decode(RawDelta.self, deltaJson))
        } catch {
            continuation.finish(throwing: error)
        }
    }

    func onError(message: String) {
        continuation.finish(throwing: JazzError.io(message))
    }
}

extension AsyncThrowingStream where Failure == any Error {
    /// The stream with `transform` applied to each element, ending on its first error.
    func map<T: Sendable>(_ transform: @escaping (Element) throws -> T)
        -> AsyncThrowingStream<T, any Error>
    {
        let upstream = self
        return AsyncThrowingStream<T, any Error> { continuation in
            let task = Task {
                do {
                    for try await element in upstream {
                        continuation.yield(try transform(element))
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}
