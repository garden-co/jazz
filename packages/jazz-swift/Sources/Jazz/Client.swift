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
    /// The app's permissions don't allow it (a write the server refuses
    /// shows up here when waiting for the `global` tier).
    case permissionDenied(String)
    /// Something Jazz for Swift doesn't support yet.
    case notSupported(String)
    /// A serializable transaction lost to another write: retrying may succeed.
    case conflict(String)
    /// The calling task was cancelled.
    case cancelled
    /// A `JazzSession`'s client kept past a change of user (or a sign-out):
    /// get the current one with `session.client()`.
    case replaced(String)
    /// A `JazzSession` with no one signed in has no client.
    case noUser(String)

    public var description: String {
        switch self {
        case .refused(let message): return "refused: \(message)"
        case .io(let message): return "io: \(message)"
        case .closed: return "closed"
        case .decoding(let message): return "decoding: \(message)"
        case .permissionDenied(let message): return "permission denied: \(message)"
        case .notSupported(let message): return "not supported yet: \(message)"
        case .conflict(let message): return "conflict: \(message)"
        case .cancelled: return "cancelled"
        case .replaced(let message): return "replaced: \(message)"
        case .noUser(let message): return "no user: \(message)"
        }
    }

    /// Any failure as a `JazzError`: jazz-ffi's errors by kind, coding
    /// errors as `decoding`.
    static func wrap(_ error: any Error) -> JazzError {
        switch from(error) {
        case let error as JazzError: return error
        case is CancellationError: return .cancelled
        case let error as DecodingError: return .decoding("\(error)")
        case let error as EncodingError: return .decoding("\(error)")
        case let error: return .io("\(error)")
        }
    }

    static func from(_ error: any Error) -> any Error {
        guard let error = error as? JazzFFI.JazzError else { return error }
        switch error {
        case .Refused(let message): return JazzError.refused(message)
        case .Io(let message): return JazzError.io(message)
        case .Closed: return JazzError.closed
        case .Permission(let message): return JazzError.permissionDenied(message)
        case .NotSupported(let message): return JazzError.notSupported(message)
        case .Conflict(let message): return JazzError.conflict(message)
        // A backend client's (jazz-ffi's for_user and close with a
        // timeout); a Swift app never opens one, but the switch is
        // exhaustive.
        case .NoAccount(_, let message): return JazzError.refused(message)
        case .Unsynced(_, let message): return JazzError.io(message)
        case .Replaced(let message): return JazzError.replaced(message)
        case .NoUser(_, let message): return JazzError.noUser(message)
        }
    }
}

/// How to open an app.
public struct Config: Sendable {
    public enum Storage: String, Sendable { case memory, persistent }

    public var storage: Storage
    public var appId: String?
    public var serverUrl: String?
    public var dataDir: String?
    /// An end user signing in through the app's Jazz endpoint (`serverUrl`).
    public var auth: Auth?

    /// Swift apps are clients (`.client`, `.local`); the app's backend runs
    /// in TypeScript (`createJazz`).
    init(storage: Storage, appId: String? = nil, serverUrl: String? = nil, dataDir: String? = nil) {
        self.storage = storage
        self.appId = appId
        self.serverUrl = serverUrl
        self.dataDir = dataDir
    }

    /// An end user's client: it syncs through the app's Jazz endpoint
    /// (`serverUrl`, its backend's handler, never the core) as `auth` says.
    public static func client(
        appId: String, serverUrl: String, auth: Auth, storage: Storage = .persistent,
        dataDir: String? = nil
    ) -> Config {
        var config = Config(storage: storage, appId: appId, serverUrl: serverUrl, dataDir: dataDir)
        config.auth = auth
        return config
    }

    /// A local-only client: no server.
    public static func local(appId: String? = nil, storage: Storage = .memory, dataDir: String? = nil)
        -> Config
    {
        Config(storage: storage, appId: appId, dataDir: dataDir)
    }

    /// Where a persistent store goes without a `dataDir`: Application
    /// Support on Apple platforms, `$XDG_DATA_HOME` (or `~/.local/share`)
    /// elsewhere, under `jazz/<appId>`.
    static func defaultDataDir(appId: String?) throws -> String {
        guard let appId else { throw JazzError.refused("a persistent store needs an appId") }
        #if canImport(Darwin)
            let base = try FileManager.default.url(
                for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
        #else
            let env = ProcessInfo.processInfo.environment
            let base =
                env["XDG_DATA_HOME"].map { URL(fileURLWithPath: $0) }
                ?? FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".local/share")
        #endif
        let dir = base.appendingPathComponent("jazz").appendingPathComponent(appId)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir.path
    }

    func json(app: String) throws -> String {
        var object: [String: JSONValue] = [
            "app": .string(app), "storage": .string(storage.rawValue),
            // Contract 2: permission, not-supported and conflict errors apart.
            "contract": .number(2),
        ]
        object["appId"] = appId.map(JSONValue.string)
        object["serverUrl"] = serverUrl.map(JSONValue.string)
        if storage == .persistent {
            object["dataDir"] = .string(try dataDir ?? Self.defaultDataDir(appId: appId))
        } else {
            object["dataDir"] = dataDir.map(JSONValue.string)
        }
        switch auth {
        case .deviceKey(let key)?: object["auth"] = .object(["deviceKey": .string(key)])
        case .bearer?: object["auth"] = .object(["bearer": .bool(true)])
        case nil: break
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
    /// The app's `JazzApp.Computed` (immutable, and `Sendable` by its protocol).
    let implementations: (any Sendable)?
    var userInfo: [CodingUserInfoKey: any Sendable] { Self.userInfo(implementations) }

    init(handle: JazzHandle, implementations: (any Sendable)?) {
        self.handle = handle
        self.implementations = implementations
    }

    /// Opens `app` (the text of `jazz.app.json`; the bindings' `JazzApp.open`
    /// passes it), with the app's `JazzApp.Computed` if it declares computed
    /// fields or transforms.
    public static func open(app: String, config: Config, implementations: (any Sendable)? = nil)
        async throws -> Client
    {
        try await typed { try await openUntyped(app: app, config: config, implementations: implementations) }
    }

    static func openUntyped(app: String, config: Config, implementations: (any Sendable)?)
        async throws -> Client
    {
        let json = try config.json(app: app)
        let handle: JazzHandle
        if case .bearer(let tokens)? = config.auth {
            let provider = Tokens(tokens)
            handle = try await run { try JazzHandle.openWithTokens(configJson: json, tokens: provider) }
        } else {
            handle = try await run { try JazzHandle.open(configJson: json) }
        }
        return Client(handle: handle, implementations: implementations)
    }

    /// The signed-in user's account id (`session.user` in permissions).
    public func account() async throws -> Jazz.ID {
        try await Self.typed {
            guard let account = try await Self.run({ try self.handle.account() }) else {
                throw JazzError.refused("no one is signed in")
            }
            return account
        }
    }

    /// Every row `query` matches.
    public func all<R>(_ query: Query<R>, tier: ReadTier? = nil) async throws -> [R] {
        try await Self.typed {
            let json = try query.json()
            let rows = try await Self.run {
                if let tier { try self.handle.allAt(queryJson: json, tier: tier.rawValue) } else { try self.handle.all(queryJson: json) }
            }
            return try Coding.decode([R].self, rows, userInfo)
        }
    }

    /// Rows of `selection`'s columns, decoded into `T` (a `Decodable` with
    /// those properties, and `id` if it wants it).
    public func all<R, T: Decodable & Sendable>(_ selection: Selection<R>, as type: T.Type)
        async throws -> [T]
    {
        try await Self.typed {
            let json = try selection.json()
            let rows = try await Self.run { try self.handle.all(queryJson: json) }
            return try Coding.decode([T].self, rows, userInfo)
        }
    }

    /// The first row `query` matches, if any.
    public func one<R>(_ query: Query<R>, tier: ReadTier? = nil) async throws -> R? {
        try await Self.typed {
            let json = try query.json()
            guard
                let row = try await Self.run({
                    if let tier { try self.handle.oneAt(queryJson: json, tier: tier.rawValue) } else { try self.handle.one(queryJson: json) }
                })
            else {
                return nil
            }
            return try Coding.decode(R.self, row, userInfo)
        }
    }

    /// Inserts `row`, returning it as stored (with its `id`).
    @discardableResult
    public func insert<I: Insert>(_ row: I, tier: WriteTier? = nil) async throws -> I.Row {
        try await Self.typed {
            let json = try Coding.json(row, userInfo)
            let inserted = try await Self.run {
                if let tier {
                    try self.handle.insertAt(table: I.Row.table, rowJson: json, tier: tier.rawValue)
                } else {
                    try self.handle.insert(table: I.Row.table, rowJson: json)
                }
            }
            return try Coding.decode(I.Row.self, inserted, userInfo)
        }
    }

    /// Changes the columns `patch` sets on row `id`.
    public func update<P: Patch>(_ id: Jazz.ID, _ patch: P, tier: WriteTier? = nil)
        async throws
    {
        try await Self.typed {
            let json = try Coding.json(patch, userInfo)
            try await Self.run {
                if let tier {
                    try self.handle.updateAt(table: P.Row.table, id: id, patchJson: json, tier: tier.rawValue)
                } else {
                    try self.handle.update(table: P.Row.table, id: id, patchJson: json)
                }
            }
        }
    }

    /// Changes the columns `patch` sets on `row`.
    public func update<P: Patch>(_ row: P.Row, _ patch: P, tier: WriteTier? = nil)
        async throws
    {
        try await update(row.id, patch, tier: tier)
    }

    public func delete<R: Row>(_ type: R.Type, id: Jazz.ID, tier: WriteTier? = nil)
        async throws
    {
        try await Self.typed {
            try await Self.run {
                if let tier {
                    try self.handle.deleteAt(table: R.table, id: id, tier: tier.rawValue)
                } else {
                    try self.handle.delete(table: R.table, id: id)
                }
            }
        }
    }

    public func delete<R: Row>(_ row: R, tier: WriteTier? = nil) async throws {
        try await delete(R.self, id: row.id, tier: tier)
    }

    /// Returns once every write made so far has reached `tier`.
    public func waitForSync(_ tier: WriteTier = .global) async throws {
        try await Self.typed {
            try await Self.run { try self.handle.waitForSync(tier: tier.rawValue) }
        }
    }

    /// Runs `body` in a mergeable transaction (TS's `transaction`): it
    /// commits when `body` returns (waiting for `commit`'s tier, if given)
    /// and rolls back when it throws, rethrowing that error. Inside, reads
    /// see the transaction's own writes. Its writes commit together and
    /// merge with concurrent ones, so its commit is never a conflict; it
    /// works offline. Don't let `tx` escape `body`.
    public func transaction<T: Sendable>(
        commit tier: WriteTier? = nil, _ body: (Transaction) async throws -> T
    ) async throws -> T {
        try await run(transaction: { try self.handle.beginMergeable() }, commit: tier, body)
    }

    /// Runs `body` in a serializable transaction (TS's
    /// `serializableTransaction`), as `transaction` does, but the server
    /// accepts it only if nothing it read changed meanwhile. A
    /// `JazzError.conflict` means another write won: running the
    /// transaction again may succeed.
    public func serializableTransaction<T: Sendable>(
        commit tier: WriteTier? = nil, _ body: (Transaction) async throws -> T
    ) async throws -> T {
        try await run(transaction: { try self.handle.begin() }, commit: tier, body)
    }

    func run<T: Sendable>(
        transaction begin: @escaping @Sendable () throws -> JazzFFI.Transaction,
        commit tier: WriteTier?, _ body: (Transaction) async throws -> T
    ) async throws -> T {
        let ffi = try await Self.typed { try await Self.run(begin) }
        let tx = Transaction(ffi, implementations: implementations)
        let result: T
        do {
            result = try await body(tx)
        } catch {
            try? await Self.run { try ffi.rollback() }
            throw error
        }
        try await Self.typed { try await Self.run { try ffi.commit(tier: tier?.rawValue) } }
        return result
    }

    /// The deltas of `query`'s result, the first a `reset` with all of it.
    /// Stop iterating (or cancel the task) to unsubscribe. It fails with a
    /// `JazzError`.
    public func subscribe<R>(_ query: Query<R>, tier: ReadTier? = nil)
        -> AsyncThrowingStream<Delta<R>, any Error>
    {
        let implementations = implementations
        return raw(query, tier: tier).map { raw in
            let userInfo = Self.userInfo(implementations)
            return Delta(
                added: try raw.added.map { try Self.row($0, userInfo) },
                updated: try raw.updated.map { try Self.row($0, userInfo) },
                removed: raw.removed, reset: raw.reset, coalesced: raw.coalesced ?? false)
        }
    }

    /// `query`'s whole result each time it changes, in its `orderBy` order.
    /// Stop iterating (or cancel the task) to unsubscribe. It fails with a
    /// `JazzError`.
    public func watch<R>(_ query: Query<R>, tier: ReadTier? = nil)
        -> AsyncThrowingStream<[R], any Error>
    {
        let implementations = implementations
        let upstream = raw(query, tier: tier)
        return AsyncThrowingStream { continuation in
            let task = _Concurrency.Task {
                var rows: [String: [String: JSONValue]] = [:]
                let userInfo = Self.userInfo(implementations)
                do {
                    for try await delta in upstream {
                        if delta.reset { rows = [:] }
                        for row in delta.added + delta.updated {
                            if case .string(let id)? = row["id"] { rows[id] = row }
                        }
                        for id in delta.removed { rows[id] = nil }
                        continuation.yield(
                            try query.sort(Array(rows.values)).map { try Self.row($0, userInfo) })
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: JazzError.wrap(error))
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    public func close() async throws {
        try await Self.typed {
            try await Self.run { try self.handle.shutdown() }
        }
    }

    static func userInfo(_ implementations: (any Sendable)?) -> [CodingUserInfoKey: any Sendable] {
        implementations.map { [implementationsKey: $0] } ?? [:]
    }

    static func row<R: Row>(_ object: [String: JSONValue], _ userInfo: [CodingUserInfoKey: any Sendable])
        throws -> R
    {
        try Coding.decode(R.self, Coding.json(JSONValue.object(object)), userInfo)
    }

    func raw<R>(_ query: Query<R>, tier: ReadTier? = nil) -> AsyncThrowingStream<RawDelta, any Error> {
        AsyncThrowingStream { continuation in
            let listener = Listener(continuation: continuation)
            do {
                let json = try query.json()
                let subscription =
                    if let tier {
                        try handle.subscribeAt(queryJson: json, tier: tier.rawValue, listener: listener)
                    } else {
                        try handle.subscribe(queryJson: json, listener: listener)
                    }
                continuation.onTermination = { _ in subscription.cancel() }
            } catch {
                continuation.finish(throwing: JazzError.wrap(error))
            }
        }
    }

    /// `body`'s failures as `JazzError`s.
    static func typed<T>(_ body: () async throws -> T) async throws -> T {
        do {
            return try await body()
        } catch {
            throw JazzError.wrap(error)
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

    func onError(kind: String, message: String) {
        continuation.finish(throwing: Self.error(kind: kind, message: message))
    }

    /// A subscription's last error, by jazz-ffi's kind.
    static func error(kind: String, message: String) -> JazzError {
        switch kind {
        case "closed": return .closed
        case "refused": return .refused(message)
        case "permission": return .permissionDenied(message)
        case "notSupported": return .notSupported(message)
        case "conflict": return .conflict(message)
        case "replaced": return .replaced(message)
        case "noUser": return .noUser(message)
        default: return .io(message)
        }
    }
}

extension AsyncThrowingStream where Failure == any Error {
    /// The stream with `transform` applied to each element, ending on its first error.
    func map<T: Sendable>(_ transform: @escaping @Sendable (Element) throws -> T)
        -> AsyncThrowingStream<T, any Error> where Element: Sendable
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
                    continuation.finish(throwing: JazzError.wrap(error))
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}

extension Config: CustomStringConvertible, CustomDebugStringConvertible {
    /// Without secrets: logs and crash reports mustn't carry them.
    public var description: String {
        var parts = ["storage: \(storage.rawValue)"]
        if let appId { parts.append("appId: \(appId)") }
        if let serverUrl { parts.append("serverUrl: \(serverUrl)") }
        if let dataDir { parts.append("dataDir: \(dataDir)") }
        if let auth { parts.append("auth: \(auth)") }
        return "Config(\(parts.joined(separator: ", ")))"
    }
    public var debugDescription: String { description }
}
