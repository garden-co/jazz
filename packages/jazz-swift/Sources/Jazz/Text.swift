import Foundation

/// One part of `Client.edit`: a partial change of one column.
public enum Edit: Sendable {
    /// A text-merge column (`"merge": "text"`) set to `text`, an edit of
    /// `editedFrom` (the value its writer last saw): what others changed
    /// since is kept, as the server merges them.
    case text(column: String, text: String, editedFrom: String)
    /// Splices into a large text or bytes value, within the range
    /// `from..<to` (in `units`) its writer last saw; each splice's `at` is
    /// relative to `from`.
    case splice(column: String, units: SpliceUnits, from: Int, to: Int, splices: [Splice])
    /// Values set at JSON pointers inside a `json` column.
    case jsonSet(column: String, edits: [(pointer: String, value: JSONValue)])

    func json() -> JSONValue {
        switch self {
        case .text(let column, let text, let editedFrom):
            return .object([
                "kind": .string("text"), "column": .string(column), "text": .string(text),
                "editedFrom": .string(editedFrom),
            ])
        case .splice(let column, let units, let from, let to, let splices):
            return .object([
                "kind": .string("splice"), "column": .string(column),
                "within": .object([
                    "units": .string(units.rawValue), "from": .number(Double(from)),
                    "to": .number(Double(to)),
                ]),
                "splices": .array(
                    splices.map {
                        .object([
                            "at": .number(Double($0.at)), "delete": .number(Double($0.delete)),
                            "insert": .string($0.insert),
                        ])
                    }),
            ])
        case .jsonSet(let column, let edits):
            return .object([
                "kind": .string("jsonSet"), "column": .string(column),
                "edits": .array(edits.map { .object(["at": .string($0.pointer), "value": $0.value]) }),
            ])
        }
    }
}

public enum SpliceUnits: String, Sendable { case utf8, utf16, bytes }

/// Deletes `delete` units at `at` and inserts `insert` there (a string, or
/// base64 for bytes).
public struct Splice: Sendable {
    public var at: Int
    public var delete: Int
    public var insert: String
    public init(at: Int, delete: Int = 0, insert: String = "") {
        self.at = at
        self.delete = delete
        self.insert = insert
    }
}

/// A text-merge column of one row, for editing: each edit is a merge with
/// what others changed meanwhile, not a whole-value write. `jazz.text(task,
/// JazzApp.tasks.notes)`. Offsets are in UTF-16 units (as `NSRange` and
/// `UITextView` count); one inside a surrogate pair is refused. Edits run
/// one at a time. `current` is the text as last written here or, once this
/// handle's own writes have come back, as `changes` last delivered it.
public final class JazzText<R: Row>: @unchecked Sendable {
    let jazz: Client
    public let id: Jazz.ID
    public let column: String
    private let lock = NSLock()
    private var _current: String
    /// What this handle wrote that `changes` hasn't echoed yet.
    private var pending: [String] = []
    private let serial = AsyncMutex()

    init(jazz: Client, id: Jazz.ID, column: String, current: String) {
        self.jazz = jazz
        self.id = id
        self.column = column
        _current = current
    }

    /// The text as this handle last saw it.
    public var current: String { lock.withLock { _current } }

    /// Sets the text to `text`, merged with others' changes since `current`.
    public func set(_ text: String) async throws {
        try await edit { _ in text }
    }

    public func insert(_ text: String, at offset: Int) async throws {
        try await replace(offset..<offset, with: text)
    }

    public func delete(_ range: Range<Int>) async throws {
        try await replace(range, with: "")
    }

    /// Replaces the UTF-16 range `range` of `current` with `text`.
    public func replace(_ range: Range<Int>, with text: String) async throws {
        try await edit { base throws in
            let utf16 = Array(base.utf16)
            guard range.lowerBound >= 0, range.upperBound <= utf16.count else {
                throw JazzError.refused("range \(range) is outside the text (\(utf16.count) UTF-16 units)")
            }
            for offset in [range.lowerBound, range.upperBound] where offset < utf16.count {
                if UTF16.isTrailSurrogate(utf16[offset]) {
                    throw JazzError.refused("offset \(offset) is inside a surrogate pair")
                }
            }
            return String(decoding: utf16[..<range.lowerBound], as: UTF16.self) + text
                + String(decoding: utf16[range.upperBound...], as: UTF16.self)
        }
    }

    /// One edit at a time, from `current`.
    private func edit(_ make: (String) throws -> String) async throws {
        await serial.lock()
        defer { serial.unlock() }
        let base = current
        let edited = try make(base)
        lock.withLock { pending.append(edited) }
        do {
            try await jazz.editText(R.self, id: id, column: column, from: base, to: edited)
        } catch {
            lock.withLock { pending.removeAll { $0 == edited } }
            throw error
        }
        lock.withLock { _current = edited }
    }

    /// The column's text each time the row changes (merged edits included).
    /// A late echo of this handle's own write doesn't roll `current` back.
    public var changes: AsyncThrowingStream<String, any Error> {
        let column = column
        let stream = jazz.rawRow(R.self, id: id)
        return AsyncThrowingStream { continuation in
            let task = _Concurrency.Task {
                do {
                    for try await row in stream {
                        guard let row else { continue }
                        let text: String
                        if case .string(let value)? = row[column] { text = value } else { text = "" }
                        self.received(text)
                        continuation.yield(text)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: JazzError.wrap(error))
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    private func received(_ text: String) {
        lock.withLock {
            if let echoed = pending.lastIndex(of: text) {
                // Our write came back: what was pending up to it is settled.
                pending.removeSubrange(...echoed)
                // While newer writes of ours are on their way, the text they
                // were made from stays current.
                if pending.isEmpty { _current = text }
            } else {
                // Anything else is someone else's change, or ours merged
                // with it: it is the text to edit from now (as TS's text
                // field sync).
                pending = []
                _current = text
            }
        }
    }
}

/// A lock held across `await`s: edits of one `JazzText` run one at a time.
final class AsyncMutex: @unchecked Sendable {
    private let lock = NSLock()
    private var locked = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func lock() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            let acquired = lock.withLock { () -> Bool in
                if locked {
                    waiters.append(continuation)
                    return false
                }
                locked = true
                return true
            }
            if acquired { continuation.resume() }
        }
    }

    func unlock() {
        let next = lock.withLock { () -> CheckedContinuation<Void, Never>? in
            if waiters.isEmpty {
                locked = false
                return nil
            }
            return waiters.removeFirst()
        }
        next?.resume()
    }
}

extension Client {
    /// Sets text-merge column `column` of row `id` to `text`, an edit of
    /// `base`: changes others made since `base` are kept.
    public func editText<R: Row>(
        _ type: R.Type, id: Jazz.ID, column: String, from base: String, to text: String
    ) async throws {
        try await Self.typed {
            try await Self.run {
                try self.handle.editText(table: R.table, id: id, column: column, editedFrom: base, text: text)
            }
        }
    }

    /// Partial edits of row `id` of `R`'s table, as one write.
    public func edit<R: Row>(_ type: R.Type, id: Jazz.ID, edits: [Edit]) async throws {
        try await edit(R.table, id: id, patchJSON: nil, edits: edits)
    }

    /// Partial edits of row `id` with a whole-column `patch`, as one write.
    public func edit<P: Patch>(_ id: Jazz.ID, _ patch: P, edits: [Edit]) async throws {
        let patchJSON: String
        do { patchJSON = try Coding.json(patch, userInfo) } catch { throw JazzError.wrap(error) }
        try await edit(P.Row.table, id: id, patchJSON: patchJSON, edits: edits)
    }

    func edit(_ table: String, id: Jazz.ID, patchJSON: String?, edits: [Edit]) async throws {
        try await Self.typed {
            let editsJSON = try Coding.json(JSONValue.array(edits.map { $0.json() }))
            try await Self.run {
                try self.handle.edit(table: table, id: id, patchJson: patchJSON, editsJson: editsJSON)
            }
        }
    }

    /// A handle editing text-merge `column` of `row` by merges.
    public func text<R: Row>(_ row: R, _ column: Column<R, String?>) async throws -> JazzText<R> {
        do {
            return try await textHandle(row, column: column.name)
        } catch {
            throw JazzError.wrap(error)
        }
    }

    /// A handle editing text-merge `column` of `row` by merges.
    public func text<R: Row>(_ row: R, _ column: Column<R, String>) async throws -> JazzText<R> {
        do {
            return try await textHandle(row, column: column.name)
        } catch {
            throw JazzError.wrap(error)
        }
    }

    // Untyped and named apart: Xcode 16's Swift (6.1) crashed emitting the
    // typed-throws overloads calling a same-named generic helper.
    func textHandle<R: Row>(_ row: R, column name: String) async throws -> JazzText<R> {
        let json = try Query<R>().where(Column<R, Jazz.ID>("id").eq(row.id)).json()
        guard let found = try await Self.run({ try self.handle.one(queryJson: json) }) else {
            throw JazzError.refused("no row \(row.id) in \(R.table)")
        }
        let object = try Coding.decode([String: JSONValue].self, found)
        let current: String
        if case .string(let text)? = object[name] { current = text } else { current = "" }
        return JazzText(jazz: self, id: row.id, column: name, current: current)
    }

    /// The row `id` as JSON each time it changes (`nil` once it's gone).
    func rawRow<R: Row>(_ type: R.Type, id: Jazz.ID) -> AsyncThrowingStream<[String: JSONValue]?, any Error> {
        let upstream = raw(Query<R>().where(Column<R, Jazz.ID>("id").eq(id)))
        return AsyncThrowingStream { continuation in
            let task = _Concurrency.Task {
                var row: [String: JSONValue]?
                do {
                    for try await delta in upstream {
                        if delta.reset { row = nil }
                        if let changed = (delta.added + delta.updated).last { row = changed }
                        if delta.removed.contains(id) { row = nil }
                        continuation.yield(row)
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

extension Transaction {
    /// `Client.editText` inside the transaction.
    public func editText<R: Row>(
        _ type: R.Type, id: Jazz.ID, column: String, from base: String, to text: String
    ) async throws {
        try await Client.typed {
            try await Client.run {
                try self.ffi.editText(table: R.table, id: id, column: column, editedFrom: base, text: text)
            }
        }
    }
}
