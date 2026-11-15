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
/// `UITextView` count). `current` is the text as last read or written here;
/// `changes` follows it as the row changes.
public final class JazzText<R: Row>: @unchecked Sendable {
    let jazz: Client
    public let id: Jazz.ID
    public let column: String
    private let lock = NSLock()
    private var _current: String

    init(jazz: Client, id: Jazz.ID, column: String, current: String) {
        self.jazz = jazz
        self.id = id
        self.column = column
        _current = current
    }

    /// The text as this handle last saw it.
    public var current: String { lock.withLock { _current } }

    /// Sets the text to `text`, merged with others' changes since `current`.
    public func set(_ text: String) async throws(JazzError) {
        let base = current
        try await jazz.editText(R.self, id: id, column: column, from: base, to: text)
        lock.withLock { _current = text }
    }

    public func insert(_ text: String, at offset: Int) async throws(JazzError) {
        try await replace(offset..<offset, with: text)
    }

    public func delete(_ range: Range<Int>) async throws(JazzError) {
        try await replace(range, with: "")
    }

    /// Replaces the UTF-16 range `range` of `current` with `text`.
    public func replace(_ range: Range<Int>, with text: String) async throws(JazzError) {
        let base = current
        let utf16 = Array(base.utf16)
        guard range.lowerBound >= 0, range.upperBound <= utf16.count else {
            throw JazzError.refused("range \(range) is outside the text (\(utf16.count) UTF-16 units)")
        }
        let edited =
            String(decoding: utf16[..<range.lowerBound], as: UTF16.self) + text
            + String(decoding: utf16[range.upperBound...], as: UTF16.self)
        try await set(edited)
    }

    /// The column's text each time the row changes (merged edits included).
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
                        self.lock.withLock { self._current = text }
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
}

extension Client {
    /// Sets text-merge column `column` of row `id` to `text`, an edit of
    /// `base`: changes others made since `base` are kept.
    public func editText<R: Row>(
        _ type: R.Type, id: Jazz.ID, column: String, from base: String, to text: String
    ) async throws(JazzError) {
        try await Self.typed {
            try await Self.run {
                try self.handle.editText(table: R.table, id: id, column: column, editedFrom: base, text: text)
            }
        }
    }

    /// Partial edits of row `id` (and an optional whole-column `patch`), as one write.
    public func edit<P: Patch>(_ id: Jazz.ID, _ patch: P? = nil, edits: [Edit]) async throws(JazzError) {
        try await Self.typed {
            let patchJSON = try patch.map { try Coding.json($0, userInfo) }
            let editsJSON = try Coding.json(JSONValue.array(edits.map { $0.json() }))
            try await Self.run {
                try self.handle.edit(table: P.Row.table, id: id, patchJson: patchJSON, editsJson: editsJSON)
            }
        }
    }

    /// A handle editing text-merge `column` of `row` by merges.
    public func text<R: Row, V>(_ row: R, _ column: Column<R, V>) async throws(JazzError) -> JazzText<R> {
        let current = try await Self.typed { () -> String in
            let json = try Query<R>().where(Column<R, Jazz.ID>("id").eq(row.id)).json()
            guard let found = try await Self.run({ try self.handle.one(queryJson: json) }) else {
                throw JazzError.refused("no row \(row.id) in \(R.table)")
            }
            let object = try Coding.decode([String: JSONValue].self, found)
            if case .string(let text)? = object[column.name] { return text }
            return ""
        }
        return JazzText(jazz: self, id: row.id, column: column.name, current: current)
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
    ) async throws(JazzError) {
        try await Client.typed {
            try await Client.run {
                try self.ffi.editText(table: R.table, id: id, column: column, editedFrom: base, text: text)
            }
        }
    }
}
