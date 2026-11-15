import Foundation

/// What `Client.all` and `one` read: a query, or a hop, gather or union of queries.
public protocol ReadQuery<Row>: Sendable {
    associatedtype Row: Jazz.Row
    func object() throws -> [String: JSONValue]
}

extension Query: ReadQuery {
    public typealias Row = R
    public func object() throws -> [String: JSONValue] { try object(withTable: true) }

    /// Reads deleted rows too.
    public func includeDeleted() -> Query<R> {
        var query = self
        query.withDeleted = true
        return query
    }

    /// The rows reached from this query's rows along `relation`: the
    /// query's clauses filter the rows hopped from; the hop's own `where`,
    /// `orderBy`, `limit` and `offset` the rows reached.
    public func hopTo<T>(_ relation: Relation<R, T>) -> Hop<T> {
        Hop(base: self.erased(), hops: [relation.name])
    }

    /// This query's rows and every row reached from them along `relation`,
    /// a forward relation of the table to itself, up to `maxDepth` (10).
    public func gather(_ relation: Relation<R, R>, maxDepth: Int? = nil) -> Gathered<R> {
        Gathered(query: self, relation: relation.name, maxDepth: maxDepth)
    }

    /// The versions of this query's rows the server accepted, oldest first
    /// unless ordered by `JazzApp.<table>.acceptedAt`-style `"$acceptedAt"`.
    /// Read it at tier `.remote`; it can't be subscribed to yet.
    public func history() -> History<R> { History(query: self) }

    func erased() -> AnyQueryObject { AnyQueryObject { try self.object(withTable: true) } }
}

struct AnyQueryObject: Sendable {
    let make: @Sendable () throws -> [String: JSONValue]
}

/// Rows of `To` reached by `hopTo`.
public struct Hop<To: Row>: ReadQuery {
    public typealias Row = To
    let base: AnyQueryObject
    var hops: [String]
    var after = Query<To>()

    /// Hops on along `relation`.
    public func hopTo<Next>(_ relation: Relation<To, Next>) -> Hop<Next> {
        Hop<Next>(base: base, hops: hops + [relation.name])
    }

    public func `where`(_ conditions: Condition<To>...) -> Hop {
        var hop = self
        hop.after.conditions += conditions
        return hop
    }

    public func orderBy(_ orders: Order<To>...) -> Hop {
        var hop = self
        hop.after.orders += orders
        return hop
    }

    public func limit(_ count: Int) -> Hop {
        var hop = self
        hop.after.limitCount = count
        return hop
    }

    public func offset(_ count: Int) -> Hop {
        var hop = self
        hop.after.offsetCount = count
        return hop
    }

    public func object() throws -> [String: JSONValue] {
        var object = try base.make()
        object["hopTo"] = hops.count == 1 ? .string(hops[0]) : .array(hops.map(JSONValue.string))
        let after = try self.after.object(withTable: false)
        if !after.isEmpty { object["after"] = .object(after) }
        return object
    }
}

/// A query's rows and the rows reached along a self-relation (`gather`).
public struct Gathered<R: Row>: ReadQuery {
    public typealias Row = R
    let query: Query<R>
    let relation: String
    let maxDepth: Int?

    public func object() throws -> [String: JSONValue] {
        var object = try query.object(withTable: true)
        var gather: [String: JSONValue] = ["relation": .string(relation)]
        if let maxDepth { gather["maxDepth"] = .number(Double(maxDepth)) }
        object["gather"] = .object(gather)
        return object
    }
}

/// Every arm's rows, as TS's union: a row two arms match comes twice, and
/// `limit`/`offset` count each occurrence. Arms take `where` only.
public struct Union<R: Row>: ReadQuery {
    public typealias Row = R
    var arms: [Query<R>]
    var outer = Query<R>()

    public init(_ arms: Query<R>...) { self.arms = arms }

    public func orderBy(_ orders: Order<R>...) -> Union {
        var union = self
        union.outer.orders += orders
        return union
    }

    public func limit(_ count: Int) -> Union {
        var union = self
        union.outer.limitCount = count
        return union
    }

    public func offset(_ count: Int) -> Union {
        var union = self
        union.outer.offsetCount = count
        return union
    }

    public func object() throws -> [String: JSONValue] {
        var object = try outer.object(withTable: false)
        object["union"] = .array(try arms.map { .object(try $0.object(withTable: true)) })
        return object
    }
}

/// A query's accepted versions (`Query.history()`).
public struct History<R: Row>: Sendable {
    let query: Query<R>

    func json() throws -> String {
        var object = try query.object(withTable: true)
        object["history"] = .bool(true)
        return try Coding.json(JSONValue.object(object))
    }
}

/// One accepted version of a row.
public struct Version<R: Row>: Sendable {
    public var row: R
    /// The transaction that wrote it.
    public var transactionId: String
    /// When the server accepted it.
    public var acceptedAt: Date
}

struct HistoryMeta: Decodable {
    struct Inner: Decodable {
        var transactionId: String
        var acceptedAt: Date
    }
    var history: Inner
    enum CodingKeys: String, CodingKey { case history = "$history" }
}

/// A part of a large value for `select`: a range of a text or bytes
/// column, or a value inside a json column. Root queries only.
public struct SelectPart<R: Row>: Sendable {
    let json: JSONValue
}

extension Column {
    /// Units `from..<to` of this text or bytes column (`.bytes` come back base64).
    public func part(_ range: Range<Int>, in units: SpliceUnits) -> SelectPart<R> {
        SelectPart(json: .object([
            "column": .string(name), "from": .number(Double(range.lowerBound)),
            "to": .number(Double(range.upperBound)), "units": .string(units.rawValue),
        ]))
    }

    /// The value at JSON pointer `pointer` inside this json column.
    public func at(_ pointer: String) -> SelectPart<R> {
        SelectPart(json: .object(["column": .string(name), "at": .string(pointer)]))
    }

    var whole: SelectPart<R> { SelectPart(json: .string(name)) }
}

extension Query {
    /// Whole columns and parts of large values, decoded with `jazz.all(_:as:)`.
    public func select(parts: SelectPart<R>...) -> Selection<R> {
        Selection(query: self, entries: parts.map(\.json))
    }
}

extension Client {
    /// Every row `query` (a hop, gather or union) reads.
    public func all<Q: ReadQuery>(_ query: Q, tier: ReadTier? = nil) async throws(JazzError) -> [Q.Row] {
        try await Self.typed {
            let json = try Coding.json(JSONValue.object(try query.object()))
            let rows = try await Self.run {
                if let tier { try self.handle.allAt(queryJson: json, tier: tier.rawValue) } else { try self.handle.all(queryJson: json) }
            }
            return try Coding.decode([Q.Row].self, rows, userInfo)
        }
    }

    /// The accepted versions `history` reads, from the server.
    public func all<R>(_ history: History<R>, tier: ReadTier = .remote) async throws(JazzError) -> [Version<R>] {
        try await Self.typed {
            let json = try history.json()
            let rows = try await Self.run { try self.handle.allAt(queryJson: json, tier: tier.rawValue) }
            let decoded = try Coding.decode([R].self, rows, userInfo)
            let meta = try Coding.decode([HistoryMeta].self, rows)
            return zip(decoded, meta).map {
                Version(row: $0, transactionId: $1.history.transactionId, acceptedAt: $1.history.acceptedAt)
            }
        }
    }
}

/// The bytes of a streamed upload.
extension Client {
    /// Inserts `row` with `column` (bytes or text) streamed from `bytes`.
    public func upload<I: Insert, S: AsyncSequence & Sendable>(
        _ row: I, column: String, from bytes: S
    ) async throws(JazzError) -> I.Row where S.Element == Data {
        try await upload(I.Row.self, column: column, kind: "insert", id: nil, row: row, from: bytes)
    }

    /// Replaces `column` of row `id` with what `bytes` streams.
    public func upload<R: Row, S: AsyncSequence & Sendable>(
        _ type: R.Type, id: Jazz.ID, column: String, from bytes: S
    ) async throws(JazzError) -> R where S.Element == Data {
        try await upload(R.self, column: column, kind: "update", id: id, row: Optional<Never?>.none, from: bytes)
    }

    func upload<R: Row, E: Encodable, S: AsyncSequence & Sendable>(
        _ type: R.Type, column: String, kind: String, id: Jazz.ID?, row: E?, from bytes: S
    ) async throws(JazzError) -> R where S.Element == Data {
        try await Self.typed {
            let rowJSON = try row.map { try Coding.json($0, userInfo) }
            let upload = try await Self.run {
                try self.handle.beginUpload(table: R.table, column: column, kind: kind, id: id, rowJson: rowJSON)
            }
            do {
                for try await chunk in bytes {
                    try await Self.run { try upload.push(bytes: chunk) }
                }
                let stored = try await Self.run { try upload.finish() }
                return try Coding.decode(R.self, stored, userInfo)
            } catch {
                try? await Self.run { try upload.abort() }
                throw error
            }
        }
    }
}
