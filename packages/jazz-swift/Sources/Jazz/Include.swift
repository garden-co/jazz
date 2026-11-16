import Foundation

/// A relation of `From` to rows of `To`, as the bindings declare it
/// (`JazzApp.tasks.assignee`, `JazzApp.shows.tasks`): pass it to
/// `Query.include`, narrowed if you like (`JazzApp.shows.tasks.orderBy(…)
/// .limit(3)`). Included rows land in the row's relation property, which is
/// `nil` when not included.
public struct Relation<From: Row, To: Row>: IncludeSpec, Sendable {
    public let name: String
    var isRequired = false
    var query = Query<To>()

    public init(_ name: String) { self.name = name }

    public func `where`(_ conditions: Condition<To>...) -> Relation {
        var relation = self
        relation.query.conditions += conditions
        return relation
    }

    public func orderBy<V>(_ column: Column<To, V>) -> Relation { orderBy(column.asc) }

    public func orderBy(_ orders: Order<To>...) -> Relation {
        var relation = self
        relation.query.orders += orders
        return relation
    }

    public func limit(_ count: Int) -> Relation {
        var relation = self
        relation.query.limitCount = count
        return relation
    }

    /// Includes the related rows' own relations.
    public func include(_ includes: any IncludeSpec<To>...) -> Relation {
        var relation = self
        relation.query.includes += includes
        return relation
    }

    /// Keeps only rows whose related row is readable (a forward relation
    /// through a non-optional column), as TS's `requireIncludes`.
    public func required() -> Relation {
        var relation = self
        relation.isRequired = true
        return relation
    }

    public func includeJSON() throws -> JSONValue {
        var object: [String: JSONValue] = ["relation": .string(name)]
        if isRequired { object["required"] = .bool(true) }
        let sub = try query.object(withTable: false)
        if !sub.isEmpty { object["query"] = .object(sub) }
        return .object(object)
    }
}

/// What `Query.include` takes: a `Relation` of the query's rows.
public protocol IncludeSpec<From>: Sendable {
    associatedtype From: Row
    func includeJSON() throws -> JSONValue
}

/// An included forward relation's row: boxed, so tables that point at each
/// other don't make row types of infinite size.
public final class Included<Value: Codable & Hashable & Sendable>: Codable, Hashable, Sendable {
    public let value: Value

    public init(_ value: Value) { self.value = value }

    public required init(from decoder: any Decoder) throws {
        value = try decoder.singleValueContainer().decode(Value.self)
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(value)
    }

    public static func == (a: Included, b: Included) -> Bool { a.value == b.value }
    public func hash(into hasher: inout Hasher) { hasher.combine(value) }
}

/// A query with `select`: rows of only some columns (and `id`), decoded
/// into a type of the app's (`jazz.all(selection, as: TaskTitle.self)`).
public struct Selection<R: Row>: Sendable {
    let query: Query<R>
    let entries: [JSONValue]

    func json() throws -> String {
        var object = try query.object(withTable: true)
        object["select"] = .array(entries)
        return try Coding.json(JSONValue.object(object))
    }
}

/// A column of `R` of any type, for `select`.
public protocol AnyColumn<Root>: Sendable {
    associatedtype Root: Row
    var name: String { get }
}

extension Column: AnyColumn {
    public typealias Root = R
}
