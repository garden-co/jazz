import Foundation

/// A typed column of `R`: what filters and `orderBy` name.
public struct Column<R: Row, Value: Encodable & Sendable>: Sendable {
    public let name: String

    public init(_ name: String) { self.name = name }

    func condition(_ op: String, _ value: (any Encodable & Sendable)?) -> Condition<R> {
        Condition(column: name, op: op, value: value)
    }

    public func eq(_ value: Value) -> Condition<R> { condition("eq", value) }
    public func ne(_ value: Value) -> Condition<R> { condition("ne", value) }
    public func lt(_ value: Value) -> Condition<R> { condition("lt", value) }
    public func lte(_ value: Value) -> Condition<R> { condition("lte", value) }
    public func gt(_ value: Value) -> Condition<R> { condition("gt", value) }
    public func gte(_ value: Value) -> Condition<R> { condition("gte", value) }
    /// Rows whose value is one of `values`.
    public func `in`(_ values: [Value]) -> Condition<R> { condition("in", values) }
    /// Text containing `part`, or an array containing an element.
    public func contains<Part: Encodable & Sendable>(_ part: Part) -> Condition<R> {
        condition("contains", part)
    }
    public func isNull() -> Condition<R> { condition("isNull", nil) }
    public func isNotNull() -> Condition<R> { condition("isNotNull", nil) }

    public var asc: Order<R> { Order(column: name, direction: .asc) }
    public var desc: Order<R> { Order(column: name, direction: .desc) }
}

/// One `where` entry.
public struct Condition<R: Row>: Sendable {
    let column: String
    let op: String
    let value: (any Encodable & Sendable)?

    func json() throws -> JSONValue {
        var entry: [String: JSONValue] = ["column": .string(column), "op": .string(op)]
        if let value { entry["value"] = try Coding.value(value) }
        return .object(entry)
    }
}

public enum Direction: String, Sendable { case asc, desc }

/// One `orderBy` entry.
public struct Order<R: Row>: Sendable {
    let column: String
    let direction: Direction
}

/// A query of `R`'s table: the bindings' `query()`, narrowed. Its JSON is
/// the query JSON jazz-ffi reads; `where` conditions are ANDed.
public struct Query<R: Row>: Sendable {
    var conditions: [Condition<R>] = []
    var orders: [Order<R>] = []
    var limitCount: Int?
    var offsetCount: Int?
    var includes: [any IncludeSpec<R>] = []
    var withDeleted = false

    public init() {}

    /// Includes related rows (`JazzApp.tasks.query().include(JazzApp.tasks.assignee)`).
    public func include(_ includes: any IncludeSpec<R>...) -> Query<R> {
        var query = self
        query.includes += includes
        return query
    }

    /// Only these columns (and `id`): decode them with `jazz.all(_:as:)`.
    /// The values are as stored: a transformed column comes in its stored
    /// form, and computed fields aren't there.
    public func select(_ columns: any AnyColumn<R>...) -> Selection<R> {
        Selection(query: self, entries: columns.map { .string($0.name) })
    }


    public func `where`(_ conditions: Condition<R>...) -> Query<R> {
        var query = self
        query.conditions += conditions
        return query
    }

    /// Orders by `column` ascending; `orderBy(Tasks.rank.desc)` descending.
    public func orderBy<V>(_ column: Column<R, V>) -> Query<R> { orderBy(column.asc) }

    public func orderBy(_ orders: Order<R>...) -> Query<R> {
        var query = self
        query.orders += orders
        return query
    }

    public func limit(_ count: Int) -> Query<R> {
        var query = self
        query.limitCount = count
        return query
    }

    public func offset(_ count: Int) -> Query<R> {
        var query = self
        query.offsetCount = count
        return query
    }

    /// The query JSON.
    public func json() throws -> String {
        try Coding.json(JSONValue.object(object(withTable: true)))
    }

    func object(withTable: Bool) throws -> [String: JSONValue] {
        var object: [String: JSONValue] = [:]
        if withTable { object["table"] = .string(R.table) }
        if !conditions.isEmpty { object["where"] = .array(try conditions.map { try $0.json() }) }
        if !orders.isEmpty {
            object["orderBy"] = .array(
                orders.map {
                    .object(["column": .string($0.column), "direction": .string($0.direction.rawValue)])
                })
        }
        if let limitCount { object["limit"] = .number(Double(limitCount)) }
        if let offsetCount { object["offset"] = .number(Double(offsetCount)) }
        if !includes.isEmpty { object["include"] = .array(try includes.map { try $0.includeJSON() }) }
        if withDeleted { object["includeDeleted"] = .bool(true) }
        return object
    }

    /// Sorts rows (as JSON objects) by `orderBy`, then by id: deltas carry
    /// no positions, so subscriptions re-sort locally.
    func sort(_ rows: [[String: JSONValue]]) -> [[String: JSONValue]] {
        rows.sorted { a, b in
            for order in orders {
                if let ascending = JSONValue.ascending(a[order.column], b[order.column]) {
                    return order.direction == .asc ? ascending : !ascending
                }
            }
            return JSONValue.ascending(a["id"], b["id"]) ?? false
        }
    }
}
