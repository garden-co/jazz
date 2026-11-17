import Foundation

/// A `json` column's value: any JSON.
public enum JSONValue: Codable, Hashable, Sendable {
    case null
    case bool(Bool)
    case number(Double)
    case string(String)
    case array([JSONValue])
    case object([String: JSONValue])

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .bool(value)
        } else if let value = try? container.decode(Double.self) {
            self = .number(value)
        } else if let value = try? container.decode(String.self) {
            self = .string(value)
        } else if let value = try? container.decode([JSONValue].self) {
            self = .array(value)
        } else {
            self = .object(try container.decode([String: JSONValue].self))
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .null: try container.encodeNil()
        case .bool(let value): try container.encode(value)
        case .number(let value):
            if value.rounded() == value, abs(value) < 9.0e15 {
                try container.encode(Int64(value))
            } else {
                try container.encode(value)
            }
        case .string(let value): try container.encode(value)
        case .array(let value): try container.encode(value)
        case .object(let value): try container.encode(value)
        }
    }

    /// Orders two values of one column, as `orderBy` does: `null` first,
    /// then numbers, strings and booleans by value.
    static func ascending(_ a: JSONValue?, _ b: JSONValue?) -> Bool? {
        switch (a ?? .null, b ?? .null) {
        case (.null, .null): return nil
        case (.null, _): return true
        case (_, .null): return false
        case (.number(let x), .number(let y)): return x == y ? nil : x < y
        case (.string(let x), .string(let y)): return x == y ? nil : x < y
        case (.bool(let x), .bool(let y)): return x == y ? nil : !x
        default: return nil
        }
    }
}
