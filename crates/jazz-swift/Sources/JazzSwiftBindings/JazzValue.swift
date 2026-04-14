import Foundation

public enum JazzValue: Encodable, Equatable, Sendable {
    case integer(Int32)
    case bigInt(Int64)
    case double(Double)
    case boolean(Bool)
    case text(String)
    case timestamp(UInt64)
    case uuid(String)
    case bytea([UInt8])
    case array([JazzValue])
    case row(id: String?, values: [JazzValue])
    case null

    private enum CodingKeys: String, CodingKey {
        case type
        case value
        case id
        case values
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)

        switch self {
        case let .integer(value):
            try container.encode("Integer", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .bigInt(value):
            try container.encode("BigInt", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .double(value):
            try container.encode("Double", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .boolean(value):
            try container.encode("Boolean", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .text(value):
            try container.encode("Text", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .timestamp(value):
            try container.encode("Timestamp", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .uuid(value):
            try container.encode("Uuid", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .bytea(value):
            try container.encode("Bytea", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .array(value):
            try container.encode("Array", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .row(id, values):
            try container.encode("Row", forKey: .type)
            var nested = container.nestedContainer(keyedBy: CodingKeys.self, forKey: .value)
            try nested.encodeIfPresent(id, forKey: .id)
            try nested.encode(values, forKey: .values)
        case .null:
            try container.encode("Null", forKey: .type)
        }
    }
}
