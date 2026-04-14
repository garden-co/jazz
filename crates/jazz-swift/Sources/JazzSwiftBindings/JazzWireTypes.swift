import Foundation

public enum JazzJSONValue: Decodable, Equatable, Sendable {
    case string(String)
    case number(Double)
    case bool(Bool)
    case array([JazzJSONValue])
    case object([String: JazzJSONValue])
    case null

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()

        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .bool(value)
        } else if let value = try? container.decode(Double.self) {
            self = .number(value)
        } else if let value = try? container.decode(String.self) {
            self = .string(value)
        } else if let value = try? container.decode([String: JazzJSONValue].self) {
            self = .object(value)
        } else if let value = try? container.decode([JazzJSONValue].self) {
            self = .array(value)
        } else {
            throw DecodingError.dataCorruptedError(
                in: container,
                debugDescription: "Unsupported JSON value."
            )
        }
    }

    public var renderedDescription: String {
        switch self {
        case let .string(value):
            return value
        case let .number(value):
            if value.rounded() == value {
                return String(Int(value))
            }
            return String(value)
        case let .bool(value):
            return String(value)
        case let .array(values):
            return "[" + values.map(\.renderedDescription).joined(separator: ", ") + "]"
        case let .object(values):
            let body = values
                .sorted(by: { $0.key < $1.key })
                .map { "\($0.key): \($0.value.renderedDescription)" }
                .joined(separator: ", ")
            return "{" + body + "}"
        case .null:
            return "null"
        }
    }

    public var stringValue: String? {
        guard case let .string(value) = self else { return nil }
        return value
    }

    public var boolValue: Bool? {
        guard case let .bool(value) = self else { return nil }
        return value
    }

    public var doubleValue: Double? {
        guard case let .number(value) = self else { return nil }
        return value
    }

    public var intValue: Int? {
        guard case let .number(value) = self, value.rounded() == value else { return nil }
        return Int(value)
    }

    public var uint64Value: UInt64? {
        guard let intValue, intValue >= 0 else { return nil }
        return UInt64(intValue)
    }

    public var arrayValue: [JazzJSONValue]? {
        guard case let .array(value) = self else { return nil }
        return value
    }

    public var objectValue: [String: JazzJSONValue]? {
        guard case let .object(value) = self else { return nil }
        return value
    }
}

public struct JazzWireValue: Decodable, Equatable, Sendable {
    public let type: String
    public let value: JazzJSONValue?

    public var renderedDescription: String {
        value?.renderedDescription ?? "null"
    }

    public var stringValue: String? {
        value?.stringValue
    }

    public var boolValue: Bool? {
        value?.boolValue
    }

    public var doubleValue: Double? {
        value?.doubleValue
    }

    public var intValue: Int? {
        value?.intValue
    }

    public var uint64Value: UInt64? {
        value?.uint64Value
    }
}

public struct JazzWireRow: Decodable, Equatable, Sendable {
    public let id: String
    public let values: [JazzWireValue]
}

public enum JazzWireRowColumnError: Error, Equatable, Sendable {
    case columnCountMismatch(expected: Int, actual: Int)
    case duplicateColumnName(String)
}

public struct JazzNamedRow: Equatable, Sendable {
    public let id: String
    public let values: [String: JazzWireValue]
}

public extension JazzWireRow {
    func named(columns: [String]) throws -> JazzNamedRow {
        guard columns.count == values.count else {
            throw JazzWireRowColumnError.columnCountMismatch(
                expected: columns.count,
                actual: values.count
            )
        }

        var mapped: [String: JazzWireValue] = [:]
        mapped.reserveCapacity(columns.count)

        for (column, value) in zip(columns, values) {
            if mapped.updateValue(value, forKey: column) != nil {
                throw JazzWireRowColumnError.duplicateColumnName(column)
            }
        }

        return JazzNamedRow(id: id, values: mapped)
    }
}

public struct JazzSubscriptionChange: Decodable, Equatable, Sendable {
    public let kind: Int
    public let id: String
    public let index: Int
    public let row: JazzWireRow?

    public var kindDescription: String {
        switch kind {
        case 0:
            return "added"
        case 1:
            return "removed"
        case 2:
            return "updated"
        default:
            return "unknown"
        }
    }
}

public struct JazzSyncOutboxMessage: Equatable, Sendable {
    public let destinationKind: String
    public let destinationId: String
    public let payloadJSON: String
    public let isCatalogue: Bool
}
