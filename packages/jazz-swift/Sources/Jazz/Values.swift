import Foundation

/// A row id (a `uuid`), as its string.
public typealias ID = String

/// A `bigint` value: JSON carries it as a decimal string.
public struct BigInt: Codable, Hashable, Sendable, Comparable, ExpressibleByIntegerLiteral,
    CustomStringConvertible
{
    public var value: Int64

    public init(_ value: Int64) { self.value = value }
    public init(integerLiteral value: Int64) { self.value = value }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let text = try? container.decode(String.self), let value = Int64(text) {
            self.value = value
        } else {
            self.value = try container.decode(Int64.self)
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(String(value))
    }

    public static func < (a: BigInt, b: BigInt) -> Bool { a.value < b.value }
    public var description: String { String(value) }
}

/// A patch's change to one column: left as it is, or set (to `nil` for `null`).
public enum Change<Value: Encodable & Sendable>: Sendable {
    case unchanged
    case set(Value)

    /// The same change of a converted value (a transform's stored form).
    public func map<T>(_ transform: (Value) throws -> T) rethrows -> Change<T> {
        switch self {
        case .unchanged: return .unchanged
        case .set(let value): return .set(try transform(value))
        }
    }
}

/// Where the coders find the app's `JazzApp.Computed` implementation.
public let implementationsKey = CodingUserInfoKey(rawValue: "jazz.implementations")!

/// The app's computed fields and transforms, as generated bindings read
/// them while coding rows: what `JazzApp.open(_:computed:)` was given.
public func implementations<T>(of type: T.Type, in userInfo: [CodingUserInfoKey: Any]) throws -> T {
    guard let implementations = userInfo[implementationsKey] as? T else {
        throw JazzError.refused(
            "no \(T.self) to compute fields and transforms with: open the app with JazzApp.open(_:computed:)")
    }
    return implementations
}

extension KeyedEncodingContainer {
    /// Writes `change` under `key` when it sets the column; an `Optional`
    /// set to `nil` writes `null`.
    public mutating func encodeChange<Value>(_ change: Change<Value>, forKey key: Key) throws {
        guard case .set(let value) = change else { return }
        try encode(value, forKey: key)
    }
}

/// A row of a table, decoded from the row JSON jazz-ffi returns.
public protocol Row: Codable, Hashable, Sendable, Identifiable where ID == Jazz.ID {
    /// The table's authored name.
    static var table: String { get }
    var id: Jazz.ID { get }
}

/// A row of a table whose rows can change: upserts and update advice take
/// only these. The members table's rows never change, so its row type
/// isn't one (and it has no `Patch`).
public protocol Updatable: Row {}

/// A new row of a table.
public protocol Insert: Encodable, Sendable {
    associatedtype Row: Jazz.Row
}

/// A change to a row of a table.
public protocol Patch: Encodable, Sendable {
    associatedtype Row: Jazz.Row
}

/// The JSON coding the bindings and jazz-ffi agree on: timestamps as
/// milliseconds since the epoch, `bytea` as base64.
enum Coding {
    static func encoder(_ userInfo: [CodingUserInfoKey: any Sendable] = [:]) -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.userInfo = userInfo
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .custom { date, encoder in
            var container = encoder.singleValueContainer()
            try container.encode(Int64((date.timeIntervalSince1970 * 1000).rounded()))
        }
        encoder.dataEncodingStrategy = .base64
        return encoder
    }

    static func decoder(_ userInfo: [CodingUserInfoKey: any Sendable] = [:]) -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.userInfo = userInfo
        decoder.dateDecodingStrategy = .millisecondsSince1970
        decoder.dataDecodingStrategy = .base64
        return decoder
    }

    static func json<T: Encodable>(_ value: T, _ userInfo: [CodingUserInfoKey: any Sendable] = [:]) throws -> String {
        String(decoding: try encoder(userInfo).encode(value), as: UTF8.self)
    }

    static func value<T: Encodable>(_ value: T, _ userInfo: [CodingUserInfoKey: any Sendable] = [:]) throws -> JSONValue {
        // Through an array, so a top-level `nil` or scalar encodes everywhere.
        let data = try encoder(userInfo).encode([value])
        let array = try decoder().decode([JSONValue].self, from: data)
        return array[0]
    }

    static func decode<T: Decodable>(
        _ type: T.Type, _ json: String, _ userInfo: [CodingUserInfoKey: any Sendable] = [:]
    ) throws -> T {
        do {
            return try decoder(userInfo).decode(type, from: Data(json.utf8))
        } catch let error as JazzError {
            throw error
        } catch {
            throw JazzError.decoding("\(T.self): \(error)")
        }
    }
}
