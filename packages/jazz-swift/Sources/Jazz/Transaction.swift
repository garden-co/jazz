import Foundation
import JazzFFI

/// Where a read is served from: the local store, local first (the
/// default), or the server.
public enum ReadTier: String, Sendable { case local, localFirst, remote }

/// The durability a write waits for: stored locally, or accepted globally
/// (by the server, which also checks permissions).
public enum WriteTier: String, Sendable { case local, global }

/// An open transaction: `Client.transaction`'s `tx`. Use it from the
/// transaction's body only, one call at a time.
public final class Transaction: @unchecked Sendable {
    let ffi: JazzFFI.Transaction
    let implementations: (any Sendable)?
    var userInfo: [CodingUserInfoKey: any Sendable] { Client.userInfo(implementations) }

    init(_ ffi: JazzFFI.Transaction, implementations: (any Sendable)?) {
        self.ffi = ffi
        self.implementations = implementations
    }

    /// Every row `query` matches, with the transaction's own writes.
    public func all<R>(_ query: Query<R>) async throws(JazzError) -> [R] {
        try await Client.typed {
            let json = try query.json()
            let rows = try await Client.run { try self.ffi.all(queryJson: json) }
            return try Coding.decode([R].self, rows, userInfo)
        }
    }

    public func one<R>(_ query: Query<R>) async throws(JazzError) -> R? {
        try await Client.typed {
            let json = try query.json()
            guard let row = try await Client.run({ try self.ffi.one(queryJson: json) }) else {
                return nil
            }
            return try Coding.decode(R.self, row, userInfo)
        }
    }

    @discardableResult
    public func insert<I: Insert>(_ row: I) async throws(JazzError) -> I.Row {
        try await Client.typed {
            let json = try Coding.json(row, userInfo)
            let inserted = try await Client.run { try self.ffi.insert(table: I.Row.table, rowJson: json) }
            return try Coding.decode(I.Row.self, inserted, userInfo)
        }
    }

    public func update<P: Patch>(_ id: Jazz.ID, _ patch: P) async throws(JazzError) {
        try await Client.typed {
            let json = try Coding.json(patch, userInfo)
            try await Client.run { try self.ffi.update(table: P.Row.table, id: id, patchJson: json) }
        }
    }

    public func update<P: Patch>(_ row: P.Row, _ patch: P) async throws(JazzError) {
        try await update(row.id, patch)
    }

    public func delete<R: Row>(_ type: R.Type, id: Jazz.ID) async throws(JazzError) {
        try await Client.typed {
            try await Client.run { try self.ffi.delete(table: R.table, id: id) }
        }
    }

    public func delete<R: Row>(_ row: R) async throws(JazzError) {
        try await delete(R.self, id: row.id)
    }
}
