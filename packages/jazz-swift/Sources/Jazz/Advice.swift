import Foundation

/// Permission advice: whether the app's permissions would allow a write or
/// read, before trying it. `nil` means unknown (no server to ask, say).
extension Client {
    public func canInsert<I: Insert>(_ row: I) async throws -> Bool? {
        try await can(I.Row.table, "insert") { ["row": try Coding.value(row, userInfo)] }
    }

    public func canRead<R: Row>(_ type: R.Type, id: Jazz.ID) async throws -> Bool? {
        try await can(R.table, "read") { ["id": .string(id)] }
    }

    /// Whether `patch` (or any update, without one) of row `id` is allowed.
    public func canUpdate<P: Patch>(_ id: Jazz.ID, _ patch: P) async throws -> Bool? {
        try await can(P.Row.table, "update") { ["id": .string(id), "row": try Coding.value(patch, userInfo)] }
    }

    public func canUpdate<R: Updatable>(_ type: R.Type, id: Jazz.ID) async throws -> Bool? {
        try await can(R.table, "update") { ["id": .string(id)] }
    }

    public func canDelete<R: Row>(_ type: R.Type, id: Jazz.ID) async throws -> Bool? {
        try await can(R.table, "delete") { ["id": .string(id)] }
    }

    func can(_ table: String, _ op: String, _ args: () throws -> [String: JSONValue])
        async throws -> Bool?
    {
        try await Self.typed {
            let json = try Coding.json(JSONValue.object(try args()))
            return try await Self.run { try self.handle.can(table: table, op: op, argsJson: json) }
        }
    }
}
