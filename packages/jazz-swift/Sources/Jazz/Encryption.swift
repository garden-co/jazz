import Foundation
import JazzFFI

// End-to-end encryption for an app that declares it. A device keeps its keys
// in a private store the app supplies (`EncryptionConfig.store`): Jazz ships
// none. The Keychain is the natural place for the secret that protects it.

/// One document of a private store, with its revision.
public struct StoredDocument: Sendable {
    public var bytes: Data
    public var revision: UInt64
    public init(bytes: Data, revision: UInt64) {
        self.bytes = bytes
        self.revision = revision
    }
}

/// What a private store's `save` throws when nothing was written.
public enum PrivateStoreError: Error, Sendable {
    /// The document's revision is not the one expected.
    case conflict
    /// The scope is not of the incarnation given: it was deleted since.
    case wiped
}

/// Where a device keeps its encryption keys. It is called off the main
/// thread, one request at a time, so answer quickly. Keep the documents in
/// files you protect with a key the Keychain holds (`protect`, `unprotect`).
public protocol PrivateStore: Sendable {
    /// The incarnation of `scope`, creating the scope with a new random,
    /// non-zero incarnation if there is none.
    func openScope(_ scope: String) throws -> UInt64
    /// The document, or `nil` if there is none.
    func load(scope: String, doc: String) throws -> StoredDocument?
    /// Replaces the document if the scope is still of `incarnation` and the
    /// document's revision is still `expected` (`nil`: it must not exist),
    /// atomically, and returns the new revision. Otherwise throws
    /// `PrivateStoreError.wiped` or `.conflict` and writes nothing.
    func save(scope: String, incarnation: UInt64, doc: String, bytes: Data, expected: UInt64?) throws -> UInt64
    /// The names of the scope's documents.
    func list(scope: String) throws -> [String]
    /// Removes the scope and every document in it: its incarnation first,
    /// then its documents.
    func delete(scope: String) throws
    /// Protects a secret at rest (encrypts it with a key the platform keeps);
    /// `unprotect` recovers it.
    func protect(_ secret: Data) throws -> Data
    func unprotect(_ blob: Data) throws -> Data
}

/// What an app that declares end-to-end encryption opens with.
public struct EncryptionConfig: Sendable {
    /// This device's private store.
    public var store: any PrivateStore
    /// The deployment's environment: the same for every client of it.
    public var environment: String
    /// Add the signed-in creator to each new space.
    public var addCreatorAsMember: Bool

    public init(store: any PrivateStore, environment: String = "dev", addCreatorAsMember: Bool = true) {
        self.store = store
        self.environment = environment
        self.addCreatorAsMember = addCreatorAsMember
    }
}

/// Adapts the app's store to UniFFI. Hosts wipe secrets they own after use;
/// lowering and lifting a callback can make temporary UniFFI copies.
final class StoreBridge: JazzFFI.PrivateStore, @unchecked Sendable {
    let store: any PrivateStore
    init(_ store: any PrivateStore) { self.store = store }

    func openScope(scope: String) throws -> UInt64 { try call { try store.openScope(scope) } }

    func load(scope: String, doc: String) throws -> JazzFFI.StoredDocument? {
        try call {
            try store.load(scope: scope, doc: doc).map {
                JazzFFI.StoredDocument(bytes: $0.bytes, revision: $0.revision)
            }
        }
    }

    func save(scope: String, incarnation: UInt64, doc: String, bytes: Data, expected: UInt64?) throws -> UInt64 {
        try call { try store.save(scope: scope, incarnation: incarnation, doc: doc, bytes: bytes, expected: expected) }
    }

    func list(scope: String) throws -> [String] { try call { try store.list(scope: scope) } }
    func delete(scope: String) throws { try call { try store.delete(scope: scope) } }

    func protect(secret: Data) throws -> Data {
        var input = secret.withUnsafeBytes { Data($0) }
        defer { wipe(&input) }
        return try call { try store.protect(input) }
    }

    func unprotect(blob: Data) throws -> Data {
        var secret = try call { try store.unprotect(blob) }
        defer { wipe(&secret) }
        return secret.withUnsafeBytes { Data($0) }
    }

    private func call<T>(_ block: () throws -> T) throws -> T {
        do { return try block() }
        catch PrivateStoreError.conflict { throw JazzFFI.PrivateStoreError.Conflict }
        catch PrivateStoreError.wiped { throw JazzFFI.PrivateStoreError.Wiped }
        catch { throw JazzFFI.PrivateStoreError.Failed(reason: "\(error)") }
    }
}

/// Zeroes `data`'s bytes in place (a copy shared with someone else is copied
/// first and left as it was).
func wipe(_ data: inout Data) {
    data.resetBytes(in: data.startIndex..<data.endIndex)
}

/// What the runtime's loop is doing.
public struct EncryptionState: Decodable, Sendable {
    /// `active`, `awaiting-approval` or `reenrolled`; `nil` before the first
    /// verification.
    public var status: String?
    public var error: String?
    public var stage: String?
}

/// A device of the account: active, or waiting for approval.
public struct Device: Decodable, Hashable, Sendable {
    public var id: String
    public var status: String
    /// Whether this device has published a request to be removed.
    public var removalRequested: Bool
}

/// The deliveries written and the spaces whose delivery failed in one pass.
public struct KeyDeliveryResult: Decodable, Sendable {
    public let delivered: UInt64
    public let failed: UInt64
}

/// The app's end-to-end encryption: this device's account, its other devices,
/// its recovery and its keys. Every call refuses for an app that declares
/// none (`JazzError.refused`, code `not-declared`).
public struct Encryption: Sendable {
    let client: Client

    /// The code of an encryption failure (`approval-code-mismatch`,
    /// `no-challenge`, `invalid-checksum`, ...), or `nil` for any other error.
    public static func code(of error: any Error) -> String? {
        switch error as? JazzError {
        case .refused(_, let code), .io(_, let code): return code
        default: return nil
        }
    }

    /// One of the runtime's operations by name (see the README), with its
    /// result as JSON.
    public func call(_ op: String, _ args: [String: JSONValue] = [:]) async throws -> JSONValue {
        try await call(op, args, as: JSONValue.self)
    }

    private func call<Result: Decodable>(
        _ op: String, _ args: [String: JSONValue] = [:], as type: Result.Type
    ) async throws -> Result {
        try await call(op, arguments: JSONValue.object(args), as: type)
    }

    private func call<Result: Decodable, Arguments: Encodable>(
        _ op: String, arguments: Arguments, as type: Result.Type
    ) async throws -> Result {
        try await Client.typed {
            let json = try Coding.json(arguments)
            let text = try await Client.run { try self.client.handle.encryptionCall(op: op, argsJson: json) }
            return try Coding.decode(type, text, self.client.userInfo)
        }
    }

    /// Verifies the account and enrols or recovers this device, waiting up to
    /// `timeout` seconds: `active`, `awaiting-approval` or `reenrolled`.
    public func ready(timeout: TimeInterval = 30) async throws -> String {
        // A negative, NaN or too large timeout would trap in the conversion.
        guard timeout >= 0, timeout * 1000 < Double(UInt64.max) else {
            throw JazzError.refused("the readiness timeout must be a non-negative number of seconds")
        }
        return try await call("waitUntilReady", arguments: ["timeoutMs": UInt64(timeout * 1000)], as: String.self)
    }

    /// What the runtime's loop is doing now.
    public func state() async throws -> EncryptionState {
        try await call("state", as: EncryptionState.self)
    }

    /// The reports waiting (`type`: `unreadable`, `missing-key`, `challenged`,
    /// `ready-failed`, ...).
    public func reports() async throws -> [JSONValue] {
        try await call("reports", as: [JSONValue].self)
    }

    /// This installation's device ID.
    public func deviceId() async throws -> String {
        try await call("deviceId", as: String.self)
    }

    /// The account's devices.
    public func devices() async throws -> [Device] {
        try await call("devices", as: [Device].self)
    }

    /// Starts approving a waiting device (on an approved one); `approvalCode`
    /// then `approve` finish it once the user confirmed both devices show the
    /// same code (`answer` is the new device's).
    public func challenge(deviceId: String) async throws {
        _ = try await call("challenge", ["deviceId": .string(deviceId)])
    }

    /// On the new device, once challenged: the code to show.
    public func answer() async throws -> String {
        try await call("answer", as: String.self)
    }

    /// On the approving device: the code to compare, `nil` until the new
    /// device answered.
    public func approvalCode(deviceId: String) async throws -> String? {
        try await call("approvalCode", ["deviceId": .string(deviceId)], as: String?.self)
    }

    /// Approves the device if `code` is the one `approvalCode` gave.
    public func approve(deviceId: String, code: String) async throws {
        _ = try await call("approve", ["deviceId": .string(deviceId), "code": .string(code)])
    }

    /// Removes a device and rotates the account.
    public func removeDevice(_ deviceId: String) async throws {
        _ = try await call("remove", ["deviceId": .string(deviceId)])
    }

    /// Runs a delivery pass for new members, newly approved devices and
    /// rotated spaces. Check the result's `failed` count for failures within the pass.
    @discardableResult
    public func keysDeliver() async throws -> KeyDeliveryResult {
        try await call("keysDeliver", as: KeyDeliveryResult.self)
    }

    /// Rotates the key of the space row `id` of `table`.
    public func keysRotate(table: String, id: String) async throws {
        _ = try await call("keysRotate", ["table": .string(table), "id": .string(id)])
    }

    /// A new recovery phrase (24 words) to keep. It registers nothing:
    /// `recoveryRegister` does. Only for an account the app signs in itself (a
    /// device account's recovery is its account key).
    public func recoveryCreate() async throws -> String {
        struct CreatedRecovery: Decodable { let phrase: String }
        return try await call("recoveryCreate", as: CreatedRecovery.self).phrase
    }

    /// Registers the recovery root `phrase` gives, from a device that is active.
    public func recoveryRegister(phrase: String) async throws {
        _ = try await call("recoveryRegister", ["phrase": .string(phrase)])
    }

    /// Approves this new device with `phrase`: its status.
    public func recoveryRestore(phrase: String) async throws -> String {
        try await call("recoveryRestore", ["phrase": .string(phrase)], as: String?.self) ?? ""
    }
}

extension Client {
    /// The app's end-to-end encryption.
    public var encryption: Encryption { Encryption(client: self) }
}
