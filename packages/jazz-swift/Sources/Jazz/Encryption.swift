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

    public init(store: any PrivateStore, environment: String = "dev") {
        self.store = store
        self.environment = environment
    }
}

/// Answers jazz-ffi's store requests (JSON text: `op`, bytes as base64,
/// numbers that may exceed 2^53 as decimal text) with a `PrivateStore`.
///
/// Requests and answers carry key bytes. The `Data` buffers this bridge owns
/// (the request's UTF-8, the secret it decodes for `protect`, the secret
/// `unprotect` returns, the answer's JSON) are zeroed once used; a copy the
/// store keeps is its own (`Data` copies on write). Swift strings (the
/// request and answer texts, the base64 in them) and Foundation's parsed JSON
/// cannot be wiped and stay until their memory is reused. jazz-ffi wipes its
/// own copies.
final class StoreBridge: JazzFFI.PrivateStore, @unchecked Sendable {
    let store: any PrivateStore
    init(_ store: any PrivateStore) { self.store = store }

    func call(request: String) -> StoreReply {
        var data = Data(request.utf8)
        let parsed = try? JSONSerialization.jsonObject(with: data)
        wipe(&data)
        guard let object = parsed as? [String: Any], let op = object["op"] as? String
        else { return StoreReply(ok: false, payload: "the private store request is not JSON") }
        func text(_ name: String) throws -> String {
            guard let value = object[name] as? String else { throw StoreRequestError.missing(name) }
            return value
        }
        func number(_ name: String) throws -> UInt64 {
            guard let value = UInt64(try text(name)) else { throw StoreRequestError.missing(name) }
            return value
        }
        func bytes(_ name: String) throws -> Data {
            guard let value = Data(base64Encoded: try text(name)) else { throw StoreRequestError.missing(name) }
            return value
        }
        func json(_ value: Any) -> String {
            guard var data = try? JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed])
            else { return "null" }
            defer { wipe(&data) }
            return String(data: data, encoding: .utf8) ?? "null"
        }
        do {
            let result: String
            switch op {
            case "openScope":
                result = json(String(try store.openScope(try text("scope"))))
            case "load":
                if let found = try store.load(scope: try text("scope"), doc: try text("doc")) {
                    result = json([
                        "bytes": found.bytes.base64EncodedString(), "revision": String(found.revision),
                    ])
                } else {
                    result = "null"
                }
            case "save":
                var expected: UInt64?
                if object["expected"] is String { expected = try number("expected") }
                let revision = try store.save(
                    scope: try text("scope"), incarnation: try number("incarnation"), doc: try text("doc"),
                    bytes: try bytes("bytes"), expected: expected)
                result = json(String(revision))
            case "list":
                result = json(try store.list(scope: try text("scope")))
            case "delete":
                try store.delete(scope: try text("scope"))
                result = "null"
            case "protect":
                var secret = try bytes("secret")
                defer { wipe(&secret) }
                result = json(try store.protect(secret).base64EncodedString())
            case "unprotect":
                var secret = try store.unprotect(try bytes("blob"))
                defer { wipe(&secret) }
                result = json(secret.base64EncodedString())
            default:
                return StoreReply(ok: false, payload: "unknown private store operation \(op)")
            }
            return StoreReply(ok: true, payload: result)
        } catch PrivateStoreError.conflict {
            return StoreReply(ok: false, payload: "conflict")
        } catch PrivateStoreError.wiped {
            return StoreReply(ok: false, payload: "wiped")
        } catch {
            return StoreReply(ok: false, payload: "\(error)")
        }
    }
}

enum StoreRequestError: Error { case missing(String) }

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
        // A `store` or `database` failure arrives as `.io`, the rest as `.refused`.
        let message: String
        switch error as? JazzError {
        case .refused(let text), .io(let text): message = text
        default: return nil
        }
        guard message.hasPrefix("encryption (") else { return nil }
        return message.dropFirst("encryption (".count).prefix { $0 != ")" }.description
    }

    /// One of the runtime's operations by name (see the README), with its
    /// result as JSON.
    public func call(_ op: String, _ args: [String: JSONValue] = [:]) async throws -> JSONValue {
        try await call(op, args, as: JSONValue.self)
    }

    private func call<Result: Decodable>(
        _ op: String, _ args: [String: JSONValue] = [:], as type: Result.Type
    ) async throws -> Result {
        try await Client.typed {
            let json = try Coding.json(JSONValue.object(args))
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
        return try await Client.typed {
            let status = try await Client.run {
                try self.client.handle.encryptionReady(timeoutMs: UInt64(timeout * 1000))
            }
            return status
        }
    }

    /// What the runtime's loop is doing now.
    public func state() async throws -> EncryptionState {
        try await Client.typed {
            let text = try await Client.run { try self.client.handle.encryptionState() }
            return try Coding.decode(EncryptionState.self, text, self.client.userInfo)
        }
    }

    /// The reports waiting (`type`: `unreadable`, `missing-key`, `challenged`,
    /// `ready-failed`, ...).
    public func reports() async throws -> [JSONValue] {
        try await Client.typed {
            let texts = try await Client.run { try self.client.handle.encryptionReports() }
            return try texts.map { try Coding.decode(JSONValue.self, $0, self.client.userInfo) }
        }
    }

    /// This installation's device ID.
    public func deviceId() async throws -> String {
        guard case .string(let id) = try await call("deviceId") else { return "" }
        return id
    }

    /// The account's devices.
    public func devices() async throws -> [Device] {
        let list = try await call("devices")
        return try Coding.decode([Device].self, try Coding.json(list), client.userInfo)
    }

    /// Starts approving a waiting device (on an approved one); `approvalCode`
    /// then `approve` finish it once the user confirmed both devices show the
    /// same code (`answer` is the new device's).
    public func challenge(deviceId: String) async throws {
        _ = try await call("challenge", ["deviceId": .string(deviceId)])
    }

    /// On the new device, once challenged: the code to show.
    public func answer() async throws -> String {
        guard case .string(let code) = try await call("answer") else { return "" }
        return code
    }

    /// On the approving device: the code to compare, `nil` until the new
    /// device answered.
    public func approvalCode(deviceId: String) async throws -> String? {
        guard case .string(let code) = try await call("approvalCode", ["deviceId": .string(deviceId)])
        else { return nil }
        return code
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
        guard case .object(let made) = try await call("recoveryCreate"), case .string(let phrase)? = made["phrase"]
        else { return "" }
        return phrase
    }

    /// Registers the recovery root `phrase` gives, from a device that is active.
    public func recoveryRegister(phrase: String) async throws {
        _ = try await call("recoveryRegister", ["phrase": .string(phrase)])
    }

    /// Approves this new device with `phrase`: its status.
    public func recoveryRestore(phrase: String) async throws -> String {
        guard case .string(let status) = try await call("recoveryRestore", ["phrase": .string(phrase)])
        else { return "" }
        return status
    }
}

extension Client {
    /// The app's end-to-end encryption.
    public var encryption: Encryption { Encryption(client: self) }
}
