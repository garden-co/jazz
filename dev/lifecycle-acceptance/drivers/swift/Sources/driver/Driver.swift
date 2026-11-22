// The Swift lifecycle driver: drivers/PROTOCOL.md on Jazz for Swift's public API.
import Foundation
import Jazz

struct Failure: Error {
    let kind: String
    let message: String
}

func bad(_ message: String) -> Failure { Failure(kind: "driver.bad", message: message) }

/// What a bearer provider gives, changed by `stoken`.
final class Token: @unchecked Sendable {
    private let lock = NSLock()
    private var value: String

    init(_ value: String) { self.value = value }
    func set(_ next: String) { lock.withLock { value = next } }
    func get() -> String? { lock.withLock { value.isEmpty ? nil : value } }
}

final class Shared: @unchecked Sendable {
    private let lock = NSLock()
    private var clients: [String: Jazz.Client] = [:]
    private var sessions: [String: (Jazz.JazzSession, Token)] = [:]
    private var running: [Int: _Concurrency.Task<Void, Never>] = [:]
    private var answered: Set<Int> = []
    private let output = NSLock()

    func client(_ h: String) throws -> Jazz.Client {
        guard let client = lock.withLock({ clients[h] }) else { throw bad("no handle") }
        return client
    }
    func put(_ h: String, _ client: Jazz.Client) { lock.withLock { clients[h] = client } }
    func session(_ h: String) throws -> (Jazz.JazzSession, Token) {
        guard let session = lock.withLock({ sessions[h] }) else { throw bad("no session") }
        return session
    }
    func put(_ h: String, _ session: Jazz.JazzSession, _ token: Token) {
        lock.withLock { sessions[h] = (session, token) }
    }
    func start(_ id: Int, _ task: _Concurrency.Task<Void, Never>) {
        lock.withLock { if !answered.contains(id) { running[id] = task } }
    }
    func finish(_ id: Int) {
        lock.withLock {
            answered.insert(id)
            running[id] = nil
        }
    }
    func cancel(_ id: Int) { lock.withLock { running[id] }?.cancel() }

    func answer(_ object: [String: Any]) {
        guard var data = try? JSONSerialization.data(withJSONObject: object) else { return }
        data.append(0x0A)
        output.withLock { FileHandle.standardOutput.write(data) }
    }
}

struct Request: Decodable, Sendable {
    var id: Int
    var op: String
    var h: String?
    var `as`: String?
    var dir: String?
    var url: String?
    var app: String?
    var key: String?
    var token: String?
    var start: String?
    var title: String?
    var owner: String?
    var tier: String?
    var target: Int?
    var ms: Int?
    var storage: String?
    var local: Bool?
}

func perform(_ shared: Shared, _ r: Request) async throws -> Any {
    let h = r.h ?? ""
    switch r.op {
    case "genkey":
        return try Jazz.generateDeviceKey()
    case "open":
        let auth: Jazz.Auth
        if let key = r.key {
            auth = .deviceKey(key)
        } else {
            let token = Token(r.token ?? "")
            auth = .bearer { _ in token.get() }
        }
        if r.local == true {
            shared.put(h, try await JazzApp.open(.local(appId: r.app, storage: .persistent, dataDir: r.dir)))
            return NSNull()
        }
        let config = Jazz.Config.client(
            appId: r.app ?? "", serverUrl: r.url ?? "", auth: auth,
            storage: r.storage == "memory" ? .memory : .persistent, dataDir: r.storage == "memory" ? nil : r.dir)
        shared.put(h, try await JazzApp.open(config))
    case "account":
        return String(describing: try await shared.client(h).account())
    case "write":
        let tier: Jazz.WriteTier? = r.tier == "global" ? .global : nil
        let new = JazzApp.NewNote(title: r.title ?? "", owner: r.owner ?? "")
        return try await shared.client(h).insert(new, tier: tier).id
    case "read":
        let tier: Jazz.ReadTier = r.tier == "remote" ? .remote : .local
        return try await shared.client(h).all(JazzApp.notes.query(), tier: tier).map(\.title).sorted()
    case "sync":
        try await shared.client(h).waitForSync()
    case "close":
        // Jazz for Swift's close() takes no timeout.
        if r.ms != nil { throw Failure(kind: "driver.unsupported", message: "close with a timeout") }
        try await shared.client(h).close()
    case "delete":
        try await shared.client(h).deleteClientStorage()
    case "disconnect":
        try await shared.client(h).disconnect()
    case "reconnect":
        try await shared.client(h).reconnect()
    case "signout":
        try await shared.client(h).signOut()
    case "sopen":
        let token = Token(r.token ?? "")
        let start = Jazz.JazzSession.Start(rawValue: r.start ?? "backend")
        let session = try await Jazz.JazzSession.open(
            app: JazzApp.app, appId: r.app ?? "", serverUrl: r.url ?? "", bearer: { _ in token.get() },
            start: start, storage: .persistent, dataDir: r.dir)
        shared.put(h, session, token)
    case "stoken":
        try shared.session(h).1.set(r.token ?? "")
    case "ssnap":
        let snapshot = try await shared.session(h).0.snapshot()
        var value: [String: Any] = ["status": snapshot.status.rawValue, "clientEpoch": snapshot.clientEpoch, "error": NSNull()]
        if let error = snapshot.error { value["error"] = ["kind": error.kind, "message": error.message] }
        return value
    case "sjazz":
        shared.put(r.as ?? "", try await shared.session(h).0.client())
    case "screate":
        try await shared.session(h).0.createDeviceAccount()
    case "sreopen":
        try await shared.session(h).0.reopenDeviceAccount()
    case "srefresh":
        try await shared.session(h).0.refresh()
    case "ssignout":
        try await shared.session(h).0.signOut()
    case "sclose":
        try await shared.session(h).0.close()
    case "cancel":
        shared.cancel(r.target ?? -1)
    default:
        throw Failure(kind: "driver.unsupported", message: r.op)
    }
    return NSNull()
}

/// A JazzError's case name, by reflection: a case added later needs no change here.
func describe(_ error: any Error) -> (String, String) {
    if let failure = error as? Failure { return (failure.kind, failure.message) }
    if error is CancellationError { return ("driver.cancelled", "CancellationError") }
    guard let error = error as? Jazz.JazzError else { return (String(describing: type(of: error)), "\(error)") }
    let payloadCase = Mirror(reflecting: error).children.first?.label
    let plainCase = error.description.split(separator: ":").first.map { String($0) } ?? ""
    return (payloadCase ?? plainCase, error.description)
}

@main
struct Driver {
    static func main() async {
        let shared = Shared()
        let lines = AsyncStream<String> { continuation in
            Thread.detachNewThread {
                while let line = readLine() { continuation.yield(line) }
                continuation.finish()
            }
        }
        shared.answer(["ready": true, "pid": Int(ProcessInfo.processInfo.processIdentifier)])
        for await line in lines {
            guard let request = try? JSONDecoder().decode(Request.self, from: Data(line.utf8)) else { continue }
            let task = _Concurrency.Task<Void, Never>.detached {
                let reply: [String: Any]
                do {
                    reply = ["id": request.id, "ok": true, "v": try await perform(shared, request)]
                } catch {
                    let (kind, message) = describe(error)
                    reply = ["id": request.id, "ok": false, "kind": kind, "msg": message]
                }
                shared.finish(request.id)
                shared.answer(reply)
            }
            shared.start(request.id, task)
        }
        // End of input: exit as a process does, closing nothing.
        exit(0)
    }
}
