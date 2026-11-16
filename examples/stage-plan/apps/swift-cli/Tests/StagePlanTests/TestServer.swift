import Foundation
import Jazz
import StagePlan
import XCTest


/// A `jazz-tools server --in-memory` of its own, for one test. The binary is
/// `$JAZZ_TOOLS_BIN`, else the repository's target/debug/jazz-tools.
final class TestServer {
    let process = Process()
    let server: Server

    init() async throws {
        let repo = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent().appendingPathComponent("../../../../../..").standardized
        let binary =
            ProcessInfo.processInfo.environment["JAZZ_TOOLS_BIN"]
            ?? repo.appendingPathComponent("target/debug/jazz-tools").path
        guard FileManager.default.isExecutableFile(atPath: binary) else {
            // On CI a missing server fails: a skip would pass silently.
            if ProcessInfo.processInfo.environment["CI"] != nil {
                throw TestServerError("no jazz-tools at \(binary)")
            }
            throw XCTSkip("no jazz-tools at \(binary): build it (cargo build -p jazz-cli --bin jazz-tools) or set JAZZ_TOOLS_BIN")
        }
        let port = Int.random(in: 20_000..<40_000)
        server = Server(
            url: "http://127.0.0.1:\(port)", appId: UUID().uuidString.lowercased(),
            backendSecret: "backend-\(UUID().uuidString)", adminSecret: "admin-\(UUID().uuidString)")
        process.executableURL = URL(fileURLWithPath: binary)
        process.arguments = [
            "server", server.appId, "--in-memory", "--port", String(port),
            "--backend-secret", server.backendSecret, "--admin-secret", server.adminSecret,
        ]
        // Its log goes to a file (a pipe nobody drains would stall it), and
        // it's up once that says "Listening on".
        let log = FileManager.default.temporaryDirectory
            .appendingPathComponent("jazz-server-\(port).log")
        FileManager.default.createFile(atPath: log.path, contents: nil)
        let handle = try FileHandle(forWritingTo: log)
        process.standardOutput = handle
        process.standardError = handle
        try process.run()
        for _ in 0..<200 {
            if let text = try? String(contentsOf: log, encoding: .utf8), text.contains("Listening on") {
                return
            }
            try await _Concurrency.Task.sleep(nanoseconds: 50_000_000)
        }
        kill(process.processIdentifier, SIGKILL)
        throw TestServerError("jazz-tools server didn't start at \(server.url)")
    }

    deinit {
        // SIGKILL: after terminate() (SIGTERM), Foundation's waitUntilExit()
        // was seen hanging here on Linux.
        kill(process.processIdentifier, SIGKILL)
        process.waitUntilExit()
    }
}


struct TestServerError: Error, CustomStringConvertible {
    let description: String
    init(_ description: String) { self.description = description }
}

/// The app's Jazz endpoint for end users, as a TS, Rust or Go backend's
/// `createJazz` handler serves it: a `jazz-app-test-relay` (`cargo build -p
/// jazz-app --features test-relay --bin jazz-app-test-relay`, or
/// `$JAZZ_TEST_RELAY_BIN`) in front of its own `TestServer`, publishing the
/// app's permissions (StagePlan's unless given). It authenticates
/// `Bearer token-<user>` as `<user>`.
final class TestRelay {
    let process = Process()
    let endpoint: String
    let server: TestServer
    let appId: String
    /// Device keys by test user name: the same name signs in as the same account.
    private var keys: [String: String] = [:]

    init(app: String? = nil, permissions: String? = nil) async throws {
        server = try await TestServer()
        appId = server.server.appId
        let here = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        let repo = here.appendingPathComponent("../../../../../..").standardized
        let example = here.appendingPathComponent("../..").standardized
        let binary =
            ProcessInfo.processInfo.environment["JAZZ_TEST_RELAY_BIN"]
            ?? repo.appendingPathComponent("target/debug/jazz-app-test-relay").path
        guard FileManager.default.isExecutableFile(atPath: binary) else {
            if ProcessInfo.processInfo.environment["CI"] != nil {
                throw TestServerError("no jazz-app-test-relay at \(binary)")
            }
            throw XCTSkip("no jazz-app-test-relay at \(binary): cargo build -p jazz-app --features test-relay --bin jazz-app-test-relay")
        }
        let work = FileManager.default.temporaryDirectory.appendingPathComponent("jazz-relay-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: work, withIntermediateDirectories: true)
        let appFile = work.appendingPathComponent("jazz.app.json")
        let permissionsFile = work.appendingPathComponent("jazz.permissions.json")
        if let app { try app.write(to: appFile, atomically: true, encoding: .utf8) }
        if let permissions { try permissions.write(to: permissionsFile, atomically: true, encoding: .utf8) }
        let out = work.appendingPathComponent("relay.out")
        FileManager.default.createFile(atPath: out.path, contents: nil)
        let handle = try FileHandle(forWritingTo: out)
        process.executableURL = URL(fileURLWithPath: binary)
        process.arguments = [
            "--core", server.server.url, "--app-id", appId,
            "--app", app == nil ? example.appendingPathComponent("jazz.app.json").path : appFile.path,
            "--permissions",
            permissions == nil ? example.appendingPathComponent("jazz.permissions.json").path : permissionsFile.path,
        ]
        var env = ProcessInfo.processInfo.environment
        env["JAZZ_BACKEND_SECRET"] = server.server.backendSecret
        env["JAZZ_ADMIN_SECRET"] = server.server.adminSecret
        process.environment = env
        process.standardOutput = handle
        process.standardError = FileHandle.nullDevice
        try process.run()
        for _ in 0..<300 {
            if let text = try? String(contentsOf: out, encoding: .utf8),
                let line = text.split(separator: "\n").first(where: { $0.hasPrefix("http") })
            {
                endpoint = String(line)
                return
            }
            try await _Concurrency.Task.sleep(nanoseconds: 50_000_000)
        }
        kill(process.processIdentifier, SIGKILL)
        throw TestServerError("jazz-app-test-relay printed no endpoint")
    }

    /// A device-account end user of StagePlan, signed in: `name` picks the
    /// account (the same name, the same account).
    func user(_ name: String = "user") async throws -> StagePlanApp {
        let key = try keys[name] ?? Jazz.generateDeviceKey()
        keys[name] = key
        return try await StagePlanApp.open(endpoint: endpoint, appId: appId, auth: .deviceKey(key))
    }

    /// An end user of this relay's app, as a plain client.
    func client(auth: Jazz.Auth) async throws -> Jazz.Client {
        try await Jazz.Client.open(
            app: try String(contentsOf: appPath, encoding: .utf8),
            config: .client(appId: appId, serverUrl: endpoint, auth: auth, storage: .memory))
    }

    private var appPath: URL {
        URL(fileURLWithPath: process.arguments![process.arguments!.firstIndex(of: "--app")! + 1])
    }

    deinit {
        kill(process.processIdentifier, SIGKILL)
        process.waitUntilExit()
    }
}

/// A sync server's address and secrets (the relay uses them; Swift itself
/// never holds backend secrets).
struct Server: Sendable {
    var url: String
    var appId: String
    var backendSecret: String
    var adminSecret: String
}
