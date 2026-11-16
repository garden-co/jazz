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
        // Port 0: the OS picks a free port, so parallel tests (and anything
        // else on a loaded CI runner) never collide on one. The server writes
        // the port it bound to `--bound-port-file`, newline-terminated once
        // complete.
        let work = FileManager.default.temporaryDirectory
            .appendingPathComponent("jazz-server-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: work, withIntermediateDirectories: true)
        let portFile = work.appendingPathComponent("port")
        let appId = UUID().uuidString.lowercased()
        let backendSecret = "backend-\(UUID().uuidString)"
        let adminSecret = "admin-\(UUID().uuidString)"
        process.executableURL = URL(fileURLWithPath: binary)
        process.arguments = [
            "server", appId, "--in-memory", "--port", "0", "--bound-port-file", portFile.path,
            "--backend-secret", backendSecret, "--admin-secret", adminSecret,
        ]
        // Its log goes to a file (a pipe nobody drains would stall it); a
        // failed start shows it.
        let log = work.appendingPathComponent("server.log")
        FileManager.default.createFile(atPath: log.path, contents: nil)
        let handle = try FileHandle(forWritingTo: log)
        process.standardOutput = handle
        process.standardError = handle
        try process.run()
        let port = try await waitFor(
            "jazz-tools server", process: process, log: log, timeout: startupTimeout
        ) { () -> Int? in
            guard let text = try? String(contentsOf: portFile, encoding: .utf8), text.hasSuffix("\n") else {
                return nil
            }
            return Int(text.trimmingCharacters(in: .whitespacesAndNewlines))
        }
        server = Server(
            url: "http://127.0.0.1:\(port)", appId: appId, backendSecret: backendSecret,
            adminSecret: adminSecret)
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

/// How long a test server or relay may take to start: generous, as CI
/// runners under load start processes slowly. `JAZZ_TEST_STARTUP_SECONDS`
/// overrides it.
let startupTimeout: TimeInterval =
    ProcessInfo.processInfo.environment["JAZZ_TEST_STARTUP_SECONDS"].flatMap(TimeInterval.init) ?? 60

/// Polls `ready` until it gives a value. Fails at once, with the process's
/// log, if the process exits first, and with the log after `timeout`.
func waitFor<T>(
    _ what: String, process: Process, log: URL, timeout: TimeInterval, ready: () throws -> T?
) async throws -> T {
    let start = Date()
    while true {
        if let value = try ready() { return value }
        if !process.isRunning {
            throw TestServerError(
                "\(what) exited (status \(process.terminationStatus)) before it was ready. Its log:\n\(tail(log))")
        }
        if Date().timeIntervalSince(start) > timeout {
            kill(process.processIdentifier, SIGKILL)
            throw TestServerError(
                "\(what) wasn't ready after \(Int(timeout)) s (JAZZ_TEST_STARTUP_SECONDS). Its log:\n\(tail(log))")
        }
        try await _Concurrency.Task.sleep(nanoseconds: 50_000_000)
    }
}

/// The last lines of `log`, for a failure message.
func tail(_ log: URL, lines: Int = 40) -> String {
    guard let text = try? String(contentsOf: log, encoding: .utf8), !text.isEmpty else { return "(empty)" }
    return text.split(separator: "\n", omittingEmptySubsequences: false).suffix(lines).joined(separator: "\n")
}

/// The app's Jazz endpoint for end users, as the TypeScript backend's
/// `createJazz` handler serves it: a `jazz-app-test-relay` (`cargo build -p
/// jazz-app-support --features test-relay --bin jazz-app-test-relay`, or
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
            throw XCTSkip("no jazz-app-test-relay at \(binary): cargo build -p jazz-app-support --features test-relay --bin jazz-app-test-relay")
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
        let log = work.appendingPathComponent("relay.log")
        FileManager.default.createFile(atPath: log.path, contents: nil)
        let logHandle = try FileHandle(forWritingTo: log)
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
        process.standardError = logHandle
        try process.run()
        endpoint = try await waitFor(
            "jazz-app-test-relay", process: process, log: log, timeout: startupTimeout
        ) { () -> String? in
            guard let text = try? String(contentsOf: out, encoding: .utf8),
                let line = text.split(separator: "\n").first(where: { $0.hasPrefix("http") })
            else { return nil }
            return String(line)
        }
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
