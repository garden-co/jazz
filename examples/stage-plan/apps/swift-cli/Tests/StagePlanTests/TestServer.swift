import Foundation
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

/// The app's Jazz endpoint for end users, as `createJazz` serves it: a
/// `jazz-app-test-relay` (`cargo build -p jazz-app --features test-relay
/// --bin jazz-app-test-relay`, or `$JAZZ_TEST_RELAY_BIN`) in front of a
/// `TestServer`, with a backend having published StagePlan's permissions.
/// It authenticates `Bearer token-<user>` as `<user>`.
final class TestRelay {
    let process = Process()
    let endpoint: String
    let backend: StagePlanApp

    init(_ server: TestServer) async throws {
        let repo = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent().appendingPathComponent("../../../../../..").standardized
        let binary =
            ProcessInfo.processInfo.environment["JAZZ_TEST_RELAY_BIN"]
            ?? repo.appendingPathComponent("target/debug/jazz-app-test-relay").path
        guard FileManager.default.isExecutableFile(atPath: binary) else {
            if ProcessInfo.processInfo.environment["CI"] != nil {
                throw TestServerError("no jazz-app-test-relay at \(binary)")
            }
            throw XCTSkip("no jazz-app-test-relay at \(binary): cargo build -p jazz-app --features test-relay --bin jazz-app-test-relay")
        }
        backend = try await StagePlanApp.open(user: "00000000-0000-4000-8000-0000000000ee", server: server.server)
        guard let bundle = try await backend.jazz.bundle() else { throw TestServerError("the backend published no bundle") }
        let out = FileManager.default.temporaryDirectory.appendingPathComponent("jazz-relay-\(UUID().uuidString).out")
        FileManager.default.createFile(atPath: out.path, contents: nil)
        let handle = try FileHandle(forWritingTo: out)
        process.executableURL = URL(fileURLWithPath: binary)
        process.arguments = ["--core", server.server.url, "--app-id", server.server.appId, "--bundle", bundle]
        var env = ProcessInfo.processInfo.environment
        env["JAZZ_BACKEND_SECRET"] = server.server.backendSecret
        env["JAZZ_ADMIN_SECRET"] = server.server.adminSecret
        process.environment = env
        process.standardOutput = handle
        process.standardError = FileHandle.nullDevice
        try process.run()
        for _ in 0..<200 {
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

    deinit {
        kill(process.processIdentifier, SIGKILL)
        process.waitUntilExit()
    }
}
