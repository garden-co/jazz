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
