import Foundation
import Jazz
import StagePlan

// stage-plan-swift [--key-file <path>] [--bearer-stdin] [--data-dir <dir>] [--show <name> | --show-id <id>] <command>
// An end user of StagePlan: it signs in to the app's Jazz endpoint
// (JAZZ_ENDPOINT, the backend's handler; JAZZ_APP_ID) with a device account,
// whose key lives in --key-file (made on first use, mode 0600; default
// ~/.config/stage-plan-swift/device-key), or with the app's bearer token,
// from $JAZZ_BEARER or, with --bearer-stdin, a line on stdin (never argv,
// which `ps` and shell history show).
// --show picks the user's own show by name (made if they have none);
// --show-id any show by id (list shows only what they may read; add into a
// show they may not write into exits 1 with "permission denied").
//   list                    the show's tasks, by rank
//   add <title>             add a task
//   move <task-id> <stage>  todo | doing | done | blocked
//   demo                    create a show, add tasks, move one, list them
var args = Array(CommandLine.arguments.dropFirst())
@MainActor func option(_ name: String) -> String? {
    guard let at = args.firstIndex(of: name), at + 1 < args.count else { return nil }
    defer { args.removeSubrange(at...at + 1) }
    return args[at + 1]
}
let keyFile = option("--key-file")
    ?? FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent(".config/stage-plan-swift/device-key").path
let bearerFromStdin = args.contains("--bearer-stdin")
args.removeAll { $0 == "--bearer-stdin" }
let bearer: String? =
    bearerFromStdin
    ? readLine(strippingNewline: true)
    : ProcessInfo.processInfo.environment["JAZZ_BEARER"]
let dataDir = option("--data-dir")
let showName = option("--show") ?? "Opening night"
let showId = option("--show-id")

func printTasks(_ tasks: [JazzApp.Task]) {
    for task in tasks {
        print("\(task.id)  \(task.status.rawValue.padding(toLength: 8, withPad: " ", startingAt: 0))\(task.title)")
    }
}

do {
    let env = ProcessInfo.processInfo.environment
    guard let endpoint = env["JAZZ_ENDPOINT"], let appId = env["JAZZ_APP_ID"] else {
        throw JazzError.refused("set JAZZ_ENDPOINT (the app's Jazz endpoint) and JAZZ_APP_ID")
    }
    let auth: Jazz.Auth
    if let bearer {
        auth = .bearer { _ in bearer }
    } else {
        auth = .deviceKey(try deviceKey(at: keyFile))
    }
    let app = try await StagePlanApp.open(endpoint: endpoint, appId: appId, auth: auth, dataDir: dataDir)
    let show: Jazz.ID
    if let showId {
        show = showId
    } else {
        show = try await app.show(named: showName).id
    }
    switch args.first {
    case "list"?:
        printTasks(try await app.tasks(ofShow: show))
    case "add"? where args.count == 2:
        let task = try await app.addTask(args[1], toShow: show)
        print(task.id)
    case "move"? where args.count == 3:
        guard let status = JazzApp.TaskStatus(rawValue: args[2]) else {
            throw JazzError.refused("stage must be one of todo, doing, done, blocked")
        }
        try await app.move(args[1], to: status)
    case "demo"?:
        let load = try await app.addTask("Load in", toShow: show)
        try await app.addTask("Sound check", toShow: show)
        try await app.move(load.id, to: .doing)
        printTasks(try await app.tasks(ofShow: show))
    default:
        print("usage: stage-plan-swift [--key-file <path>] [--bearer-stdin] [--data-dir <dir>] [--show <name> | --show-id <id>] list | add <title> | move <task-id> <stage> | demo")
        exit(2)
    }
    try await app.jazz.close()
} catch {
    FileHandle.standardError.write(Data("error: \(error)\n".utf8))
    exit(1)
}

/// The device account's key at `path`, made if there's none. The key is the
/// account: only a missing file makes a new one (any other read error is
/// thrown, never overwritten), and a new file is created exclusively with
/// mode 0600 from the start.
func deviceKey(at path: String) throws -> String {
    do {
        let key = try String(contentsOfFile: path, encoding: .utf8)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard !key.isEmpty else { throw JazzError.refused("\(path) is empty: restore the device key or delete the file") }
        return key
    } catch let error as CocoaError where error.code == .fileReadNoSuchFile {
        // No key yet: make one below.
    }
    let key = try Jazz.generateDeviceKey()
    let url = URL(fileURLWithPath: path)
    try FileManager.default.createDirectory(
        at: url.deletingLastPathComponent(), withIntermediateDirectories: true,
        attributes: [.posixPermissions: 0o700])
    let fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0o600)
    guard fd >= 0 else {
        throw JazzError.io("can't create \(path): \(String(cString: strerror(errno)))")
    }
    defer { close(fd) }
    let bytes = Array((key + "\n").utf8)
    let written = bytes.withUnsafeBufferPointer { write(fd, $0.baseAddress, $0.count) }
    guard written == bytes.count, fsync(fd) == 0 else {
        let reason = String(cString: strerror(errno))
        unlink(path)
        throw JazzError.io("can't write \(path): \(reason)")
    }
    return key
}
