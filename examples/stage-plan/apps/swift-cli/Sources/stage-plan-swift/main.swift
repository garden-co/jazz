import Foundation
import Jazz
import StagePlan

// stage-plan-swift [--key-file <path>] [--bearer <token>] [--data-dir <dir>] [--show <name>] <command>
// An end user of StagePlan: it signs in to the app's Jazz endpoint
// (JAZZ_ENDPOINT, the backend's handler; JAZZ_APP_ID) with a device account,
// whose key lives in --key-file (made on first use, mode 0600; default
// ~/.config/stage-plan-swift/device-key), or with the app's bearer token.
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
let bearer = option("--bearer")
let dataDir = option("--data-dir")
let showName = option("--show") ?? "Opening night"

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
    let show = try await app.show(named: showName)
    switch args.first {
    case "list"?:
        printTasks(try await app.tasks(of: show))
    case "add"? where args.count == 2:
        let task = try await app.addTask(args[1], to: show)
        print(task.id)
    case "move"? where args.count == 3:
        guard let status = JazzApp.TaskStatus(rawValue: args[2]) else {
            throw JazzError.refused("stage must be one of todo, doing, done, blocked")
        }
        try await app.move(args[1], to: status)
    case "demo"?:
        let load = try await app.addTask("Load in", to: show)
        try await app.addTask("Sound check", to: show)
        try await app.move(load.id, to: .doing)
        printTasks(try await app.tasks(of: show))
    default:
        print("usage: stage-plan-swift [--key-file <path>] [--bearer <token>] [--data-dir <dir>] [--show <name>] list | add <title> | move <task-id> <stage> | demo")
        exit(2)
    }
    try await app.jazz.close()
} catch {
    FileHandle.standardError.write(Data("error: \(error)\n".utf8))
    exit(1)
}

/// The device account's key at `path`, made (mode 0600) if there's none.
func deviceKey(at path: String) throws -> String {
    if let key = try? String(contentsOfFile: path, encoding: .utf8) {
        return key.trimmingCharacters(in: .whitespacesAndNewlines)
    }
    let key = try Jazz.generateDeviceKey()
    let url = URL(fileURLWithPath: path)
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
    FileManager.default.createFile(atPath: path, contents: Data(key.utf8), attributes: [.posixPermissions: 0o600])
    return key
}
