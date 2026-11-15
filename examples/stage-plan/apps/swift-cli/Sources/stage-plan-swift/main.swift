import Foundation
import Jazz
import StagePlan

// stage-plan-swift [--user <uuid>] [--data-dir <dir>] [--show <name>] <command>
// The sync server comes from JAZZ_SERVER_URL, JAZZ_APP_ID, JAZZ_BACKEND_SECRET
// and JAZZ_ADMIN_SECRET (`jazz-tools server <app-id> --in-memory
// --backend-secret … --admin-secret …` runs one locally).
//   list                    the show's tasks, by rank
//   add <title>             add a task
//   move <task-id> <stage>  todo | doing | done | blocked
//   demo                    create a show, add tasks, move one, list them
var args = Array(CommandLine.arguments.dropFirst())
func option(_ name: String) -> String? {
    guard let at = args.firstIndex(of: name), at + 1 < args.count else { return nil }
    defer { args.removeSubrange(at...at + 1) }
    return args[at + 1]
}
let user = option("--user") ?? "00000000-0000-4000-8000-000000000001"
let dataDir = option("--data-dir")
let showName = option("--show") ?? "Opening night"

func printTasks(_ tasks: [JazzApp.Task]) {
    for task in tasks {
        print("\(task.id)  \(task.status.rawValue.padding(toLength: 8, withPad: " ", startingAt: 0))\(task.title)")
    }
}

do {
    guard let server = Server.fromEnvironment() else {
        throw JazzError.refused(
            "set JAZZ_SERVER_URL, JAZZ_APP_ID, JAZZ_BACKEND_SECRET and JAZZ_ADMIN_SECRET")
    }
    let app = try await StagePlanApp.open(user: user, server: server, dataDir: dataDir)
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
        print("usage: stage-plan-swift [--user <uuid>] [--data-dir <dir>] [--show <name>] list | add <title> | move <task-id> <stage> | demo")
        exit(2)
    }
    try await app.jazz.close()
} catch {
    FileHandle.standardError.write(Data("error: \(error)\n".utf8))
    exit(1)
}
