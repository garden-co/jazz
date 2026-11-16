// StagePlan in SwiftUI: a show's tasks, live, through @JazzAll. It opens a
// local client (no server) so it runs on its own, and puts it in the
// environment as `\.jazz`. An app of yours uses
// `JazzRoot(JazzApp.session(appId:serverUrl:…)) { … } signedOut: { … }`,
// which does that for the signed-in user.
import Jazz
import SwiftUI

@main
struct StagePlanUIApp: App {
    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}

struct ContentView: View {
    @State private var jazz: Jazz.Client?
    @State private var show: JazzApp.Show?
    @State private var failure: String?

    var body: some View {
        if let jazz, let show {
            TasksView(show: show).environment(\.jazz, jazz)
        } else if let failure {
            Text(failure)
        } else {
            ProgressView().task { await open() }
        }
    }

    func open() async {
        do {
            let jazz = try await JazzApp.open(.local())
            show = try await jazz.insert(
                JazzApp.NewShow(
                    name: "Summer tour", venue: "The Roundhouse", date: "2026-07-01", doors: "19:00",
                    chiefAccount: UUID().uuidString.lowercased()))
            self.jazz = jazz
        } catch {
            failure = "\(error)"
        }
    }
}

struct TasksView: View {
    let show: JazzApp.Show
    @Environment(\.jazz) private var jazz
    /// The show's tasks, in rank order, kept current as they change.
    @JazzAll<JazzApp.Task> private var tasks: [JazzApp.Task]

    init(show: JazzApp.Show) {
        self.show = show
        _tasks = JazzAll(JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(show.id)).orderBy(JazzApp.tasks.rank))
    }

    var body: some View {
        NavigationStack {
            List(tasks, id: \.id) { task in
                Text(task.title)
            }
            .overlay {
                if $tasks?.isLoading == false && tasks.isEmpty { Text("No tasks yet") }
            }
            .navigationTitle(show.name)
            .toolbar {
                Button("Add task") {
                    _Concurrency.Task { await add() }
                }
            }
        }
    }

    func add() async {
        guard let jazz else { return }
        let rank = (tasks.last?.rank ?? 0) + 1
        _ = try? await jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Task \(Int(rank))", status: .todo, rank: rank))
    }
}
