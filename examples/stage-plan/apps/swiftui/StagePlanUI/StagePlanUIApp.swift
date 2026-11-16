// StagePlan in SwiftUI: a show's tasks, live, through JazzQuery. It opens a
// local client (no server) so it runs on its own; an app of yours opens
// `.client(appId:serverUrl:auth:)` through your backend's /jazz instead.
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
            TasksView(jazz: jazz, show: show)
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
    let jazz: Jazz.Client
    let show: JazzApp.Show
    /// The show's tasks, in rank order, kept current as they change.
    @State private var tasks: JazzQuery<JazzApp.Task>

    init(jazz: Jazz.Client, show: JazzApp.Show) {
        self.jazz = jazz
        self.show = show
        _tasks = State(
            initialValue: JazzQuery(
                jazz, JazzApp.tasks.query().where(JazzApp.tasks.showId.eq(show.id)).orderBy(JazzApp.tasks.rank)))
    }

    var body: some View {
        NavigationStack {
            List(tasks.rows, id: \.id) { task in
                Text(task.title)
            }
            .overlay {
                if !tasks.isLoading && tasks.rows.isEmpty { Text("No tasks yet") }
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
        let rank = (tasks.rows.last?.rank ?? 0) + 1
        _ = try? await jazz.insert(
            JazzApp.NewTask(showId: show.id, title: "Task \(Int(rank))", status: .todo, rank: rank))
    }
}
