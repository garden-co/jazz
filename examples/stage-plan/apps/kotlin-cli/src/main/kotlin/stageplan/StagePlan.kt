package stageplan

import jazzapp.Show
import jazzapp.ShowInsert
import jazzapp.Shows
import jazzapp.Task
import jazzapp.TaskInsert
import jazzapp.TaskStatus
import jazzapp.Tasks
import tools.jazz.Jazz
import tools.jazz.JazzConfig
import tools.jazz.Storage

/** The app's `jazz.permissions.json`, bundled as a resource. */
fun permissions(): String =
    checkNotNull(object {}.javaClass.getResource("/jazz.permissions.json")) { "jazz.permissions.json is not bundled" }
        .readText()

/** Opens StagePlan as a backend of the server at [serverUrl], acting as [user]. */
suspend fun openAsUser(serverUrl: String, appId: String, backendSecret: String, adminSecret: String, user: String): Jazz =
    jazzapp.open(
        JazzConfig(
            appId = appId,
            serverUrl = serverUrl,
            storage = Storage.Memory,
            permissions = permissions(),
            backendSecret = backendSecret,
            adminSecret = adminSecret,
            sessionSubject = user,
        ),
    )

/** The signed-in user's show called [name], created (with them as its chief) if there is none. */
suspend fun showNamed(jazz: Jazz, name: String): Show {
    val me = checkNotNull(jazz.account()) { "not signed in" }
    jazz.one(Shows.where(Shows.name eq name, Shows.chiefAccount eq me))?.let { return it }
    return jazz.insert(
        Shows,
        ShowInsert(name = name, venue = "TBA", date = "TBA", doors = "TBA", chiefAccount = me),
    )
}

/** The show's tasks, in `rank` order. */
suspend fun tasksOf(jazz: Jazz, show: Show): List<Task> =
    jazz.all(Tasks.where(Tasks.showId eq show.id).orderBy(Tasks.rank.asc()))

/** Adds a task at the end of the show's list, in `todo`. */
suspend fun addTask(jazz: Jazz, show: Show, title: String): Task {
    val last = jazz.one(Tasks.where(Tasks.showId eq show.id).orderBy(Tasks.rank.desc()))
    return jazz.insert(
        Tasks,
        TaskInsert(showId = show.id, title = title, status = TaskStatus.Todo, rank = (last?.rank ?: 0.0) + 1.0),
    )
}

/** Moves a task to another stage. */
suspend fun moveTask(jazz: Jazz, taskId: String, status: TaskStatus) {
    jazz.update(Tasks, taskId) { status(status) }
}
