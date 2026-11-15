package stageplan

import jazzapp.Show
import jazzapp.NewShow
import jazzapp.Shows
import jazzapp.Task
import jazzapp.NewTask
import jazzapp.TaskStatus
import jazzapp.Tasks
import tools.jazz.Jazz
import tools.jazz.JazzAuth
import tools.jazz.JazzConfig
import tools.jazz.SessionState
import tools.jazz.Storage

/**
 * Opens StagePlan as an end user of the backend's Jazz endpoint [endpoint]
 * (e.g. `https://stageplan.example/jazz`), signed in with [auth]: a device
 * account, or the backend's own sign-in.
 */
suspend fun openAsUser(endpoint: String, appId: String, auth: JazzAuth, dataDir: String? = null): Jazz =
    jazzapp.open(
        JazzConfig(
            appId = appId,
            serverUrl = endpoint,
            storage = if (dataDir == null) Storage.Memory else Storage.Persistent,
            dataDir = dataDir,
            auth = auth,
        ),
    )

/** The signed-in user's account id. */
fun me(jazz: Jazz): String = (jazz.session.value as? SessionState.SignedIn)?.user?.account ?: error("not signed in")

/** The signed-in user's show called [name], created (with them as its chief) if there is none. */
suspend fun showNamed(jazz: Jazz, name: String): Show {
    val me = me(jazz)
    jazz.one(Shows.where(Shows.name eq name, Shows.chiefAccount eq me))?.let { return it }
    return jazz.insert(
        Shows,
        NewShow(name = name, venue = "TBA", date = "TBA", doors = "TBA", chiefAccount = me),
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
        NewTask(showId = show.id, title = title, status = TaskStatus.Todo, rank = (last?.rank ?: 0.0) + 1.0),
    )
}

/** Moves a task to another stage. */
suspend fun moveTask(jazz: Jazz, taskId: String, status: TaskStatus) {
    jazz.update(Tasks, taskId) { status(status) }
}
