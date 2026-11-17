package stageplan

import jazzapp.TaskStatus
import kotlinx.coroutines.runBlocking
import tools.jazz.JazzAuth
import tools.jazz.JazzException
import kotlin.system.exitProcess

private const val USAGE = """usage: stage-plan (<show name> | --show <show id>) [tasks | add <title> | move <task id> <todo|doing|done|blocked>]

<show name> is the signed-in user's show of that name (created, with them as its
chief, if there is none); --show <id> is any show by id, if they may read it.
Writes wait for the server: a write its permissions refuse exits 4.

An end user of StagePlan's backend, through its Jazz endpoint JAZZ_ENDPOINT
(e.g. https://stageplan.example/jazz) for the app JAZZ_APP_ID. It signs in with
the backend's bearer token JAZZ_TOKEN if set, else with a device account whose
key it keeps in JAZZ_DEVICE_KEY_FILE (default ~/.stage-plan/device-key), and
keeps its data in JAZZ_DATA_DIR (default ~/.stage-plan/data)."""

fun main(args: Array<String>): Unit = runBlocking {
    if (args.isEmpty()) {
        System.err.println(USAGE)
        exitProcess(2)
    }
    fun env(name: String) = System.getenv(name) ?: run {
        System.err.println("$name is unset\n\n$USAGE")
        exitProcess(2)
    }
    val home = System.getProperty("user.home") + "/.stage-plan"
    val token = System.getenv("JAZZ_TOKEN")
    val auth = if (token != null) JazzAuth.Bearer { token } else JazzAuth.DeviceAccount(deviceKey(System.getenv("JAZZ_DEVICE_KEY_FILE") ?: "$home/device-key"))
    openAsUser(env("JAZZ_ENDPOINT"), env("JAZZ_APP_ID"), auth, System.getenv("JAZZ_DATA_DIR") ?: "$home/data").use { jazz ->
        val byId = args[0] == "--show"
        val rest = if (byId) args.drop(2) else args.drop(1)
        val show = if (byId) {
            showById(jazz, args.getOrNull(1) ?: usage()) ?: run {
                System.err.println("no show ${args[1]} visible to you")
                exitProcess(3)
            }
        } else {
            showNamed(jazz, args[0])
        }
        try {
            when (rest.getOrNull(0) ?: "tasks") {
                "tasks" -> Unit
                "add" -> println("added " + addTask(jazz, show, rest.drop(1).joinToString(" ")).id)
                "move" -> {
                    val status = TaskStatus.entries.firstOrNull { it.name.equals(rest.getOrNull(2), ignoreCase = true) }
                    if (rest.size != 3 || status == null) usage()
                    moveTask(jazz, rest[1], status)
                    println("moved ${rest[1]} to ${status.name.lowercase()}")
                }
                else -> usage()
            }
        } catch (e: JazzException) {
            // Permission: the server's permissions refused it; Refused: e.g. a row this user can't read.
            if (e !is JazzException.Permission && e !is JazzException.Refused) throw e
            System.err.println("refused: ${e.message}")
            exitProcess(4)
        }
        println("${show.name} (${show.id})")
        for (task in tasksOf(jazz, show)) {
            println("  ${task.id}  [${task.status.name.lowercase()}] ${task.title}")
        }
    }
}

private fun usage(): Nothing {
    System.err.println(USAGE)
    exitProcess(2)
}

/** The device account's key in [path], made (readable by this user only) the first time. */
private fun deviceKey(path: String): String {
    val file = java.io.File(path)
    if (!file.exists()) {
        file.parentFile?.mkdirs()
        file.writeText(JazzAuth.generateDeviceKey())
        file.setReadable(false, false)
        file.setReadable(true, true)
        file.setWritable(false, false)
        file.setWritable(true, true)
    }
    return file.readText().trim()
}
