package stageplan

import jazzapp.TaskStatus
import kotlinx.coroutines.runBlocking
import tools.jazz.JazzAuth
import kotlin.system.exitProcess

private const val USAGE = """usage: stage-plan <show> [tasks | add <title> | move <task id> <todo|doing|done|blocked>]

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
        val show = showNamed(jazz, args[0])
        when (args.getOrNull(1) ?: "tasks") {
            "tasks" -> Unit
            "add" -> addTask(jazz, show, args.drop(2).joinToString(" "))
            "move" -> {
                val status = TaskStatus.entries.firstOrNull { it.name.equals(args.getOrNull(3), ignoreCase = true) }
                if (args.size != 4 || status == null) {
                    System.err.println(USAGE)
                    exitProcess(2)
                }
                moveTask(jazz, args[2], status)
            }
            else -> {
                System.err.println(USAGE)
                exitProcess(2)
            }
        }
        println("${show.name} (${show.id})")
        for (task in tasksOf(jazz, show)) {
            println("  ${task.id}  [${task.status.name.lowercase()}] ${task.title}")
        }
    }
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
