package stageplan

import jazzapp.TaskStatus
import kotlinx.coroutines.runBlocking
import kotlin.system.exitProcess

private const val USAGE = """usage: stage-plan <show> [tasks | add <title> | move <task id> <todo|doing|done|blocked>]

Acts as the user JAZZ_USER, as a backend of the Jazz server at JAZZ_SERVER_URL
(app JAZZ_APP_ID, secrets JAZZ_BACKEND_SECRET and JAZZ_ADMIN_SECRET)."""

fun main(args: Array<String>): Unit = runBlocking {
    if (args.isEmpty()) {
        System.err.println(USAGE)
        exitProcess(2)
    }
    fun env(name: String) = System.getenv(name) ?: run {
        System.err.println("$name is unset\n\n$USAGE")
        exitProcess(2)
    }
    openAsUser(
        serverUrl = env("JAZZ_SERVER_URL"),
        appId = env("JAZZ_APP_ID"),
        backendSecret = env("JAZZ_BACKEND_SECRET"),
        adminSecret = env("JAZZ_ADMIN_SECRET"),
        user = env("JAZZ_USER"),
    ).use { jazz ->
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
