package stageplan

import jazzapp.Comments
import jazzapp.CrewTable
import jazzapp.NewComment
import jazzapp.NewCrew
import jazzapp.TaskStatus
import kotlinx.coroutines.launch
import tools.jazz.include
import jazzapp.Tasks
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import tools.jazz.Jazz
import java.net.ServerSocket
import java.net.Socket
import java.util.UUID
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** The StagePlan slice end to end, against a real in-memory Jazz server. */
private operator fun <T> List<T>.component6(): T = this[5]
private operator fun <T> List<T>.component7(): T = this[6]

class StagePlanTest {
    private val appId = UUID.randomUUID().toString()
    private val backendSecret = "backend-" + UUID.randomUUID()
    private val adminSecret = "admin-" + UUID.randomUUID()
    private lateinit var server: Process
    private lateinit var url: String

    @BeforeTest
    fun startServer() {
        val port = ServerSocket(0).use { it.localPort }
        url = "http://127.0.0.1:$port"
        server = ProcessBuilder(
            System.getenv("JAZZ_TOOLS"), "server", appId, "--in-memory", "--port", "$port",
            "--backend-secret", backendSecret, "--admin-secret", adminSecret,
        ).inheritIO().start()
        val deadline = System.currentTimeMillis() + 30_000
        while (true) {
            try {
                Socket("127.0.0.1", port).close()
                break
            } catch (e: java.io.IOException) {
                check(server.isAlive) { "jazz-tools server exited with ${server.exitValue()}" }
                check(System.currentTimeMillis() < deadline) { "jazz-tools server didn't start" }
                Thread.sleep(100)
            }
        }
    }

    @AfterTest
    fun stopServer() {
        server.destroy()
    }

    private suspend fun open(user: String = "user-1"): Jazz = openAsUser(url, appId, backendSecret, adminSecret, user)

    @Test
    fun create_a_show_add_tasks_and_move_them_between_stages() = runBlocking {
        open().use { jazz ->
            val show = showNamed(jazz, "Opening night")
            assertEquals(jazz.account(), show.chiefAccount)
            // The same show the second time.
            assertEquals(show.id, showNamed(jazz, "Opening night").id)

            val load = addTask(jazz, show, "Load in")
            val sound = addTask(jazz, show, "Sound check")
            assertEquals(listOf("Load in", "Sound check"), tasksOf(jazz, show).map { it.title })

            moveTask(jazz, load.id, TaskStatus.Doing)
            moveTask(jazz, sound.id, TaskStatus.Blocked)
            assertEquals(listOf(TaskStatus.Doing, TaskStatus.Blocked), tasksOf(jazz, show).map { it.status })
        }
    }

    @Test
    fun docs_snippets_run() = runBlocking {
        DocsSnippets.openBackend(url, appId, backendSecret, adminSecret, "user-2").close()
        open("user-2").use { jazz ->
            val show = showNamed(jazz, "Matinee")
            val a = addTask(jazz, show, "A")
            addTask(jazz, show, "B")
            assertEquals(listOf("A", "B"), DocsSnippets.oneShot(jazz, show.id).sortedBy { it.rank }.map { it.title })
            assertEquals(listOf("A", "B"), DocsSnippets.sorting(jazz, show.id).map { it.title })
            assertEquals(2, DocsSnippets.filters(jazz, show.id).size)
            assertEquals(emptyList(), DocsSnippets.pagination(jazz, show.id))
            moveTask(jazz, a.id, TaskStatus.Blocked)
            val (blocked, notDone, topTen, later, sound, unassigned, assigned) = DocsSnippets.whereOperators(jazz, show.id)
            assertEquals(listOf("A"), blocked.map { it.title })
            assertEquals(2, notDone.size)
            assertEquals(2, topTen.size)
            assertEquals(0, later.size)
            assertEquals(0, sound.size)
            assertEquals(2, unassigned.size)
            assertEquals(0, assigned.size)
            DocsSnippets.crud(jazz, show.id)
            DocsSnippets.nullableUpdate(jazz, a.id)
            assertNull(jazz.one(Tasks.where(Tasks.id eq a.id))!!.assigneeId)
            val results = kotlinx.coroutines.coroutineScope {
                val watching = async { kotlinx.coroutines.withTimeout(30_000) { DocsSnippets.subscriptions(jazz, show.id) } }
                kotlinx.coroutines.delay(500)
                addTask(jazz, show, "C")
                watching.await()
            }
            assertEquals(listOf("A", "B"), results[0])
            assertEquals(listOf("A", "B", "C"), results[1])
        }
    }

    @Test
    fun includes_and_select() = runBlocking {
        open("user-3").use { jazz ->
            val me = jazz.account()!!
            val show = showNamed(jazz, "Encore")
            val ada = jazz.insert(CrewTable, NewCrew(account = me, name = "Ada"))
            // Permissions let a task be assigned only to the show's crew.
            jazz.insert(jazzapp.ShowCrewTable, jazzapp.NewShowCrew(showId = show.id, crewId = ada.id, account = me, role = jazzapp.ShowCrewRole.Chief))
            val load = addTask(jazz, show, "Load in")
            addTask(jazz, show, "Sound check")
            jazz.update(Tasks, load.id) { assigneeId(ada.id) }
            jazz.insert(Comments, NewComment(taskId = load.id, authorId = ada.id, body = "b: trucks at 9"))
            jazz.insert(Comments, NewComment(taskId = load.id, authorId = ada.id, body = "a: dock 2"))
            assertEquals(
                listOf("Load in (Ada) [Ada: a: dock 2, Ada: b: trucks at 9]", "Sound check (unassigned) []"),
                DocsSnippets.includes(jazz, show.id),
            )
            assertEquals(mapOf("Encore" to 2), DocsSnippets.reverseRelation(jazz, show.id))
            assertEquals(
                listOf("Load in" to TaskStatus.Todo, "Sound check" to TaskStatus.Todo),
                DocsSnippets.select(jazz, show.id).sortedBy { it.first },
            )
            // A required include (through a non-optional reference) keeps only rows whose target is readable.
            val withAuthors = jazz.all(Comments.where(Comments.taskId eq load.id).include(Comments.author.required()))
            assertEquals(listOf("Ada", "Ada"), withAuthors.map { it[Comments.author]?.name })
            // Subscriptions deliver included rows, and an update when a related row changes.
            val seen = kotlinx.coroutines.channels.Channel<List<String?>>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            val job = launch {
                jazz.watch(Tasks.where(Tasks.showId eq show.id).orderBy(Tasks.rank.asc()).include(Tasks.assignee))
                    .collect { rows -> seen.send(rows.map { it[Tasks.assignee]?.name }) }
            }
            kotlinx.coroutines.withTimeout(30_000) {
                while (seen.receive() != listOf("Ada", null)) Unit
                jazz.update(CrewTable, ada.id) { name("Ada L.") }
                while (seen.receive() != listOf("Ada L.", null)) Unit
            }
            job.cancel()
        }
    }

    @Test
    fun close_ends_watches_and_later_calls_with_closed(): Unit = runBlocking {
        val jazz = open("user-4")
        val show = showNamed(jazz, "Closing night")
        val seen = kotlinx.coroutines.channels.Channel<Int>(kotlinx.coroutines.channels.Channel.UNLIMITED)
        val watching = async { runCatching { jazz.watch(Tasks.where(Tasks.showId eq show.id)).collect { seen.send(it.size) } } }
        kotlinx.coroutines.withTimeout(30_000) { seen.receive() }
        jazz.close()
        jazz.close()
        val ended = kotlinx.coroutines.withTimeout(30_000) { watching.await() }
        kotlin.test.assertTrue(ended.exceptionOrNull() is tools.jazz.JazzException.Closed, "$ended")
        kotlin.test.assertFailsWith<tools.jazz.JazzException.Closed> { tasksOf(jazz, show) }
    }
}
