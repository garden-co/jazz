package todoclient

import jazzapp.NewProject
import jazzapp.NewTodo
import jazzapp.Projects
import jazzapp.Todo
import jazzapp.Todos
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import tools.jazz.Jazz
import tools.jazz.JazzAuth
import tools.jazz.JazzConfig
import tools.jazz.ReadTier
import tools.jazz.Storage
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The docs snippets end to end, as an end user of the docs todo app's backend. */
class DocsSnippetsTest {
    private lateinit var core: Process
    private lateinit var relay: Process
    private val appId = java.util.UUID.randomUUID().toString()
    private lateinit var endpoint: String

    /**
     * The app's backend for the test: a Jazz server (`jazz-tools server`) and
     * `jazz-app-test-relay`, which publishes the app's permissions and stands
     * in for a backend's Jazz endpoint. A test harness, not a backend to copy.
     */
    @BeforeTest
    fun startBackend() {
        val backendSecret = "backend-" + java.util.UUID.randomUUID()
        val adminSecret = "admin-" + java.util.UUID.randomUUID()
        val port = java.net.ServerSocket(0).use { it.localPort }
        core = ProcessBuilder(System.getenv("JAZZ_TOOLS"), "server", appId, "--in-memory", "--port", "$port")
            .redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT)
            .apply { environment()["JAZZ_BACKEND_SECRET"] = backendSecret; environment()["JAZZ_ADMIN_SECRET"] = adminSecret }
            .start()
        val deadline = System.currentTimeMillis() + 30_000
        while (runCatching { java.net.Socket("127.0.0.1", port).close() }.isFailure) {
            check(core.isAlive && System.currentTimeMillis() < deadline) { "jazz-tools server didn't start" }
            Thread.sleep(100)
        }
        relay = ProcessBuilder(
            System.getenv("JAZZ_APP_TEST_RELAY"), "--core", "http://127.0.0.1:$port", "--app-id", appId,
            "--app", java.io.File("jazz.app.json").absolutePath,
            "--permissions", java.io.File("jazz.permissions.json").absolutePath,
        ).redirectError(ProcessBuilder.Redirect.INHERIT)
            .apply { environment()["JAZZ_BACKEND_SECRET"] = backendSecret; environment()["JAZZ_ADMIN_SECRET"] = adminSecret }
            .start()
        endpoint = relay.inputStream.bufferedReader().readLine() ?: error("jazz-app-test-relay didn't start")
    }

    @AfterTest
    fun stopBackend() {
        relay.destroy()
        core.destroy()
    }

    /** [user], signed in with the backend's sign-in (the harness takes `token-<user>`). */
    private suspend fun open(user: String = "user-1"): Jazz = jazzapp.open(
        JazzConfig(appId = appId, serverUrl = endpoint, storage = Storage.Memory, auth = JazzAuth.Bearer { "token-$user" }),
    )

    private suspend fun Jazz.todo(title: String, done: Boolean = false, priority: Int? = null, parentId: String? = null, projectId: String? = null): Todo =
        insert(Todos, NewTodo(title = title, done = done, priority = priority, parentId = parentId, projectId = projectId))

    private suspend fun Jazz.byId(id: String): Todo? = one(Todos.where(Todos.id eq id))

    @Test
    fun reading_snippets() = runBlocking {
        open().use { jazz ->
            val project = jazz.insert(Projects, NewProject(name = "Docs site"))
            val other = jazz.insert(Projects, NewProject(name = "Release"))
            val write = jazz.todo("Write docs", projectId = project.id)
            jazz.todo("Proofread docs", projectId = project.id, parentId = write.id)
            jazz.todo("Archive docs", done = true, projectId = project.id)
            jazz.todo("Cut release", projectId = other.id)
            val open = listOf("Cut release", "Proofread docs", "Write docs")

            assertEquals(open, readOpenTodos(jazz).map { it.title }.sorted())
            assertEquals("Write docs", readTodo(jazz, write.id)?.title)
            assertNull(readTodo(jazz, java.util.UUID.randomUUID().toString()))
            assertEquals(listOf("Proofread docs", "Write docs"), readOpenDocsTodos(jazz).map { it.title }.sorted())
            assertEquals(open, readOpenTodosByTitle(jazz).map { it.title })
            assertEquals(open, readTodoPage(jazz, page = 1).map { it.title })
            assertEquals(listOf("Proofread docs"), readTodoPage(jazz, page = 2, pageSize = 1).map { it.title })
            assertEquals(emptyList(), readTodoPage(jazz, page = 2).map { it.title })
            jazz.waitForSync()
            assertEquals(open, readOpenTodosAtRemoteTier(jazz).map { it.title }.sorted())

            // Each todo's project, and its parent's project.
            val included = readOpenTodosWithProjects(jazz).associate { todo ->
                todo[Todos.title] to (todo[Todos.project]?.name to todo.row(Todos.parent)?.get(Todos.project)?.name)
            }
            assertEquals(
                mapOf(
                    "Cut release" to ("Release" to null),
                    "Proofread docs" to ("Docs site" to "Docs site"),
                    "Write docs" to ("Docs site" to null),
                ),
                included,
            )
            assertEquals(
                mapOf("Docs site" to listOf("Proofread docs", "Write docs"), "Release" to listOf("Cut release")),
                readProjectsWithOpenTodos(jazz).associate { p -> p[Projects.name] to p[Projects.todos].map { it.title }.sorted() },
            )
            assertEquals(
                listOf("Cut release" to "Release", "Proofread docs" to "Docs site", "Write docs" to "Docs site"),
                readOpenTodoTitlesWithProjectNames(jazz)
                    .map { it[Todos.title] to it.row(Todos.project)?.get(Projects.name) }
                    .sortedBy { it.first },
            )

            // Permission advice: the docs app lets everyone read, write and delete todos.
            assertEquals(true, canReadTodo(jazz, write.id))
            assertEquals(true, canCreateTodo(jazz, "New todo"))
            assertEquals(4, readTodosWithDeletePermission(jazz).size)
            assertEquals(
                listOf("Archive docs", "Cut release", "Proofread docs", "Write docs"),
                readEditableTodos(jazz).map { it[Todos.title] },
            )
        }
    }

    @Test
    fun subscriptions_deliver_the_whole_result_until_cancelled() = runBlocking {
        open().use { jazz ->
            jazz.todo("First")
            val seen = Channel<List<String>>(Channel.UNLIMITED)
            val job = subscribeOpenTodos(this, jazz) { todos -> seen.trySend(todos.map { it.title }.sorted()) }
            withTimeout(30_000) {
                while (seen.receive() != listOf("First")) Unit
                val second = jazz.todo("Second")
                while (seen.receive() != listOf("First", "Second")) Unit
                jazz.update(Todos, second.id) { done(true) }
                while (seen.receive() != listOf("First")) Unit
            }
            job.cancel()
            job.join()
            // Unsubscribed: no more results.
            jazz.todo("Third")
            kotlinx.coroutines.delay(300)
            assertTrue(seen.tryReceive().isFailure)
        }
    }

    @Test
    fun where_subscription_snippet() = runBlocking {
        open().use { jazz ->
            jazz.todo("First")
            withTimeout(30_000) {
                val results = watchOpenTodos(jazz).map { todos -> todos.map { it.title }.sorted() }
                val seen = async { results.first { it == listOf("First", "Second") } }
                kotlinx.coroutines.delay(300)
                jazz.todo("Second")
                assertEquals(listOf("First", "Second"), seen.await())
            }
        }
    }

    @Test
    fun recursive_snippet() = runBlocking {
        open().use { jazz ->
            val root = jazz.todo("Root", done = true)
            val middle = jazz.todo("Middle", done = true, parentId = root.id)
            jazz.todo("Leaf", parentId = middle.id)
            jazz.todo("Alone")
            assertEquals(
                listOf("Alone", "Leaf", "Middle", "Root"),
                jazz.all(buildTodoLineageQuery()).map { it.title }.sorted(),
            )
        }
    }

    @Test
    fun where_operator_snippets() = runBlocking {
        open().use { jazz ->
            val a = jazz.todo("Buy milk", priority = 5)
            kotlinx.coroutines.delay(5) // distinct $createdAt, for the order-limit query
            val b = jazz.todo("Draft", done = true, priority = 12)
            kotlinx.coroutines.delay(5)
            jazz.todo("Oat milk", priority = 1, parentId = a.id)
            kotlinx.coroutines.delay(5)
            jazz.todo("No priority")
            jazz.todo("Shipped", done = true, projectId = java.util.UUID.randomUUID().toString())
            val r = whereOperatorExamples(jazz, "milk", a.id, b.id)
            assertEquals(listOf("Buy milk", "No priority", "Oat milk"), r.incompleteTodos.map { it.title }.sorted())
            assertEquals(listOf("Buy milk", "No priority", "Oat milk", "Shipped"), r.nonDraftTodos.map { it.title }.sorted())
            assertEquals(setOf(a.id, b.id), r.selectedTodos.map { it.id }.toSet())
            assertEquals(5, r.recentTodos.size)
            assertEquals(listOf("Buy milk", "Draft"), r.highPriority.map { it.title }.sorted())
            assertEquals(listOf("Buy milk", "Oat milk"), r.lowPriority.map { it.title }.sorted())
            assertEquals(listOf("Buy milk", "Oat milk"), r.matches.map { it.title }.sorted())
            assertEquals(listOf("Buy milk", "Draft", "No priority", "Shipped"), r.unlinkedTodos.map { it.title }.sorted())
            assertEquals(listOf("Oat milk"), r.linkedTodos.map { it.title })
            assertEquals(listOf("Shipped"), r.doneWithProject.map { it.title })
            // Open todos, oldest first.
            assertEquals(listOf("Buy milk", "Oat milk", "No priority"), r.recentIncomplete.map { it.title })
        }
    }

    @Test
    fun writing_snippets() = runBlocking {
        open().use { jazz ->
            // CRUD: inserts "Write docs", then completes and deletes todoId.
            val existing = jazz.todo("Existing")
            writeTodoCrud(jazz, existing.id)
            assertNull(jazz.byId(existing.id))
            val written = jazz.one(Todos.where(Todos.title eq "Write docs"))
            assertNotNull(written)
            assertEquals(EXAMPLE_OWNER_ID to EXAMPLE_PROJECT_ID, written.owner_id to written.projectId)

            // Clearing a nullable column leaves the others alone.
            val owned = jazz.insert(Todos, NewTodo(title = "Owned", done = false, description = "kept", owner_id = EXAMPLE_OWNER_ID))
            clearTodoOwner(jazz, owned.id)
            val cleared = jazz.byId(owned.id)!!
            assertEquals(null to "kept", cleared.owner_id to cleared.description)

            // Upsert, at the sync server.
            val importedId = java.util.UUID.randomUUID().toString()
            upsertTodo(jazz, importedId)
            assertEquals("Imported task", jazz.one(Todos.where(Todos.id eq importedId), ReadTier.Remote)?.title)

            // Delete and restore with new content.
            val restored = restoreDeletedTodo(jazz, importedId)
            assertEquals(importedId to "Restored task", restored.id to restored.title)
            assertEquals("Restored task", jazz.byId(importedId)?.title)

            // Insert, update and delete, each at the sync server: nothing left.
            writeTodoWithDurabilityTiers(jazz)
            assertEquals(
                emptyList(),
                jazz.all(Todos.where(Todos.title eq "Write docs with durability tier"), ReadTier.Remote),
            )

            // A global insert the server accepts.
            val shipped = insertTodoAndWait(jazz)
            assertNotNull(shipped)
            assertEquals("Ship review fixes", jazz.one(Todos.where(Todos.id eq shipped.id), ReadTier.Remote)?.title)

            // A transaction: the new todo's id, and existingTodoId done.
            val target = jazz.todo("Review transaction docs")
            val createdId = groupTodoWrites(jazz, target.id)
            assertEquals("Write transaction docs", jazz.one(Todos.where(Todos.id eq createdId), ReadTier.Remote)?.title)
            assertEquals(true, jazz.one(Todos.where(Todos.id eq target.id), ReadTier.Remote)?.done)

            // A transaction the server accepts.
            val other = jazz.todo("Finish in a transaction")
            completeTodoInTransaction(jazz, other.id)
            assertEquals(true, jazz.one(Todos.where(Todos.id eq other.id), ReadTier.Remote)?.done)

            // A serializable transaction: the todo's id, and the todo done.
            val serial = jazz.todo("Finish serializably")
            assertEquals(serial.id, finishTodoSerializably(jazz, serial.id))
            assertEquals(true, jazz.one(Todos.where(Todos.id eq serial.id), ReadTier.Remote)?.done)
            // A missing todo fails the block, and nothing is written.
            val missing = kotlin.runCatching { finishTodoSerializably(jazz, java.util.UUID.randomUUID().toString()) }
            assertEquals("Todo not found", missing.exceptionOrNull()?.message)
        }
    }
}
