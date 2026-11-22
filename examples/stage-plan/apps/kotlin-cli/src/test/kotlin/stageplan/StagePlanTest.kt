package stageplan

import jazzapp.Comments
import jazzapp.CrewTable
import jazzapp.NewComment
import jazzapp.NewCrew
import jazzapp.TaskStatus
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import tools.jazz.history
import tools.jazz.include
import jazzapp.Tasks
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import tools.jazz.Jazz
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** The StagePlan slice end to end: end users of a StagePlan backend (a Jazz server, its backend and its endpoint). */
class StagePlanTest {
    private lateinit var core: Process
    private lateinit var relay: Process
    private val appId = java.util.UUID.randomUUID().toString()
    private lateinit var endpoint: String

    /**
     * StagePlan's backend for the test: a Jazz server (`jazz-tools server`)
     * and `jazz-app-test-relay`, which publishes StagePlan's permissions and
     * stands in for the backend's Jazz endpoint. A test harness, not a backend
     * to copy: an app's main backend runs in TypeScript.
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
    private suspend fun open(user: String = "user-1"): Jazz = openAsUser(endpoint, appId, tools.jazz.JazzAuth.Bearer { "token-$user" })

    @Test
    fun create_a_show_add_tasks_and_move_them_between_stages() = runBlocking {
        open().use { jazz ->
            val show = showNamed(jazz, "Opening night")
            assertEquals(me(jazz), show.chiefAccount)
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
    fun includes_and_select() = runBlocking {
        open("user-3").use { jazz ->
            val me = me(jazz)
            val show = showNamed(jazz, "Encore")
            val ada = jazz.insert(CrewTable, NewCrew(account = me, name = "Ada"))
            // Permissions let a task be assigned only to the show's crew.
            jazz.insert(jazzapp.ShowCrewTable, jazzapp.NewShowCrew(showId = show.id, crewId = ada.id, account = me, role = jazzapp.ShowCrewRole.Chief))
            val load = addTask(jazz, show, "Load in")
            addTask(jazz, show, "Sound check")
            jazz.update(Tasks, load.id) { assigneeId(ada.id) }
            jazz.insert(Comments, NewComment(taskId = load.id, authorId = ada.id, body = "b: trucks at 9"))
            jazz.insert(Comments, NewComment(taskId = load.id, authorId = ada.id, body = "a: dock 2"))
            // Nested includes, typed by the relations jazz.app.json declares.
            val withComments = jazz.all(
                Tasks.where(Tasks.showId eq show.id)
                    .orderBy(Tasks.rank.asc())
                    .include(Tasks.assignee, Tasks.comments.orderBy(Comments.body.asc()).include(Comments.author)),
            ).map { task ->
                val comments = task.rows(Tasks.comments).map { "${it[Comments.author]?.name}: ${it[Comments.body]}" }
                "${task[Tasks.title]} (${task[Tasks.assignee]?.name ?: "unassigned"}) $comments"
            }
            assertEquals(
                listOf("Load in (Ada) [Ada: a: dock 2, Ada: b: trucks at 9]", "Sound check (unassigned) []"),
                withComments,
            )
            // A required include (through a non-optional reference) keeps only rows whose target is readable.
            val withAuthors = jazz.all(Comments.where(Comments.taskId eq load.id).include(Comments.author.required()))
            assertEquals(listOf("Ada", "Ada"), withAuthors.map { it[Comments.author]?.name })
            assertEquals(2, DocsSnippets.requireIncludes(jazz, load.id).size)
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
        assertEquals(tools.jazz.SessionState.SignedOut, jazz.session.value)
        kotlin.test.assertFailsWith<tools.jazz.JazzException.Closed> { tasksOf(jazz, show) }
    }

    @Test
    fun transactions_tiers_and_permission_errors(): Unit = runBlocking {
        open("user-5").use { jazz ->
            val me = me(jazz)
            val show = showNamed(jazz, "Gala")
            val a = addTask(jazz, show, "A")
            addTask(jazz, show, "B")
            // Rolls back when the block throws, and rethrows.
            val error = kotlin.test.assertFailsWith<IllegalStateException> {
                jazz.transaction { tx ->
                    tx.update(Tasks, a.id) { title("renamed") }
                    assertEquals("renamed", tx.one(Tasks.where(Tasks.id eq a.id))!!.title)
                    error("changed my mind")
                }
            }
            assertEquals("changed my mind", error.message)
            assertEquals("A", jazz.one(Tasks.where(Tasks.id eq a.id))!!.title)
            // A global write the permissions refuse throws: Ada isn't in the show's crew.
            val ada = jazz.insert(CrewTable, NewCrew(account = me, name = "Ada"), tier = tools.jazz.WriteTier.Global)
            kotlin.test.assertFailsWith<tools.jazz.JazzException.Permission> {
                jazz.update(Tasks, a.id, tier = tools.jazz.WriteTier.Global) { assigneeId(ada.id) }
            }
            // A transaction the server refuses throws at WriteTier.Global, with nothing applied.
            kotlin.test.assertFailsWith<tools.jazz.JazzException.Permission> {
                jazz.transaction(tier = tools.jazz.WriteTier.Global) { tx ->
                    tx.update(Tasks, a.id) { status(TaskStatus.Blocked) }
                    tx.insert(Comments, NewComment(taskId = a.id, authorId = java.util.UUID.randomUUID().toString(), body = "blocked"))
                }
            }
            // Nothing of the refused transaction applied.
            kotlin.test.assertTrue(jazz.one(Tasks.where(Tasks.id eq a.id))!!.status != TaskStatus.Blocked)
        }
    }

    @Test
    fun text_edits_and_permission_advice(): Unit = runBlocking {
        open("user-6").use { jazz ->
            val me = me(jazz)
            val show = showNamed(jazz, "Premiere")
            val task = addTask(jazz, show, "Rig")
            jazz.update(Tasks, task.id, tier = tools.jazz.WriteTier.Global) { notes("Lights") }
            val read = jazz.one(Tasks.where(Tasks.id eq task.id))!!
            assertEquals("Lights\nBring gaffer tape.", DocsSnippets.editText(jazz, read))
            // An edit of an older value keeps what changed since: both edits survive.
            jazz.editText(Tasks, task.id, Tasks.notes, editedFrom = "Lights", text = "Stage lights")
            val merged = jazz.one(Tasks.where(Tasks.id eq task.id))!!.notes!!
            kotlin.test.assertTrue(merged.contains("Stage lights") && merged.contains("gaffer tape"), merged)
            // Streaming a large value in, and reading part of it back.
            val long = "0123456789".repeat(50_000)
            val preview = DocsSnippets.largeValues(jazz, task.id, long.byteInputStream())
            assertEquals(long.take(80), preview)
            assertEquals(long.length, jazz.one(Tasks.where(Tasks.id eq task.id))!!.notes!!.length)
            // Partial edits: a splice of the title.
            jazz.edit(Tasks, task.id) { splice(Tasks.title, tools.jazz.SpliceUnits.Utf16, 0, 3, tools.jazz.Splice.text(3, 0, "ging")) }
            assertEquals("Rigging", jazz.one(Tasks.where(Tasks.id eq task.id))!!.title)
            // Permission advice: Ada isn't in the show's crew, so assigning her is refused.
            val ada = jazz.insert(CrewTable, NewCrew(account = me, name = "Ada"), tier = tools.jazz.WriteTier.Global)
            assertEquals(true, jazz.canUpdate(Tasks, read.id) { title("Renamed") })
            assertEquals(false, jazz.canUpdate(Tasks, read.id) { assigneeId(ada.id) })
            assertEquals(true, jazz.canInsert(Tasks, jazzapp.NewTask(showId = show.id, title = "New", status = TaskStatus.Todo, rank = 1.0)))
            kotlin.test.assertNotNull(jazz.canDelete(Tasks, read.id))
        }
    }

    @Test
    fun end_users_sign_in_with_device_accounts_and_bearer_tokens(): Unit = runBlocking {
        run {
            // A device account signs up by opening with a new key.
            val carol = DocsSnippets.openClient(endpoint, appId, tools.jazz.JazzAuth.generateDeviceKey())
            carol.use {
                val state = carol.session.value
                kotlin.test.assertTrue(state is tools.jazz.SessionState.SignedIn && state.user.kind == "device", "$state")
                val account = (state as tools.jazz.SessionState.SignedIn).user.account
                val show = carol.insert(
                    jazzapp.Shows,
                    jazzapp.NewShow(name = "Carol's", venue = "V", date = "D", doors = "19:00", chiefAccount = account),
                    tier = tools.jazz.WriteTier.Global,
                )
                assertEquals("Carol's", carol.one(jazzapp.Shows.where(jazzapp.Shows.id eq show.id), tools.jazz.ReadTier.Remote)?.name)
                kotlinx.coroutines.withTimeout(30_000) {
                    carol.syncStatus.first { it.link == tools.jazz.Link.Connected && it.synced }
                }
            }

            // A user the backend signs in, with a bearer token.
            val asked = java.util.Collections.synchronizedList(ArrayList<Boolean>())
            val alice = DocsSnippets.signIn(endpoint, appId) { fresh -> asked += fresh; "token-alice" }
            assertEquals("alice", (alice.session.value as tools.jazz.SessionState.SignedIn).user.subject)
            assertEquals(false, asked.first())
            val states = kotlinx.coroutines.channels.Channel<tools.jazz.SessionState?>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            val watching = launch { alice.session.collect { states.send(it) } }
            // Opening doesn't wait for the server: the snippet reports the
            // status once the link is up and nothing waits for upload.
            kotlinx.coroutines.withTimeout(30_000) {
                alice.syncStatus.first { it.link == tools.jazz.Link.Connected && it.synced }
            }
            assertEquals(listOf("signed in as alice"), DocsSnippets.sessionState(alice).filter { it != "offline" })
            kotlin.test.assertTrue(asked.contains(true), "refreshSession asks for a fresh token")
            kotlinx.coroutines.withTimeout(30_000) { while (states.receive() != tools.jazz.SessionState.SignedOut) Unit }
            watching.cancel()
            kotlin.test.assertFailsWith<tools.jazz.JazzException.Closed> { alice.all(jazzapp.Shows.query()) }
        }
    }

    @Test
    fun hops_unions_history_and_deleted_rows(): Unit = runBlocking {
        open("user-7").use { jazz ->
            val me = me(jazz)
            val show = showNamed(jazz, "Tour")
            val bo = jazz.insert(CrewTable, NewCrew(account = me, name = "Bo"), tier = tools.jazz.WriteTier.Global)
            jazz.insert(
                jazzapp.ShowCrewTable,
                jazzapp.NewShowCrew(showId = show.id, crewId = bo.id, account = me, role = jazzapp.ShowCrewRole.Chief),
                tier = tools.jazz.WriteTier.Global,
            )
            val a = addTask(jazz, show, "Rig")
            val b = addTask(jazz, show, "Pack")
            val c = addTask(jazz, show, "Drive")
            jazz.update(Tasks, a.id, tier = tools.jazz.WriteTier.Global) { assigneeId(bo.id) }
            jazz.update(Tasks, b.id, tier = tools.jazz.WriteTier.Global) { status(TaskStatus.Blocked); assigneeId(bo.id) }
            assertEquals(listOf("Bo"), DocsSnippets.hopTo(jazz, show.id))
            assertEquals(listOf("Pack", "Drive"), DocsSnippets.union(jazz, show.id).map { it.title })
            val versions = DocsSnippets.history(jazz, b.id)
            kotlin.test.assertTrue(versions.size >= 2, "$versions")
            assertEquals(TaskStatus.Blocked, versions.first().row.status)
            kotlin.test.assertTrue(versions.first().acceptedAt >= versions.last().acceptedAt)
            jazz.delete(Tasks, c.id, tier = tools.jazz.WriteTier.Global)
            assertEquals(2, tasksOf(jazz, show).size)
            kotlin.test.assertTrue(DocsSnippets.includeDeleted(jazz, show.id).any { it.id == c.id })
        }
    }

    @Test
    fun a_token_provider_that_throws_doesnt_sign_the_user_out(): Unit = runBlocking {
        var n = 0
        val jazz = openAsUser(endpoint, appId, tools.jazz.JazzAuth.Bearer { if (n++ == 0) "token-tp" else throw java.io.IOException("sign-in server down") })
        jazz.use {
            assertEquals("tp", (jazz.session.value as tools.jazz.SessionState.SignedIn).user.subject)
            runCatching { jazz.refreshSession() }
            kotlin.test.assertTrue(jazz.session.value !is tools.jazz.SessionState.SignedOut, "${jazz.session.value}")
            // Still open, and reading.
            jazz.all(jazzapp.Shows.query())
        }
    }

    @Test
    fun one_users_show_is_invisible_to_another_who_cant_write_into_it(): Unit = runBlocking {
        val (showId, taskId) = open("alice").use { alice ->
            val show = showNamed(alice, "Alice's private show")
            show.id to addTask(alice, show, "Secret setlist").id
        }
        open("bob").use { bob ->
            assertNull(showById(bob, showId))
            assertEquals(emptyList(), bob.all(Tasks.where(Tasks.showId eq showId), tools.jazz.ReadTier.Remote))
            val notHers = jazzapp.Show(showId, "x", "x", "x", "x", me(bob))
            kotlin.test.assertFailsWith<tools.jazz.JazzException.Permission> { addTask(bob, notHers, "Sneaky") }
            // Bob can't read her task, so he can't update it either.
            val refused = runCatching { moveTask(bob, taskId, TaskStatus.Done) }.exceptionOrNull()
            kotlin.test.assertTrue(refused is tools.jazz.JazzException.Permission || refused is tools.jazz.JazzException.Refused, "$refused")
        }
        open("alice").use { alice ->
            kotlin.test.assertNotNull(showById(alice, showId))
            val hers = alice.all(Tasks.where(Tasks.showId eq showId), tools.jazz.ReadTier.Remote)
            assertEquals(listOf("Secret setlist" to TaskStatus.Todo), hers.map { it.title to it.status })
        }
    }

    @Test
    fun lifecycle_disconnect_mutation_errors_delete_storage_and_history_watch(): Unit = runBlocking {
        val me: String
        val closing = open("user-8")
        // Closing the app completes a mutationErrors collection (jazz-ffi's closed notice).
        val ended = async { closing.mutationErrors.toList() }
        kotlinx.coroutines.delay(200)
        closing.close()
        assertEquals(emptyList(), kotlinx.coroutines.withTimeout(30_000) { ended.await() })
        open("user-8").use { jazz ->
            me = me(jazz)
            val show = showNamed(jazz, "Lifecycle")
            // Offline writes sync after reconnecting.
            jazz.disconnect()
            val offline = jazz.insert(Tasks, jazzapp.NewTask(showId = show.id, title = "Offline", status = TaskStatus.Todo, rank = 1.0))
            jazz.reconnect()
            jazz.waitForSync(tools.jazz.WriteTier.Global)
            assertEquals("Offline", jazz.one(Tasks.where(Tasks.id eq offline.id), tools.jazz.ReadTier.Remote)?.title)
            // A local-first write the server refuses arrives as a mutation error.
            val errors = kotlinx.coroutines.channels.Channel<tools.jazz.MutationError>(kotlinx.coroutines.channels.Channel.UNLIMITED)
            val watching = launch { jazz.mutationErrors.collect { errors.send(it) } }
            kotlinx.coroutines.delay(200)
            val stranger = jazz.insert(CrewTable, NewCrew(account = me, name = "Stranger"), tier = tools.jazz.WriteTier.Global)
            jazz.update(Tasks, offline.id) { assigneeId(stranger.id) }
            val error = kotlinx.coroutines.withTimeout(30_000) { errors.receive() }
            kotlin.test.assertTrue(error.denied, "$error")
            watching.cancel()
            // Both transaction kinds commit.
            // (At WriteTier.Global: waitForSync would rethrow the refusal above.)
            val global = tools.jazz.WriteTier.Global
            assertEquals(1, jazz.transaction(global) { tx -> tx.update(Tasks, offline.id) { title("Merged") }; 1 })
            assertEquals(2, jazz.transaction(global, serializable = true) { tx -> tx.update(Tasks, offline.id) { title("Serial") }; 2 })
            // A history query can be watched: each accepted version is a result of its own.
            val versions = kotlinx.coroutines.withTimeout(30_000) {
                jazz.watch(Tasks.where(Tasks.id eq offline.id).history(), tools.jazz.ReadTier.Remote).first { it.size >= 2 }
            }
            kotlin.test.assertTrue(versions.map { it.row.title }.toSet().size >= 2, "$versions")
        }
        // Deleting a persistent client's storage closes it and removes its store.
        val dir = kotlin.io.path.createTempDirectory("jazz-delete").toFile()
        val jazz = openAsUser(endpoint, appId, tools.jazz.JazzAuth.Bearer { "token-user-8" }, dir.path)
        showNamed(jazz, "Lifecycle")
        jazz.deleteClientStorage()
        kotlin.test.assertFailsWith<tools.jazz.JazzException.Closed> { tasksOf(jazz, jazzapp.Show("x", "x", "x", "x", "x", me)) }
        // The store is gone (an empty lock file may stay behind).
        val left = dir.listFiles()?.filter { it.name != "jazz.lock" }.orEmpty()
        kotlin.test.assertTrue(left.isEmpty(), "$left")
    }

    @Test
    fun delete_client_storage_is_refused_while_a_second_app_holds_the_store(): Unit = runBlocking {
        val dir = kotlin.io.path.createTempDirectory("jazz-held").toFile()
        val auth = tools.jazz.JazzAuth.Bearer { "token-user-held" }
        openAsUser(endpoint, appId, auth, dir.path).use { deleting ->
            openAsUser(endpoint, appId, auth, dir.path).use { holder ->
                val show = showNamed(deleting, "Held")
                kotlin.test.assertIs<tools.jazz.SessionState.SignedIn>(deleting.session.value)
                val titles = kotlinx.coroutines.channels.Channel<List<String>>(kotlinx.coroutines.channels.Channel.UNLIMITED)
                val watching = launch {
                    deleting.watch(Tasks.where(Tasks.showId eq show.id)).collect { rows -> titles.send(rows.map { it.title }) }
                }
                assertEquals(emptyList(), kotlinx.coroutines.withTimeout(30_000) { titles.receive() })

                kotlin.test.assertFailsWith<tools.jazz.JazzException.Refused> { deleting.deleteClientStorage() }

                // Both apps are still open, and the watch from before still delivers.
                addTask(deleting, show, "After the refusal")
                kotlinx.coroutines.withTimeout(30_000) { while (titles.receive() != listOf("After the refusal")) Unit }
                kotlin.test.assertIs<tools.jazz.SessionState.SignedIn>(deleting.session.value)
                assertEquals(show.id, showById(holder, show.id)?.id)
                watching.cancel()
            }
            // The other app closed: now it deletes.
            deleting.deleteClientStorage()
            assertEquals(tools.jazz.SessionState.SignedOut, deleting.session.value)
        }
    }

    @Test
    fun delete_client_storage_refused_by_another_process_closes_the_app(): Unit = runBlocking {
        // flock(1), standing in for another process's app: the same shared lock on the store's lock file.
        if (!System.getProperty("os.name").startsWith("Linux")) return@runBlocking
        val dir = kotlin.io.path.createTempDirectory("jazz-held-process").toFile()
        openAsUser(endpoint, appId, tools.jazz.JazzAuth.Bearer { "token-user-other" }, dir.path).use { jazz ->
            kotlin.test.assertIs<tools.jazz.SessionState.SignedIn>(jazz.session.value)
            val lock = dir.walk().single { it.name == "jazz.lock" }
            val holder = ProcessBuilder("flock", "--shared", lock.path, "sh", "-c", "echo held; exec sleep 60").start()
            try {
                assertEquals("held", holder.inputStream.bufferedReader().readLine())
                kotlin.test.assertFailsWith<tools.jazz.JazzException.Refused> { jazz.deleteClientStorage() }
                assertEquals(tools.jazz.SessionState.SignedOut, jazz.session.value)
                kotlin.test.assertFailsWith<tools.jazz.JazzException.Closed> { jazz.all(Tasks.query()) }
            } finally {
                holder.descendants().forEach { it.destroy() }
                holder.destroy()
            }
        }
    }

    @Test
    fun plain_transactions_merge_and_serializable_ones_conflict(): Unit = runBlocking {
        open("user-9").use { jazz ->
            val show = showNamed(jazz, "Concurrent")
            val task = jazz.insert(Tasks, jazzapp.NewTask(showId = show.id, title = "Rig", status = TaskStatus.Todo, rank = 1.0))
            // Two transactions both read the task and update it before either
            // commits: their commits are concurrent.
            suspend fun race(serializable: Boolean?): List<Result<Unit>> {
                val wrote = List(2) { kotlinx.coroutines.CompletableDeferred<Unit>() }
                return List(2) { i ->
                    async {
                        runCatching {
                            val block: suspend (Jazz.Transaction) -> Unit = { tx ->
                                tx.one(Tasks.where(Tasks.id eq task.id))
                                tx.update(Tasks, task.id) { title("Rig $i") }
                                wrote[i].complete(Unit)
                                wrote[1 - i].await()
                            }
                            // null: no argument, so the test pins the default.
                            if (serializable == null) jazz.transaction(block = block)
                            else jazz.transaction(serializable = serializable, block = block)
                        }
                    }
                }.map { it.await() }
            }
            // The default is mergeable (as TS's): both commit.
            val merged = race(serializable = null)
            assertEquals(listOf(true, true), merged.map { it.isSuccess }, "$merged")
            // serializable = true: the second to commit conflicts.
            val serial = race(serializable = true)
            assertEquals(1, serial.count { it.isSuccess }, "$serial")
            kotlin.test.assertIs<tools.jazz.JazzException.Conflict>(serial.single { it.isFailure }.exceptionOrNull())
        }
    }
}
