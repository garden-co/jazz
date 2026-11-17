package tools.jazz

import jazzapp.NewNote
import jazzapp.Notes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * One common test against the real jazz-ffi: through UniFFI/JNA on the JVM,
 * through cinterop on the C ABI on Kotlin/Native. The app is jazz-codegen's
 * `notes` golden (crates/jazz-codegen/tests/apps/notes/JazzApp.kt), compiled
 * as it is in common code.
 */
class LocalAppTest {
    private fun test(body: suspend CoroutineScope.() -> Unit) = runTest {
        // JazzTest swaps in a FakeNative opener; this test is about the real one.
        Jazz.opener = platformOpener()
        // Real time: deltas come from jazz-ffi's own threads.
        withContext(Dispatchers.Default) { withTimeout(30_000) { coroutineScope { body() } } }
    }

    @Test
    fun opens_a_local_app_inserts_queries_and_subscribes() = test {
        val jazz = jazzapp.open(JazzConfig())
        try {
            val first = jazz.insert(Notes, NewNote(body = "first"))
            assertEquals("first", first.body)
            assertEquals(false, first.pinned) // the schema's default
            assertEquals(listOf("first"), jazz.all(Notes.query()).map { it.body })

            val seen = Channel<List<String>>(Channel.UNLIMITED)
            val watching = launch { jazz.watch(Notes.orderBy(Notes.body.asc())).collect { seen.send(it.map { n -> n.body }) } }
            assertEquals(listOf("first"), seen.receive())

            val second = jazz.insert(Notes, NewNote(body = "a second", pinned = true))
            assertEquals(listOf("a second", "first"), seen.receive())
            jazz.update(Notes, first.id) { body("first, edited") }
            assertEquals(listOf("a second", "first, edited"), seen.receive())
            jazz.delete(Notes, second.id)
            assertEquals(listOf("first, edited"), seen.receive())
            watching.cancel()

            assertEquals(true, jazz.one(Notes.where(Notes.id eq first.id))!!.body == "first, edited")
            assertNull(jazz.one(Notes.where(Notes.id eq second.id)))
            assertEquals(true, jazz.subscribe(Notes.query()).first().reset)

            val inTx = jazz.transaction { tx -> tx.insert(Notes, NewNote(body = "in a transaction")) }
            assertEquals("in a transaction", jazz.one(Notes.where(Notes.id eq inTx.id))!!.body)
        } finally {
            jazz.close()
        }
        assertFailsWith<JazzException.Closed> { jazz.all(Notes.query()) }
    }

    @Test
    fun closing_ends_open_subscriptions_with_closed() = test {
        val jazz = jazzapp.open(JazzConfig())
        val ended = Channel<Throwable?>(1)
        val job = launch {
            try {
                jazz.subscribe(Notes.query()).collect {}
                ended.send(null)
            } catch (e: JazzException) {
                ended.send(e)
            }
        }
        jazz.insert(Notes, NewNote(body = "x"))
        // Let the subscription start before closing.
        jazz.subscribe(Notes.query()).first()
        jazz.close()
        val e = ended.receive()
        assertTrue(e is JazzException.Closed, "ended with $e")
        job.cancel()
    }

    @Test
    fun errors_come_back_typed() = test {
        val jazz = jazzapp.open(JazzConfig())
        try {
            val e = assertFailsWith<JazzException> { jazz.delete(Notes, "not-a-uuid") }
            println("a bad id fails with ${e::class.simpleName}: ${e.message}")
        } finally {
            jazz.close()
        }
    }
}
