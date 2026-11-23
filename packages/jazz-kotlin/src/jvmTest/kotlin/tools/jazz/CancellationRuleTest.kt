package tools.jazz

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * The cancellation rule for every language:
 * - a call whose caller is cancelled before it starts does nothing;
 * - a transaction whose caller is cancelled before its commit starts rolls
 *   back, and a commit that has started stands.
 */
class CancellationRuleTest {
    /** A [FakeNative] that records every call made to it. */
    private class Recording {
        val fake = FakeNative()
        val calls: MutableList<String> = Collections.synchronizedList(ArrayList())
        val native: Native = Proxy.newProxyInstance(Native::class.java.classLoader, arrayOf(Native::class.java)) { _, method, args ->
            calls += method.name
            try {
                method.invoke(fake, *(args ?: emptyArray()))
            } catch (e: InvocationTargetException) {
                throw e.targetException
            }
        } as Native
    }

    /**
     * Runs [call] from a caller already cancelled, and returns the native
     * calls it made, expecting it to throw the caller's cancellation.
     */
    private fun cancelledBefore(call: suspend (Jazz) -> Unit): List<String> {
        val recording = Recording()
        runBlocking {
            Jazz.opener = Native.Opener { recording.native }
            val jazz = Jazz.open("{}", JazzConfig())
            recording.calls.clear()
            var thrown: Throwable? = null
            launch(Dispatchers.Default) {
                currentCoroutineContext().cancel()
                thrown = runCatching { call(jazz) }.exceptionOrNull()
            }.join()
            assertIs<CancellationException>(thrown, "not the caller's cancellation")
        }
        return recording.calls.toList()
    }

    private val query = Items.where(Items.title eq "x")
    private val row = ItemInsert("t", 1.0, ItemKind.C)

    @Test
    fun a_call_cancelled_before_it_starts_does_nothing() {
        val calls = mapOf<String, suspend (Jazz) -> Unit>(
            "all" to { it.all(query) },
            "all at a tier" to { it.all(query, ReadTier.Remote) },
            "one" to { it.one(query) },
            "insert" to { it.insert(Items, row) },
            "insert at a tier" to { it.insert(Items, row, WriteTier.Global) },
            "update" to { it.update(Items, "i") { title("u") } },
            "delete" to { it.delete(Items, "i") },
            "upsert" to { it.upsert(Items, "i", row) },
            "restore" to { it.restore(Items, "i") },
            "editText" to { it.editText(Items, "i", Items.note, "a", "b") },
            "canRead" to { it.canRead(Items, "i") },
            "waitForSync" to { it.waitForSync() },
            "disconnect" to { it.disconnect() },
            "reconnect" to { it.reconnect() },
            "refreshSession" to { it.refreshSession() },
            "link" to { it.link("bearer") },
            "signOut" to { it.signOut() },
            "deleteClientStorage" to { it.deleteClientStorage() },
            "transaction" to { it.transaction { } },
            "insertStreaming" to { it.insertStreaming(Items, row, Items.note, emptyFlow()) },
            "watch" to { it.watch(query).first() },
            "subscribe" to { it.subscribe(query).first() },
            "mutationErrors" to { it.mutationErrors.first() },
        )
        val failures = calls.mapNotNull { (name, call) ->
            val made = cancelledBefore(call)
            if (made.isEmpty()) null else "$name called $made"
        }
        assertEquals(emptyList(), failures)
    }

    @Test
    fun an_open_cancelled_before_it_starts_opens_nothing(): Unit = runBlocking {
        var opened = 0
        Jazz.opener = Native.Opener { opened++; FakeNative() }
        var thrown: Throwable? = null
        launch(Dispatchers.Default) {
            currentCoroutineContext().cancel()
            thrown = runCatching { Jazz.open("{}", JazzConfig()) }.exceptionOrNull()
        }.join()
        assertIs<CancellationException>(thrown)
        assertEquals(0, opened)
    }

    /** A native transaction that records how it ended. */
    private class Ended : NativeTransaction {
        val ends: MutableList<String> = Collections.synchronizedList(ArrayList())
        val committing = CountDownLatch(1)
        val release = CountDownLatch(1)

        @Volatile var holdCommit = false

        override fun all(queryJson: String): String = "[]"
        override fun one(queryJson: String): String? = null
        override fun insert(table: String, rowJson: String): String = """{"id":"i","title":"t","rank":1,"kind":"c"}"""
        override fun update(table: String, id: String, patchJson: String) {}
        override fun delete(table: String, id: String) {}
        override fun commit(tier: String?) {
            committing.countDown()
            if (holdCommit) check(release.await(5, TimeUnit.SECONDS))
            ends += "commit"
        }
        override fun rollback() {
            ends += "rollback"
        }
    }

    private suspend fun openWith(tx: NativeTransaction): Jazz {
        val native = object : Native by FakeNative() {
            override fun begin(): NativeTransaction = tx
            override fun beginMergeable(): NativeTransaction = tx
        }
        Jazz.opener = Native.Opener { native }
        return Jazz.open("{}", JazzConfig())
    }

    @Test
    fun a_transaction_cancelled_before_its_commit_starts_rolls_back() {
        for (serializable in listOf(false, true)) {
            val tx = Ended()
            var thrown: Throwable? = null
            runBlocking {
                val jazz = openWith(tx)
                launch(Dispatchers.Default) {
                    thrown = runCatching {
                        jazz.transaction(serializable = serializable) { t ->
                            t.insert(Items, row)
                            // Cancelled with no suspension point left in the block.
                            currentCoroutineContext().cancel()
                        }
                    }.exceptionOrNull()
                }.join()
            }
            assertIs<CancellationException>(thrown, "serializable=$serializable")
            assertEquals(listOf("rollback"), tx.ends, "serializable=$serializable")
        }
    }

    @Test
    fun a_commit_that_has_started_stands_when_its_caller_is_cancelled() {
        val tx = Ended().apply { holdCommit = true }
        var returned = false
        runBlocking {
            val jazz = openWith(tx)
            val caller = launch(Dispatchers.Default) {
                jazz.transaction { t -> t.insert(Items, row) }
                returned = true
            }
            assertTrue(tx.committing.await(5, TimeUnit.SECONDS), "the commit never started")
            caller.cancel()
            tx.release.countDown()
            caller.join()
        }
        assertEquals(listOf("commit"), tx.ends)
        // Committed: the caller isn't told it failed.
        assertTrue(returned, "a committed transaction threw its caller's cancellation")
    }
}
