package tools.jazz

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A caller cancelled around a native call that returns a handle never gets
 * it, so it has to be given back: withContext drops its result then.
 */
class CancellationTest {
    /** A native call that makes a handle, and says when it runs and when it returned. */
    private class Made {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val returned = CountDownLatch(1)

        @Volatile var givenBack = 0

        fun <T> make(handle: () -> T): T {
            entered.countDown()
            check(release.await(5, TimeUnit.SECONDS))
            return handle().also { returned.countDown() }
        }
    }

    private enum class Cancelled { DuringTheCall, BeforeTheCallerResumes }

    /**
     * Runs [call] cancelled in each window, over a fresh [Made], and expects
     * its handle given back once.
     */
    private fun givesBack(call: suspend (Made) -> Unit) {
        for (window in Cancelled.entries) {
            val made = Made()
            runBlocking {
                when (window) {
                    Cancelled.DuringTheCall -> {
                        val caller = launch(Dispatchers.Default) { call(made) }
                        assertTrue(made.entered.await(5, TimeUnit.SECONDS), "$window: no native call")
                        caller.cancel()
                        made.release.countDown()
                        caller.join()
                    }
                    Cancelled.BeforeTheCallerResumes -> {
                        // The caller is on this thread, which waits blocked: it can't
                        // take the result before it is cancelled.
                        val caller = launch { call(made) }
                        while (made.entered.count > 0) yield()
                        made.release.countDown()
                        assertTrue(made.returned.await(5, TimeUnit.SECONDS), "$window: the call didn't return")
                        Thread.sleep(50)
                        caller.cancel()
                        caller.join()
                    }
                }
            }
            assertEquals(1, made.givenBack, "$window")
        }
    }

    private suspend fun open(native: Native): Jazz {
        Jazz.opener = Native.Opener { native }
        return Jazz.open("{}", JazzConfig())
    }

    @Test
    fun a_cancelled_open_closes_the_app_it_opened(): Unit = givesBack { made ->
        val native = object : Native by FakeNative() {
            override fun close() {
                made.givenBack++
            }
        }
        Jazz.opener = Native.Opener { made.make { native } }
        Jazz.open("{}", JazzConfig())
    }

    @Test
    fun a_cancelled_bearer_open_closes_the_app_it_opened(): Unit = givesBack { made ->
        val native = object : Native by FakeNative() {
            override fun close() {
                made.givenBack++
            }
        }
        Jazz.opener = object : Native.Opener {
            override fun open(configJson: String): Native = error("not a bearer open")
            override fun openWithTokens(configJson: String, tokens: (String) -> String?): Native = made.make { native }
        }
        Jazz.open("{}", JazzConfig(serverUrl = "https://app/jazz", auth = JazzAuth.Bearer { "t" }))
    }

    @Test
    fun a_cancelled_begin_rolls_the_transaction_back() {
        for (serializable in listOf(false, true)) {
            givesBack { made ->
                val tx = object : NativeTransaction {
                    override fun all(queryJson: String): String = "[]"
                    override fun one(queryJson: String): String? = null
                    override fun insert(table: String, rowJson: String): String = error("the block never runs")
                    override fun update(table: String, id: String, patchJson: String): Unit = error("the block never runs")
                    override fun delete(table: String, id: String): Unit = error("the block never runs")
                    override fun commit(tier: String?): Unit = error("a cancelled begin committed")
                    override fun rollback() {
                        made.givenBack++
                    }
                }
                val native = object : Native by FakeNative() {
                    override fun begin(): NativeTransaction = made.make { tx }
                    override fun beginMergeable(): NativeTransaction = made.make { tx }
                }
                open(native).transaction(serializable = serializable) { }
            }
        }
    }

    @Test
    fun a_cancelled_begin_upload_aborts_the_upload() {
        val starts = listOf<suspend (Jazz) -> Unit>(
            { it.insertStreaming(Items, ItemInsert("t", 1.0, ItemKind.C), Items.note, emptyFlow()) },
            { it.updateStreaming(Items, "i", Items.note, emptyFlow()) },
            { it.upsertStreaming(Items, "i", ItemInsert("t", 1.0, ItemKind.C), Items.note, emptyFlow()) },
        )
        for (start in starts) {
            givesBack { made ->
                val upload = object : NativeUpload {
                    override fun push(bytes: ByteArray) {}
                    override fun finish(): String = error("a cancelled upload finished")
                    override fun abort() {
                        made.givenBack++
                    }
                }
                val native = object : Native by FakeNative() {
                    override fun beginUpload(table: String, column: String, kind: String, id: String?, rowJson: String?): NativeUpload =
                        made.make { upload }
                }
                start(open(native))
            }
        }
    }

    @Test
    fun cancelling_while_starting_to_watch_mutation_errors_stops_the_watch(): Unit = givesBack { made ->
        val native = object : Native by FakeNative() {
            override fun watchMutationErrors(onError: (String) -> Unit): () -> Unit = made.make { { made.givenBack++; Unit } }
        }
        open(native).mutationErrors.collect { }
    }

    @Test
    fun an_upload_cancelled_while_it_finishes_is_still_written() {
        // As the streaming writes' KDoc says: once [data] has completed, cancelling doesn't undo the write.
        val starts = listOf<suspend (Jazz) -> Unit>(
            { it.insertStreaming(Items, ItemInsert("t", 1.0, ItemKind.C), Items.note, emptyFlow()) },
            { it.updateStreaming(Items, "i", Items.note, emptyFlow()) },
            { it.upsertStreaming(Items, "i", ItemInsert("t", 1.0, ItemKind.C), Items.note, emptyFlow()) },
        )
        for (start in starts) {
            val finishing = CountDownLatch(1)
            val release = CountDownLatch(1)
            var written = false
            var aborted = false
            val upload = object : NativeUpload {
                override fun push(bytes: ByteArray) {}
                override fun finish(): String {
                    finishing.countDown()
                    check(release.await(5, TimeUnit.SECONDS))
                    written = true
                    return """{"id":"i","title":"t","rank":1,"kind":"c"}"""
                }
                override fun abort() {
                    // Nothing is left to abort by then, as with jazz-ffi's.
                    if (!written) aborted = true
                }
            }
            val native = object : Native by FakeNative() {
                override fun beginUpload(table: String, column: String, kind: String, id: String?, rowJson: String?): NativeUpload = upload
            }
            runBlocking {
                val jazz = open(native)
                var returned = false
                val caller = launch(Dispatchers.Default) {
                    start(jazz)
                    returned = true
                }
                assertTrue(finishing.await(5, TimeUnit.SECONDS), "finish never ran")
                caller.cancel()
                release.countDown()
                caller.join()
                assertTrue(caller.isCancelled && !returned, "the caller got the row")
            }
            assertTrue(written, "not written")
            assertEquals(false, aborted)
        }
    }

    @Test
    fun a_transaction_cancelled_during_its_block_rolls_back() {
        // However the block ends after the cancel (the rule since beta.1).
        val blocks = listOf<Triple<String, String, suspend () -> Unit>>(
            Triple("suspends", "rolled back", { kotlinx.coroutines.delay(10) }),
            Triple("only computes", "rolled back", { Thread.sleep(10) }),
            Triple("returns at once", "rolled back", { }),
        )
        for ((name, expected, afterCancel) in blocks) {
            var ended = "neither"
            val tx = object : NativeTransaction {
                override fun all(queryJson: String): String = "[]"
                override fun one(queryJson: String): String? = null
                override fun insert(table: String, rowJson: String): String = error("the block writes nothing")
                override fun update(table: String, id: String, patchJson: String): Unit = error("the block writes nothing")
                override fun delete(table: String, id: String): Unit = error("the block writes nothing")
                override fun commit(tier: String?) {
                    ended = "committed"
                }
                override fun rollback() {
                    ended = "rolled back"
                }
            }
            val native = object : Native by FakeNative() {
                override fun beginMergeable(): NativeTransaction = tx
            }
            runBlocking {
                val jazz = open(native)
                val caller = launch {
                    jazz.transaction {
                        // Cancelled inside the block, so before any commit starts.
                        currentCoroutineContext().cancel()
                        afterCancel()
                    }
                }
                caller.join()
                assertTrue(caller.isCancelled, "$name: the caller wasn't cancelled")
            }
            assertEquals(expected, ended, "a block that $name")
        }
    }

    @Test
    fun an_upload_cancelled_before_its_data_completes_writes_nothing() {
        // However the data ends after the cancel: by suspending, by emitting, or by just completing.
        val rests = listOf<Pair<String, suspend FlowCollector<ByteArray>.() -> Unit>>(
            "suspends" to { kotlinx.coroutines.delay(10) },
            "emits" to { emit(byteArrayOf(1)) },
            "completes" to { },
        )
        val starts = listOf<suspend (Jazz, Flow<ByteArray>) -> Unit>(
            { jazz, data -> jazz.insertStreaming(Items, ItemInsert("t", 1.0, ItemKind.C), Items.note, data) },
            { jazz, data -> jazz.updateStreaming(Items, "i", Items.note, data) },
            { jazz, data -> jazz.upsertStreaming(Items, "i", ItemInsert("t", 1.0, ItemKind.C), Items.note, data) },
        )
        for ((name, rest) in rests) {
            for (start in starts) {
                var ended = "neither"
                val upload = object : NativeUpload {
                    override fun push(bytes: ByteArray) {}
                    override fun finish(): String {
                        ended = "written"
                        return """{"id":"i","title":"t","rank":1,"kind":"c"}"""
                    }
                    override fun abort() {
                        if (ended != "written") ended = "aborted"
                    }
                }
                val native = object : Native by FakeNative() {
                    override fun beginUpload(table: String, column: String, kind: String, id: String?, rowJson: String?): NativeUpload = upload
                }
                runBlocking {
                    val jazz = open(native)
                    val data = flow {
                        currentCoroutineContext().cancel()
                        rest()
                    }
                    launch { start(jazz, data) }.join()
                }
                assertEquals("aborted", ended, "data that $name")
            }
        }
    }

    // `subscribe` already gave its handle back: the control for the tests above.
    @Test
    fun cancelling_while_subscribing_unsubscribes(): Unit = givesBack { made ->
        val native = object : Native by FakeNative() {
            override fun subscribe(queryJson: String, onDelta: (String) -> Unit, onError: (String, String) -> Unit): () -> Unit =
                made.make { { made.givenBack++; Unit } }
        }
        open(native).subscribe(Items.query()).collect { }
    }
}
