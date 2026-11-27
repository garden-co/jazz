package tools.jazz

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * A cancelled caller (or `withTimeout`) cuts its native call short through
 * [Native.newCall]: the wait ends at once instead of when the server answers.
 */
class CancellableWaitTest {
    /** A native core whose waits last until their call is cancelled. */
    private class Waiting(private val queued: Boolean = false) : Native by FakeNative() {
        @Volatile var cancels = 0
        private val current = ThreadLocal<CountDownLatch?>()

        override fun newCall(): NativeCall {
            val cancelled = CountDownLatch(1)
            return object : NativeCall {
                override fun <T> run(block: () -> T): T {
                    current.set(cancelled)
                    try {
                        return block()
                    } finally {
                        current.remove()
                    }
                }

                override fun cancel() {
                    cancels++
                    cancelled.countDown()
                }
            }
        }

        private fun waitForCancel(): Nothing {
            val cancelled = checkNotNull(current.get()) { "called outside its call" }
            check(cancelled.await(10, TimeUnit.SECONDS)) { "never cancelled" }
            throw if (queued) JazzException.StillQueued("committed locally") else CancellationException("cut short")
        }

        override fun waitForSync(tier: String): Unit = waitForCancel()

        override fun insertAt(table: String, rowJson: String, tier: String): String = waitForCancel()
    }

    private suspend fun open(native: Native): Jazz {
        Jazz.opener = Native.Opener { native }
        return Jazz.open("{}", JazzConfig())
    }

    private fun elapsedMs(since: Long) = (System.nanoTime() - since) / 1_000_000

    @Test
    fun with_timeout_ends_a_wait(): Unit = runBlocking {
        val native = Waiting()
        val jazz = open(native)
        val started = System.nanoTime()
        val thrown = runCatching { withTimeout(200) { jazz.waitForSync() } }.exceptionOrNull()
        assertIs<TimeoutCancellationException>(thrown)
        assertTrue(elapsedMs(started) < 2_000, "returned after ${elapsedMs(started)} ms")
        assertEquals(1, native.cancels)
    }

    @Test
    fun cancelling_ends_a_wait(): Unit = runBlocking {
        val native = Waiting()
        val jazz = open(native)
        val caller = launch(Dispatchers.Default) { jazz.waitForSync() }
        delay(100)
        val started = System.nanoTime()
        caller.cancel()
        caller.join()
        assertTrue(elapsedMs(started) < 2_000, "returned after ${elapsedMs(started)} ms")
        assertTrue(caller.isCancelled)
        assertEquals(1, native.cancels)
    }

    @Test
    fun a_write_cut_short_after_it_committed_locally_says_it_is_still_queued(): Unit = runBlocking {
        val jazz = open(Waiting(queued = true))
        val thrown = runCatching {
            withTimeout(200) { jazz.insert(Items, ItemInsert("t", 1.0, ItemKind.C), WriteTier.Global) }
        }.exceptionOrNull()
        // Not the timeout: the write may still reach the server, so don't retry it.
        assertIs<JazzException.StillQueued>(thrown)
        // The timeout is its cause (kotlinx's stack-trace recovery may wrap it in a copy first).
        assertTrue(generateSequence(thrown.cause) { it.cause }.any { it is TimeoutCancellationException }, "${thrown.cause}")
        // Nor does withTimeoutOrNull turn it into null.
        val orNull = runCatching {
            kotlinx.coroutines.withTimeoutOrNull(200) { jazz.insert(Items, ItemInsert("t", 1.0, ItemKind.C), WriteTier.Global) }
        }.exceptionOrNull()
        assertIs<JazzException.StillQueued>(orNull)
    }

    @Test
    fun a_call_nobody_cancels_isnt_cut_short(): Unit = runBlocking {
        val native = Waiting()
        val jazz = open(native)
        assertEquals(emptyList(), jazz.all(Items.where(Items.title eq "x")))
        assertEquals(0, native.cancels)
    }
}
