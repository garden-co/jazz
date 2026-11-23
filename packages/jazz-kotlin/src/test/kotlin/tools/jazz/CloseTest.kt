package tools.jazz

import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** `close()` returns at once; the native core shuts down in the background. */
class CloseTest {
    /** A native core whose shutdown waits for [release], as one waiting on DNS or local work does. */
    private class SlowClose : Native by FakeNative() {
        val release = CountDownLatch(1)
        val closeStarted = CountDownLatch(1)
        @Volatile var closed = false

        override fun close() {
            closeStarted.countDown()
            release.await()
            closed = true
        }
    }

    @Test
    fun close_returns_at_once_and_later_calls_are_closed(): Unit = runBlocking {
        val native = SlowClose()
        Jazz.opener = Native.Opener { native }
        val jazz = Jazz.open("{}", JazzConfig())
        val started = System.nanoTime()
        jazz.close()
        val tookMs = (System.nanoTime() - started) / 1_000_000
        assertTrue(tookMs < 500, "close() blocked for $tookMs ms")
        assertTrue(native.closeStarted.await(5, TimeUnit.SECONDS), "the shutdown never started")
        assertFalse(native.closed)
        assertFailsWith<JazzException.Closed> { jazz.all(Items.query()) }
        assertFailsWith<JazzException.Closed> { jazz.subscribe(Items.query()).collect {} }
        jazz.close() // again: nothing
        native.release.countDown()
        withTimeout(5_000) { jazz.closeAndWait() }
        assertTrue(native.closed)
    }

    @Test
    fun close_and_wait_waits_for_the_shutdown(): Unit = runBlocking {
        val native = SlowClose()
        Jazz.opener = Native.Opener { native }
        val jazz = Jazz.open("{}", JazzConfig())
        // Undispatched: closeAndWait (not close) starts the shutdown before this thread blocks below.
        val waiting = async(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) { jazz.closeAndWait() }
        assertTrue(native.closeStarted.await(5, TimeUnit.SECONDS))
        delay(100)
        assertFalse(waiting.isCompleted, "closeAndWait returned before the shutdown ended")
        native.release.countDown()
        withTimeout(5_000) { waiting.await() }
        assertTrue(native.closed)
    }

    @Test
    fun close_and_wait_throws_what_the_shutdown_threw(): Unit = runBlocking {
        val native = object : Native by FakeNative() {
            override fun close(): Unit = throw JazzException.Io("disk gone")
        }
        Jazz.opener = Native.Opener { native }
        val jazz = Jazz.open("{}", JazzConfig())
        jazz.close() // doesn't throw
        assertEquals("disk gone", assertFailsWith<JazzException.Io> { jazz.closeAndWait() }.message)
    }

    @Test
    fun reopening_the_same_store_waits_for_its_teardown(): Unit = runBlocking {
        val dir = kotlin.io.path.createTempDirectory("jazz-close").toFile()
        val first = SlowClose()
        val opened = AtomicInteger()
        Jazz.opener = Native.Opener { opened.incrementAndGet(); if (opened.get() == 1) first else FakeNative() }
        val config = JazzConfig(storage = Storage.Persistent, dataDir = dir.path)
        val jazz = Jazz.open("{}", config)
        jazz.close()
        assertTrue(first.closeStarted.await(5, TimeUnit.SECONDS))
        // The same directory, written differently: it's the same store.
        val reopening = async { Jazz.open("{}", config.copy(dataDir = dir.path + "/.")) }
        delay(200)
        assertEquals(1, opened.get(), "opened again while the first app still held the store")
        first.release.countDown()
        withTimeout(5_000) { reopening.await() }.close()
        assertEquals(2, opened.get())
        // A memory app, or another directory, never waits.
        val other = SlowClose()
        Jazz.opener = Native.Opener { other }
        Jazz.open("{}", JazzConfig()).close()
        Jazz.opener = Native.Opener { FakeNative() }
        withTimeout(1_000) { Jazz.open("{}", JazzConfig()).close() }
        other.release.countDown()
    }

    @Test
    fun a_closed_app_never_asks_its_bearer_provider(): Unit = runBlocking {
        var tokens: ((String) -> String?)? = null
        val asked = AtomicInteger()
        Jazz.opener = object : Native.Opener {
            override fun open(configJson: String): Native = error("not a bearer open")
            override fun openWithTokens(configJson: String, tokens: (String) -> String?): Native {
                tokensSeen(tokens)
                return FakeNative()
            }
            private fun tokensSeen(given: (String) -> String?) {
                tokens = given
            }
        }
        val jazz = Jazz.open("{}", JazzConfig(serverUrl = "https://app/jazz", auth = JazzAuth.Bearer { asked.incrementAndGet(); "t" }))
        assertEquals("t", tokens!!("renew"))
        jazz.close()
        // The native core's ask after close fails (a failed fetch), never null (a sign-out).
        assertFailsWith<JazzException.Closed> { tokens!!("renew") }
        assertEquals(1, asked.get())
    }

    @Test
    fun deleting_a_store_waits_for_another_apps_teardown(): Unit = runBlocking {
        val dir = kotlin.io.path.createTempDirectory("jazz-close-delete").toFile()
        val first = SlowClose()
        val deleted = java.util.concurrent.atomic.AtomicBoolean(false)
        val second = object : Native by FakeNative() {
            override fun deleteClientStorage() {
                check(first.closed) { "deleted while the other app still held the store" }
                deleted.set(true)
            }
        }
        var opened = 0
        Jazz.opener = Native.Opener { if (opened++ == 0) first else second }
        val config = JazzConfig(storage = Storage.Persistent, dataDir = dir.path)
        val closing = Jazz.open("{}", config)
        val deleting = Jazz.open("{}", config)
        closing.close()
        assertTrue(first.closeStarted.await(5, TimeUnit.SECONDS))
        val deletion = async { deleting.deleteClientStorage() }
        delay(200)
        assertFalse(deleted.get())
        first.release.countDown()
        withTimeout(5_000) { deletion.await() }
        assertTrue(deleted.get())
    }
}
