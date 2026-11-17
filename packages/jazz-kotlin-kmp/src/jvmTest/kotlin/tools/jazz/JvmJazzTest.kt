package tools.jazz

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** jazz-kotlin's JazzTest cases that use JVM APIs (reflection, CountDownLatch). */
class JvmJazzTest {
    private suspend fun open(fake: FakeNative = FakeNative()): Jazz {
        Jazz.opener = Native.Opener { fake }
        return Jazz.open("{}", JazzConfig())
    }

    @Test
    fun open_errors_are_not_wrapped() {
        val error = assertFailsWith<JazzException.Refused> { openReflectively(RefusingNative::class.java.name, "{}") }
        assertEquals("no app", error.message)
    }


    @Test
    fun cancelling_while_subscribing_still_unsubscribes(): Unit = runBlocking {
        val fake = FakeNative()
        val gate = java.util.concurrent.CountDownLatch(1)
        fake.beforeSubscribe = { gate.await() }
        val jazz = open(fake)
        val job = launch(Dispatchers.Default) { jazz.subscribe(Items.query()).collect {} }
        // The collector is cancelled while the native subscribe is still running.
        withTimeout(5_000) { while (fake.subs.isEmpty() && fake.errorCallbacks.isEmpty()) { delay(5); if (gate.count == 1L) break } }
        job.cancel()
        gate.countDown()
        job.join()
        withTimeout(5_000) { while (fake.subs.isNotEmpty()) delay(10) }
    }

}

object RefusingNative {
    @JvmStatic
    fun open(config: String): Native = throw JazzException.Refused("no app")

}
