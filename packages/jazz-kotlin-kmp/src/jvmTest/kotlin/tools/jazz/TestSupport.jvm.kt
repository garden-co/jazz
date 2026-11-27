package tools.jazz

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking

actual fun <T> runBlockingTest(block: suspend CoroutineScope.() -> T): T = runBlocking(block = block)
