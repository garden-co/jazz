@file:OptIn(ExperimentalAtomicApi::class)

package tools.jazz

import kotlinx.coroutines.CoroutineScope
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/** `runBlocking`, which kotlinx-coroutines has on the JVM and Kotlin/Native but not in common code. */
expect fun <T> runBlockingTest(block: suspend CoroutineScope.() -> T): T

/** A thread-safe list for the tests (jazz-kotlin's `Collections.synchronizedList`). */
class ConcurrentList<T> {
    private val list = AtomicReference<List<T>>(emptyList())

    private inline fun update(f: (List<T>) -> List<T>) {
        while (true) {
            val old = list.load()
            if (list.compareAndSet(old, f(old))) return
        }
    }

    operator fun plusAssign(item: T) = update { it + item }
    fun remove(item: T) = update { it - item }
    fun toList(): List<T> = list.load()
    fun isEmpty(): Boolean = list.load().isEmpty()
    fun isNotEmpty(): Boolean = !isEmpty()
}
