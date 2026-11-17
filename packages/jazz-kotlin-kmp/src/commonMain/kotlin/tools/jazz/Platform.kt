package tools.jazz

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.SendChannel
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

// What each platform provides: the KMP spike's expect/actual seam. Everything
// else in commonMain is jazz-kotlin's code with these swapped in for the
// java.* and kotlinx-coroutines-JVM uses.

/** Where blocking native calls run by default: `Dispatchers.IO` on the JVM and Kotlin/Native. */
internal expect val ioDispatcher: CoroutineDispatcher

/** How [Jazz.open] reaches jazz-ffi: UniFFI (JNA) on the JVM, the C ABI (cinterop) on Kotlin/Native. */
internal expect fun platformOpener(): Native.Opener

/** A new device account's key, from jazz-ffi. */
internal expect fun deviceKeys(): String

/** Runs [block] to completion on this (native) thread: a token callback's bridge into a `suspend` provider. */
internal expect fun <T> blockOn(block: suspend () -> T): T

/** Sends [element], blocking this (native) thread while [channel] is full; never drops. */
internal expect fun <E> sendBlocking(channel: SendChannel<E>, element: E)

/** jazz-kotlin's `CopyOnWriteArrayList` of cancels, in common code. */
@OptIn(ExperimentalAtomicApi::class)
internal class Watches {
    private val list = AtomicReference<List<() -> Unit>>(emptyList())

    operator fun plusAssign(cancel: () -> Unit) {
        while (true) {
            val old = list.load()
            if (list.compareAndSet(old, old + cancel)) return
        }
    }

    fun forEach(action: (() -> Unit) -> Unit) = list.load().forEach(action)

    fun clear() = list.store(emptyList())
}
