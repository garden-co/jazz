package tools.jazz

import kotlin.concurrent.atomics.AtomicReference
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.SendChannel

// The platform seam: what commonMain needs from the JVM and Android (their
// actuals are src/jna/kotlin/tools/jazz/Platform.jna.kt, compiled into both).

/** Where blocking native calls run by default: `Dispatchers.IO`. */
internal expect val ioDispatcher: CoroutineDispatcher

/** How [Jazz.open] reaches jazz-ffi by default: UniffiNative, by reflection. */
internal expect fun platformOpener(): Native.Opener

/** A fresh device key ([JazzAuth.device]), from jazz-ffi. */
internal expect fun deviceKeys(): String

/** [path], canonical: the same directory always gives the same string. */
internal expect fun canonicalPath(path: String): String

/** Runs [block] to its end, blocking this thread (only on a thread of the native core's own). */
internal expect fun <T> blockOn(block: suspend () -> T): T

/** Sends [element], blocking this thread while the channel is full. */
internal expect fun <E> SendChannel<E>.trySendBlockingCompat(element: E): ChannelResult<Unit>

/** Runs [block] on a new thread named [name], not a daemon: a program that ends meanwhile waits for it. */
internal expect fun startThread(name: String, block: () -> Unit)

/** Sets this to [transform] of its value, retrying while another thread changes it meanwhile. */
internal inline fun <T> AtomicReference<T>.updateAtomic(transform: (T) -> T) {
    while (true) {
        val old = load()
        if (compareAndSet(old, transform(old))) return
    }
}

/** [cause] as this throwable's cause, where the platform allows setting it after construction (the JVM's initCause). */
internal expect fun Throwable.initCauseCompat(cause: Throwable)
