@file:OptIn(ExperimentalForeignApi::class, ObsoleteWorkersApi::class)

package tools.jazz

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.runBlocking
import kotlin.native.concurrent.ObsoleteWorkersApi
import kotlin.native.concurrent.Worker

// The platform seam's actuals for Kotlin/Native (the experimental iOS targets,
// -PjazzIos=true): jazz-ffi through its C ABI (CAbiNative), not UniFFI.

internal actual val ioDispatcher: CoroutineDispatcher get() = Dispatchers.IO

internal actual fun platformOpener(): Native.Opener = object : Native.Opener {
    override fun open(configJson: String): Native = CAbiNative.open(configJson)

    override fun openWithTokens(configJson: String, tokens: (String) -> String?): Native =
        CAbiNative.openWithTokens(configJson, tokens)
}

internal actual fun deviceKeys(): String = CAbiNative.generateDeviceKey()

/** realpath(3) when [path] exists; otherwise [path] itself (the JVM's canonicalPath also accepts missing paths). */
internal actual fun canonicalPath(path: String): String {
    val resolved = platform.posix.realpath(path, null) ?: return path
    return try {
        resolved.toKString()
    } finally {
        platform.posix.free(resolved)
    }
}

internal actual fun <T> blockOn(block: suspend () -> T): T = runBlocking { block() }

internal actual fun <E> SendChannel<E>.trySendBlockingCompat(element: E): ChannelResult<Unit> = trySendBlocking(element)

/** A worker of its own, which ends after [block]. */
internal actual fun startThread(name: String, block: () -> Unit) {
    val worker = Worker.start(name = name)
    worker.executeAfter(0L) {
        try {
            block()
        } finally {
            worker.requestTermination(processScheduledJobs = false)
        }
    }
}

/** Kotlin/Native can't set a cause after construction: the throwable keeps its own. */
internal actual fun Throwable.initCauseCompat(cause: Throwable) {}
