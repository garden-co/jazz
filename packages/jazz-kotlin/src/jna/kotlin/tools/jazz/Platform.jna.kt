package tools.jazz

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.runBlocking

// The platform seam's actuals for the JVM and Android (compiled into both
// targets; see build.gradle.kts).

internal actual val ioDispatcher: CoroutineDispatcher get() = Dispatchers.IO

internal actual fun platformOpener(): Native.Opener = object : Native.Opener {
    override fun open(configJson: String): Native = openReflectively("tools.jazz.UniffiNative", configJson)

    override fun openWithTokens(configJson: String, tokens: (String) -> String?): Native =
        openReflectively("tools.jazz.UniffiNative", configJson, tokens)
}

/**
 * `className.open(config)`, by reflection: UniffiNative is compiled from the
 * sources uniffi-bindgen generates for jazz-ffi (see build.gradle.kts), so
 * the rest of the library builds without them. What `open` throws is
 * rethrown as it is, not wrapped.
 */
internal fun openReflectively(className: String, config: String, tokens: ((String) -> String?)? = null): Native {
    val native = Class.forName(className)
    return try {
        if (tokens == null) {
            native.getMethod("open", String::class.java).invoke(null, config) as Native
        } else {
            native.getMethod("openWithTokens", String::class.java, Function1::class.java).invoke(null, config, tokens) as Native
        }
    } catch (e: java.lang.reflect.InvocationTargetException) {
        throw e.targetException
    }
}

internal actual fun deviceKeys(): String {
    val native = Class.forName("tools.jazz.UniffiNative")
    return try {
        native.getMethod("generateDeviceKey").invoke(null) as String
    } catch (e: java.lang.reflect.InvocationTargetException) {
        throw e.targetException
    }
}

internal actual fun canonicalPath(path: String): String = java.io.File(path).canonicalPath

internal actual fun <T> blockOn(block: suspend () -> T): T = runBlocking { block() }

internal actual fun <E> SendChannel<E>.trySendBlockingCompat(element: E): ChannelResult<Unit> = trySendBlocking(element)

internal actual fun Throwable.initCauseCompat(cause: Throwable) {
    initCause(cause)
}

internal actual fun startThread(name: String, block: () -> Unit) {
    Thread(block, name).apply { isDaemon = false }.start()
}
