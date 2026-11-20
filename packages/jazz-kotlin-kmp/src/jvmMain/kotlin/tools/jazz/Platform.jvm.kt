package tools.jazz

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.runBlocking

internal actual val ioDispatcher: CoroutineDispatcher get() = Dispatchers.IO

// As in jazz-kotlin: UniffiNative is compiled from generated sources, so it's
// reached by reflection and the rest builds without them (-PjazzFfi=skip).
internal actual fun platformOpener(): Native.Opener = object : Native.Opener {
    override fun open(configJson: String): Native = openReflectively("tools.jazz.UniffiNative", configJson)

    override fun openWithTokens(configJson: String, tokens: (String) -> String?): Native =
        openReflectively("tools.jazz.UniffiNative", configJson, tokens)
}

internal actual fun deviceKeys(): String {
    val native = Class.forName("tools.jazz.UniffiNative")
    return try {
        native.getMethod("generateDeviceKey").invoke(null) as String
    } catch (e: java.lang.reflect.InvocationTargetException) {
        throw e.targetException
    }
}

internal actual fun <T> blockOn(block: suspend () -> T): T = runBlocking { block() }

internal actual fun <E> sendBlocking(channel: SendChannel<E>, element: E) {
    channel.trySendBlocking(element)
}

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

/** This stream's bytes as a [kotlinx.coroutines.flow.Flow] of chunks of [size], read on [Dispatchers.IO]; closes it at the end. JVM only. */
public fun java.io.InputStream.chunks(size: Int = 256 * 1024): kotlinx.coroutines.flow.Flow<ByteArray> =
    kotlinx.coroutines.flow.flow {
        use { input ->
            val buffer = ByteArray(size)
            while (true) {
                val n = input.read(buffer)
                if (n < 0) break
                if (n > 0) emit(buffer.copyOf(n))
            }
        }
    }.flowOn(Dispatchers.IO)
