package tools.jazz

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.runBlocking

internal actual val ioDispatcher: CoroutineDispatcher get() = Dispatchers.IO

internal actual fun platformOpener(): Native.Opener = object : Native.Opener {
    override fun open(configJson: String): Native = CAbiNative.open(configJson)

    override fun openWithTokens(configJson: String, tokens: (String) -> String?): Native =
        CAbiNative.openWithTokens(configJson, tokens)
}

internal actual fun deviceKeys(): String = CAbiNative.generateDeviceKey()

internal actual fun <T> blockOn(block: suspend () -> T): T = runBlocking { block() }

internal actual fun <E> sendBlocking(channel: SendChannel<E>, element: E) {
    channel.trySendBlocking(element)
}
