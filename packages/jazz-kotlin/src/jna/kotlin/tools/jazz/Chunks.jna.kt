// In SerializersKt, beside the commonMain part (Serializers.kt), as in beta.1.
@file:JvmName("SerializersKt")
@file:JvmMultifileClass

package tools.jazz

import kotlinx.coroutines.flow.flowOn

/** This stream's bytes as a [kotlinx.coroutines.flow.Flow] of chunks of [size], read on [kotlinx.coroutines.Dispatchers.IO]; closes it at the end. */
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
    }.flowOn(kotlinx.coroutines.Dispatchers.IO)
