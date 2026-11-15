package tools.jazz

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

/** `bigint` columns: a [Long], sent as a decimal string. */
public object JazzBigInt : KSerializer<Long> {
    override val descriptor: kotlinx.serialization.descriptors.SerialDescriptor = PrimitiveSerialDescriptor("tools.jazz.BigInt", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: Long): Unit = encoder.encodeString(value.toString())

    override fun deserialize(decoder: Decoder): Long = decoder.decodeString().toLong()
}

/** `bytea` columns: a [ByteArray], sent as base64. */
@OptIn(ExperimentalEncodingApi::class)
public object JazzBytes : KSerializer<ByteArray> {
    override val descriptor: kotlinx.serialization.descriptors.SerialDescriptor = PrimitiveSerialDescriptor("tools.jazz.Bytes", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: ByteArray): Unit = encoder.encodeString(Base64.encode(value))

    override fun deserialize(decoder: Decoder): ByteArray = Base64.decode(decoder.decodeString())
}

/** [serializer] with `null` as a value too: what generated bindings use for optional columns. */
public fun <T : Any> nullable(serializer: KSerializer<T>): KSerializer<T?> = serializer.nullable
