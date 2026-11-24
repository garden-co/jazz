// One JVM class, SerializersKt, with InputStream.chunks (src/jna): the class
// beta.1's callers (Java, and anything compiled against it) link against.
@file:JvmName("SerializersKt")
@file:JvmMultifileClass

package tools.jazz

import kotlin.jvm.JvmMultifileClass
import kotlin.jvm.JvmName
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

/**
 * For generated `fromStored`/`toStored`: replaces [column]'s value in [row]
 * (read with [from]) by [convert]'s result (written with [to]). A column
 * the row lacks (a `select` left it out) stays out.
 */
public fun <A, B> transformColumn(
    row: MutableMap<String, kotlinx.serialization.json.JsonElement>,
    column: String,
    from: KSerializer<A>,
    to: KSerializer<B>,
    convert: (A) -> B,
) {
    val value = row[column] ?: return
    row[column] = jazzJson.encodeToJsonElement(to, convert(jazzJson.decodeFromJsonElement(from, value)))
}

/**
 * For generated `fromStored`: adds computed field [name] to [row], from
 * the (view) values of the columns [from] names, as [compute] gives it. A
 * row lacking one of those columns (a `select` left it out) gets none.
 */
public fun <R> computeField(
    row: MutableMap<String, kotlinx.serialization.json.JsonElement>,
    name: String,
    result: KSerializer<R>,
    from: List<Pair<String, KSerializer<*>>>,
    compute: (List<Any?>) -> R,
) {
    val args = from.map { (column, serializer) ->
        val value = row[column] ?: return
        jazzJson.decodeFromJsonElement(serializer, value)
    }
    row[name] = jazzJson.encodeToJsonElement(result, compute(args))
}
