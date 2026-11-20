package tools.jazz

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.util.Base64

// End-to-end encryption for an app that declares it. A device keeps its keys
// in a private store the app supplies ([JazzEncryption.store]): Jazz ships
// none. On Android, the Android Keystore protects the secret that protects it.

/** One document of a [PrivateStore], with its revision. */
public class StoredDocument(public val bytes: ByteArray, public val revision: Long)

/** What [PrivateStore.save] throws when nothing was written. */
public sealed class PrivateStoreException(message: String) : Exception(message) {
    /** The document's revision is not the one expected. */
    public class Conflict : PrivateStoreException("the private store's document changed")

    /** The scope is not of the incarnation given: it was deleted since. */
    public class Wiped : PrivateStoreException("the private store's scope was wiped")
}

/**
 * Where a device keeps its encryption keys. It is called on the client's
 * thread, one request at a time, so answer quickly. Revisions and
 * incarnations are unsigned 64-bit numbers: use [java.lang.Long]'s unsigned
 * methods if you compare them.
 */
public interface PrivateStore {
    /** The incarnation of [scope], creating the scope with a new random, non-zero one if there is none. */
    public fun openScope(scope: String): Long

    /** The document, or `null` if there is none. */
    public fun load(scope: String, doc: String): StoredDocument?

    /**
     * Replaces the document if the scope is still of [incarnation] and the document's
     * revision is still [expected] (`null`: it must not exist), atomically, and returns the
     * new revision. Otherwise throws [PrivateStoreException.Wiped] or
     * [PrivateStoreException.Conflict] and writes nothing.
     */
    public fun save(scope: String, incarnation: Long, doc: String, bytes: ByteArray, expected: Long?): Long

    /** The names of the scope's documents. */
    public fun list(scope: String): List<String>

    /** Removes the scope and every document in it: its incarnation first, then its documents. */
    public fun delete(scope: String)

    /**
     * Protects a secret at rest (encrypts it with a key the platform keeps); [unprotect] recovers it.
     * Jazz zeroes the secret it passes to [protect] once it returns, and the array [unprotect] returns
     * once it has read it: keep neither (return a copy from [unprotect] if you keep the secret).
     */
    public fun protect(secret: ByteArray): ByteArray

    public fun unprotect(blob: ByteArray): ByteArray
}

/** What an app that declares end-to-end encryption opens with ([JazzConfig.encryption]). */
public data class JazzEncryption(
    /** This device's private store. */
    val store: PrivateStore,
    /** The deployment's environment: the same for every client of it. */
    val environment: String = "dev",
)

/** A private store's answer to jazz-ffi: [ok] with the result as JSON, or not ok with `conflict`, `wiped` or a message. */
public class StoreReply(public val ok: Boolean, public val payload: String)

/**
 * Answers jazz-ffi's store requests (JSON text: `op`, bytes as base64, numbers that
 * may exceed 2^53 as decimal text) with a [PrivateStore].
 *
 * Requests and answers carry key bytes. The arrays this bridge owns (the secret it decodes
 * for `protect`, the secret `unprotect` returns) are zeroed once used. JVM strings (the
 * request and answer texts, the base64 in them) and the parsed JSON are immutable and stay
 * until the garbage collector reuses their memory. jazz-ffi wipes its own copies.
 */
internal class PrivateStoreBridge(private val store: PrivateStore) {
    fun call(request: String): StoreReply {
        return try {
            val parsed = jazzJson.parseToJsonElement(request).jsonObject
            fun text(name: String): String =
                parsed[name]?.jsonPrimitive?.contentOrNull ?: error("the private store request has no $name")
            fun number(name: String): Long = java.lang.Long.parseUnsignedLong(text(name))
            fun bytes(name: String): ByteArray = Base64.getDecoder().decode(text(name))
            val result: JsonElement = when (val op = text("op")) {
                "openScope" -> JsonPrimitive(java.lang.Long.toUnsignedString(store.openScope(text("scope"))))
                "load" -> store.load(text("scope"), text("doc"))?.let {
                    buildJsonObject {
                        put("bytes", Base64.getEncoder().encodeToString(it.bytes))
                        put("revision", java.lang.Long.toUnsignedString(it.revision))
                    }
                } ?: JsonNull
                "save" -> {
                    val expected = if (parsed["expected"] is JsonPrimitive && parsed["expected"] !is JsonNull) number("expected") else null
                    JsonPrimitive(
                        java.lang.Long.toUnsignedString(
                            store.save(text("scope"), number("incarnation"), text("doc"), bytes("bytes"), expected),
                        ),
                    )
                }
                "list" -> JsonArray(store.list(text("scope")).map { JsonPrimitive(it) })
                "delete" -> {
                    store.delete(text("scope"))
                    JsonNull
                }
                "protect" -> {
                    val secret = bytes("secret")
                    try {
                        JsonPrimitive(Base64.getEncoder().encodeToString(store.protect(secret)))
                    } finally {
                        secret.fill(0)
                    }
                }
                "unprotect" -> {
                    val secret = store.unprotect(bytes("blob"))
                    try {
                        JsonPrimitive(Base64.getEncoder().encodeToString(secret))
                    } finally {
                        secret.fill(0)
                    }
                }
                else -> return StoreReply(false, "unknown private store operation $op")
            }
            StoreReply(true, result.toString())
        } catch (e: PrivateStoreException.Conflict) {
            StoreReply(false, "conflict")
        } catch (e: PrivateStoreException.Wiped) {
            StoreReply(false, "wiped")
        } catch (e: Throwable) {
            StoreReply(false, e.message ?: e.toString())
        }
    }
}

/** A device of the account: active (`status` `active`), or waiting for approval (`waiting`). */
public data class Device(val id: String, val status: String)

/** The deliveries written and the spaces whose delivery failed in one pass. */
@Serializable
public data class KeyDeliveryResult(val delivered: Long, val failed: Long)

/**
 * The app's end-to-end encryption ([Jazz.encryption]): this device's account, its
 * other devices, its recovery and its keys. Every call fails with
 * [JazzException.Refused] for an app that declares none.
 */
public class Encryption internal constructor(
    private val native: Native,
    private val blocking: suspend (() -> String) -> String,
) {
    public companion object {
        /** The code of an encryption failure (`approval-code-mismatch`, `no-challenge`, ...), or `null`. */
        @JvmStatic
        public fun codeOf(error: Throwable): String? {
            val message = error.message ?: return null
            if (!message.startsWith("encryption (")) return null
            return message.removePrefix("encryption (").substringBefore(")")
        }
    }

    /** One of the runtime's operations by name (see the README), with its result as JSON. */
    public suspend fun call(op: String, args: JsonObject = JsonObject(emptyMap())): JsonElement =
        jazzJson.parseToJsonElement(blocking { native.encryptionCall(op, args.toString()) })

    /**
     * Verifies the account and enrols or recovers this device, waiting up to [timeoutMs]:
     * `active`, `awaiting-approval` or `reenrolled`.
     */
    public suspend fun ready(timeoutMs: Long = 30_000): String {
        // A negative timeout would be read as an unsigned number, a wait of centuries.
        if (timeoutMs < 0) throw JazzException.Refused("the readiness timeout must not be negative")
        return blocking { native.encryptionReady(timeoutMs) }
    }

    /** What the runtime's loop is doing now, as JSON (`status`, `error`, `stage`). */
    public suspend fun state(): JsonObject = jazzJson.parseToJsonElement(blocking { native.encryptionState() }).jsonObject

    /** The reports waiting (`type`: `unreadable`, `missing-key`, `challenged`, `ready-failed`, ...). */
    public suspend fun reports(): List<JsonObject> {
        val texts = blocking { native.encryptionReports().joinToString(separator = "\n") }
        return texts.lines().filter { it.isNotBlank() }.map { jazzJson.parseToJsonElement(it).jsonObject }
    }

    private suspend fun text(op: String, args: JsonObject = JsonObject(emptyMap())): String? =
        (call(op, args) as? JsonPrimitive)?.contentOrNull

    /** This installation's device ID. */
    public suspend fun deviceId(): String = text("deviceId").orEmpty()

    /** The account's devices. */
    public suspend fun devices(): List<Device> =
        (call("devices") as? JsonArray).orEmpty().map {
            val device = it.jsonObject
            Device(device["id"]!!.jsonPrimitive.content, device["status"]!!.jsonPrimitive.content)
        }

    /**
     * Starts approving a waiting device (on an approved one); [approvalCode] then [approve]
     * finish it once the user confirmed both devices show the same code ([answer] is the
     * new device's).
     */
    public suspend fun challenge(deviceId: String) {
        call("challenge", buildJsonObject { put("deviceId", deviceId) })
    }

    /** On the new device, once challenged: the code to show. */
    public suspend fun answer(): String = text("answer").orEmpty()

    /** On the approving device: the code to compare, `null` until the new device answered. */
    public suspend fun approvalCode(deviceId: String): String? = text("approvalCode", buildJsonObject { put("deviceId", deviceId) })

    /** Approves the device if [code] is the one [approvalCode] gave. */
    public suspend fun approve(deviceId: String, code: String) {
        call("approve", buildJsonObject { put("deviceId", deviceId); put("code", code) })
    }

    /** Removes a device and rotates the account. */
    public suspend fun removeDevice(deviceId: String) {
        call("remove", buildJsonObject { put("deviceId", deviceId) })
    }

    /**
     * Runs a delivery pass for new members, newly approved devices and rotated spaces.
     * Check the result's [KeyDeliveryResult.failed] count for failures within the pass.
     */
    public suspend fun keysDeliver(): KeyDeliveryResult =
        jazzJson.decodeFromJsonElement(KeyDeliveryResult.serializer(), call("keysDeliver"))

    /** Rotates the key of the space row [id] of [table]. */
    public suspend fun keysRotate(table: String, id: String) {
        call("keysRotate", buildJsonObject { put("table", table); put("id", id) })
    }

    /**
     * A new recovery phrase (24 words) to keep. It registers nothing: [recoveryRegister] does.
     * Only for an account the app signs in itself (a device account's recovery is its account key).
     */
    public suspend fun recoveryCreate(): String =
        (call("recoveryCreate") as? JsonObject)?.get("phrase")?.jsonPrimitive?.contentOrNull.orEmpty()

    /** Registers the recovery root [phrase] gives, from a device that is active. */
    public suspend fun recoveryRegister(phrase: String) {
        call("recoveryRegister", buildJsonObject { put("phrase", phrase) })
    }

    /** Approves this new device with [phrase]: its status. */
    public suspend fun recoveryRestore(phrase: String): String =
        text("recoveryRestore", buildJsonObject { put("phrase", phrase) }).orEmpty()
}
