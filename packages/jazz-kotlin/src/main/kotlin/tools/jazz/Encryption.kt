package tools.jazz

import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

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
    /** Add the signed-in creator to each new space. */
    val addCreatorAsMember: Boolean = true,
)

/** A device of the account: active (`status` `active`), or waiting for approval (`waiting`). */
@Serializable
public data class Device(
    val id: String,
    val status: String,
    /** Whether this device has published a request to be removed. */
    val removalRequested: Boolean = false,
)

/** A row granting an account membership in a space. */
@Serializable
public data class SpaceMember(val id: String, val scopeId: String, val identifier: String, val accountId: String)

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
        public fun codeOf(error: Throwable): String? = (error as? JazzException)?.encryptionCode
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
        return call("waitUntilReady", buildJsonObject { put("timeoutMs", timeoutMs) }).jsonPrimitive.content
    }

    /** What the runtime's loop is doing now, as JSON (`status`, `error`, `stage`). */
    public suspend fun state(): JsonObject = call("state").jsonObject

    /** The reports waiting (`type`: `unreadable`, `missing-key`, `challenged`, `ready-failed`, ...). */
    public suspend fun reports(): List<JsonObject> = call("reports").jsonArray.map { it.jsonObject }

    private suspend fun text(op: String, args: JsonObject = JsonObject(emptyMap())): String? =
        (call(op, args) as? JsonPrimitive)?.contentOrNull

    /** This installation's device ID. */
    public suspend fun deviceId(): String = text("deviceId").orEmpty()

    /** The account's devices. */
    public suspend fun devices(): List<Device> =
        jazzJson.decodeFromJsonElement(ListSerializer(Device.serializer()), call("devices"))

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

    /** Admits an account to a space and returns its membership row, as an insert would. */
    public suspend fun addMember(table: String, id: String, accountId: String, tier: WriteTier? = null): SpaceMember =
        jazzJson.decodeFromString(blocking { native.memberAdd(table, id, accountId, tier?.wire) })

    /** Lists the space's membership rows visible to the caller. */
    public suspend fun listMembers(table: String, id: String): List<SpaceMember> =
        jazzJson.decodeFromString(blocking { native.memberList(table, id) })

    /** Removes the account's visible membership rows. Returns the number the authority accepted. */
    public suspend fun removeMember(table: String, id: String, accountId: String): ULong =
        blocking { native.memberRemove(table, id, accountId).toString() }.toULong()

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
