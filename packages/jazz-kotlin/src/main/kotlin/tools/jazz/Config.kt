package tools.jazz

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/** How fresh a read must be. */
public enum class ReadTier(internal val wire: String) {
    /** What this process has, now. */
    Local("local"),

    /** What this process has, after first syncing what the query needs (the default). */
    LocalFirst("localFirst"),

    /** As the sync server has it. */
    Remote("remote"),
}

/** How far a write must get before the call returns. */
public enum class WriteTier(internal val wire: String) {
    /** Stored in this process (the default: it syncs in the background). */
    Local("local"),

    /** Accepted by the sync server: a write its permissions refuse fails with [JazzException.Permission]. */
    Global("global"),
}

/** Where the app keeps its data. */
public enum class Storage {
    /** In memory, gone when the handle closes. */
    Memory,

    /** On disk, in [JazzConfig.dataDir]. */
    Persistent,
}

/**
 * How to open an app. Locally (no [serverUrl]) or as a backend of a Jazz
 * server: [permissions] (the text of `jazz.permissions.json`), [serverUrl],
 * [backendSecret], [adminSecret] and, to act as a user, [sessionSubject].
 */
public data class JazzConfig(
    val appId: String? = null,
    val serverUrl: String? = null,
    val storage: Storage = Storage.Memory,
    val dataDir: String? = null,
    val permissions: String? = null,
    val backendSecret: String? = null,
    val adminSecret: String? = null,
    /** The user a backend acts as (issuer `jazz:backend`). */
    val sessionSubject: String? = null,
    /** How an end user's app signs in to [serverUrl] (the app's Jazz endpoint); not for a backend. */
    val auth: JazzAuth? = null,
    /** Where the blocking native calls run; every call is main-safe either way. */
    val dispatcher: kotlinx.coroutines.CoroutineDispatcher = kotlinx.coroutines.Dispatchers.IO,
) {
    internal fun toJson(app: String): String {
        val json: JsonObject = buildJsonObject {
            put("app", app)
            // jazz-ffi contract 2: typed error kinds (Permission, NotSupported, Conflict).
            put("contract", 2)
            permissions?.let { put("permissions", it) }
            appId?.let { put("appId", it) }
            serverUrl?.let { put("serverUrl", it) }
            dataDir?.let { put("dataDir", it) }
            put(
                "storage",
                when (storage) {
                    Storage.Memory -> "memory"
                    Storage.Persistent -> "persistent"
                },
            )
            backendSecret?.let { put("backendSecret", it) }
            adminSecret?.let { put("adminSecret", it) }
            when (auth) {
                is JazzAuth.DeviceAccount -> put("auth", buildJsonObject { put("deviceKey", auth.key) })
                is JazzAuth.Bearer -> put("auth", buildJsonObject { put("bearer", true) })
                null -> {}
            }
            sessionSubject?.let {
                put(
                    "session",
                    buildJsonObject {
                        put("issuer", "jazz:backend")
                        put("subject", JsonPrimitive(it))
                    },
                )
            }
        }
        return json.toString()
    }
}
