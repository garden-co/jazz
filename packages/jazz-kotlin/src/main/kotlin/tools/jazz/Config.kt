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
 * How to open an app: locally (no [serverUrl]), or as an end user of your
 * backend's Jazz endpoint ([serverUrl], e.g. `https://app.example/jazz`,
 * signed in with [auth]). Kotlin is for apps: your app's main backend runs in TypeScript.
 */
public data class JazzConfig(
    val appId: String? = null,
    /** Your backend's Jazz endpoint (its `createJazz` handler), e.g. `https://app.example/jazz`. */
    val serverUrl: String? = null,
    val storage: Storage = Storage.Memory,
    /**
     * The directory of a [Storage.Persistent] store. One app at a time has it
     * open: share that [Jazz], as opening a second one throws [JazzException.Refused].
     */
    val dataDir: String? = null,
    /** How the user signs in to [serverUrl]: a device account, or your backend's sign-in. */
    val auth: JazzAuth? = null,
    /** Where the blocking native calls run; every call is main-safe either way. */
    val dispatcher: kotlinx.coroutines.CoroutineDispatcher = kotlinx.coroutines.Dispatchers.IO,
) {
    internal fun toJson(app: String): String {
        val json: JsonObject = buildJsonObject {
            put("app", app)
            // jazz-ffi contract 2: typed error kinds (Permission, NotSupported, Conflict).
            put("contract", 2)
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
            when (auth) {
                is JazzAuth.DeviceAccount -> put("auth", buildJsonObject { put("deviceKey", auth.key) })
                is JazzAuth.Bearer -> put("auth", buildJsonObject { put("bearer", true) })
                null -> {}
            }
        }
        return json.toString()
    }
}
