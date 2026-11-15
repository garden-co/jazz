package tools.jazz

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

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
) {
    internal fun toJson(app: String): String {
        val json: JsonObject = buildJsonObject {
            put("app", app)
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
