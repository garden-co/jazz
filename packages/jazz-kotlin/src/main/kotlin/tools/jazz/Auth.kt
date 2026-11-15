package tools.jazz

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * How an end user's app signs in to the app's Jazz endpoint
 * ([JazzConfig.serverUrl], e.g. `https://app.example/jazz`, the backend's
 * `createJazz` handler).
 */
public sealed interface JazzAuth {
    /**
     * A local-first device account: [key] is the device's secret
     * (`jazz-auth-v1:…`, from [generateDeviceKey] once, then kept in the
     * platform's keychain). Signing up is opening with a new key.
     */
    public class DeviceAccount(public val key: String) : JazzAuth {
        override fun toString(): String = "DeviceAccount(…)"
    }

    /**
     * A user your backend signs in: [token] gives its bearer token (`null`
     * signs the user out). It's asked when connecting, before the token
     * expires, after the server refused one, and on [Jazz.refreshSession].
     */
    public class Bearer(public val token: suspend (TokenReason) -> String?) : JazzAuth

    public companion object {
        /** A new device account's secret, `jazz-auth-v1:…` (as TS's `exportDeviceKey`). */
        public fun generateDeviceKey(): String = deviceKeys()
    }
}

/** Why a [JazzAuth.Bearer] provider is asked for a token. */
public enum class TokenReason(internal val wire: String) {
    Connect("connect"),
    Renew("renew"),
    /** The server refused the last token: give a fresh one. */
    Expired("expired"),
    /** [Jazz.refreshSession]: give a fresh one. */
    Refresh("refresh"),
    ;

    /** Whether the provider should fetch a fresh token rather than reuse a cached one (TS's `getToken({ fresh })`). */
    public val fresh: Boolean get() = this == Expired || this == Refresh

    internal companion object {
        fun of(wire: String): TokenReason = entries.firstOrNull { it.wire == wire } ?: Connect
    }
}

/** Who is signed in. */
public data class JazzUser(
    /** The account id policies compare with `session.user`. */
    val account: String,
    /** `device` (a device account) or `backend` (a user your backend signed in). */
    val kind: String,
    val issuer: String,
    val subject: String,
    /** The claims your backend gave the user (`{}` for a device account). */
    val claims: JsonElement,
)

/** An end user's session, as [Jazz.session] reports it. */
public sealed interface SessionState {
    public data class SignedIn(val user: JazzUser) : SessionState

    /**
     * The server refused the credential and a fresh one didn't help (or
     * can't: [reason] `revoked`, `disabled`). Local data stays readable;
     * [Jazz.refreshSession] tries again.
     */
    public data class Expired(val user: JazzUser, val reason: String) : SessionState

    public data object SignedOut : SessionState

    public companion object {
        internal fun parse(json: String): SessionState {
            val o = jazzJson.parseToJsonElement(json).jsonObject
            fun user() = o["user"]!!.jsonObject.let { u ->
                JazzUser(
                    account = u["account"]!!.jsonPrimitive.content,
                    kind = u["kind"]?.jsonPrimitive?.content ?: "",
                    issuer = u["issuer"]?.jsonPrimitive?.content ?: "",
                    subject = u["subject"]?.jsonPrimitive?.content ?: "",
                    claims = u["claims"] ?: JsonObject(emptyMap()),
                )
            }
            return when (o["status"]?.jsonPrimitive?.content) {
                "signedIn" -> SignedIn(user())
                "expired" -> Expired(user(), o["reason"]?.jsonPrimitive?.content ?: "")
                else -> SignedOut
            }
        }
    }
}

/** How the app syncs, as TS's `SyncStatus.link`. */
public sealed interface SyncStatus {
    /** No server: the app runs locally. */
    public data object LocalOnly : SyncStatus
    public data object Connecting : SyncStatus
    public data object Connected : SyncStatus

    /** Not syncing: [reason] `network`, `stopped`, or an auth failure; [retrying] when it reconnects by itself. */
    public data class Offline(val reason: String, val retrying: Boolean, val message: String?) : SyncStatus

    public companion object {
        internal fun parse(json: String): SyncStatus {
            val link = jazzJson.parseToJsonElement(json).jsonObject["link"]?.jsonObject ?: return LocalOnly
            return when (link["state"]?.jsonPrimitive?.content) {
                "connecting" -> Connecting
                "connected" -> Connected
                "offline" -> Offline(
                    reason = when (val r = link["reason"]) {
                        is JsonPrimitive -> r.content
                        is JsonObject -> r.entries.firstOrNull()?.let { (k, v) -> "$k:${(v as? JsonPrimitive)?.content ?: v}" } ?: "offline"
                        else -> "offline"
                    },
                    retrying = link["retrying"]?.jsonPrimitive?.booleanOrNull ?: false,
                    message = (link["message"] as? JsonPrimitive)?.content,
                )
                else -> LocalOnly
            }
        }
    }
}

private fun deviceKeys(): String {
    val native = Class.forName("tools.jazz.UniffiNative")
    return try {
        native.getMethod("generateDeviceKey").invoke(null) as String
    } catch (e: java.lang.reflect.InvocationTargetException) {
        throw e.targetException
    }
}
