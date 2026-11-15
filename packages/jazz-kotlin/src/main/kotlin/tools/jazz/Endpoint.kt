package tools.jazz

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

/** An HTTP request to a [JazzEndpoint], as your HTTP server received it. */
public class JazzRequest(
    public val method: String,
    /** The path, with its query string. */
    public val path: String,
    public val headers: List<Pair<String, String>> = emptyList(),
    public val body: ByteArray? = null,
) {
    /** The first header [name] (case-insensitive), e.g. `Authorization`. */
    public fun header(name: String): String? = headers.firstOrNull { it.first.equals(name, ignoreCase = true) }?.second

    @OptIn(ExperimentalEncodingApi::class)
    internal fun toJson(): String = buildJsonObject {
        put("method", method)
        put("path", path)
        put("headers", buildJsonArray { headers.forEach { (k, v) -> add(buildJsonArray { add(JsonPrimitive(k)); add(JsonPrimitive(v)) }) } })
        body?.let { put("body", Base64.encode(it)) }
    }.toString()

    internal companion object {
        @OptIn(ExperimentalEncodingApi::class)
        fun parse(json: String): JazzRequest {
            val o = jazzJson.parseToJsonElement(json).jsonObject
            return JazzRequest(
                method = o["method"]!!.jsonPrimitive.content,
                path = o["path"]!!.jsonPrimitive.content,
                headers = (o["headers"] as? JsonArray)?.map { it.jsonArray[0].jsonPrimitive.content to it.jsonArray[1].jsonPrimitive.content } ?: emptyList(),
                body = (o["body"] as? JsonPrimitive)?.content?.let { Base64.decode(it) },
            )
        }
    }
}

/** What a [JazzEndpoint] answers: send it back as is. */
public class JazzResponse(
    public val status: Int,
    public val headers: List<Pair<String, String>>,
    public val body: ByteArray,
) {
    internal companion object {
        @OptIn(ExperimentalEncodingApi::class)
        fun parse(json: String): JazzResponse {
            val o = jazzJson.parseToJsonElement(json).jsonObject
            return JazzResponse(
                status = o["status"]!!.jsonPrimitive.content.toInt(),
                headers = (o["headers"] as? JsonArray)?.map { it.jsonArray[0].jsonPrimitive.content to it.jsonArray[1].jsonPrimitive.content } ?: emptyList(),
                body = (o["body"] as? JsonPrimitive)?.content?.let { Base64.decode(it) } ?: ByteArray(0),
            )
        }
    }
}

/** Who a request's user is, as your own sign-in says (for [Jazz.endpoint]'s `authenticate`). */
public sealed interface EndpointUser {
    /** A user your app signed in: [userId] is their subject; the session lasts until [expiresAt] (Unix seconds). */
    public data class User(val userId: String, val expiresAt: Long, val claims: JsonElement? = null) : EndpointUser

    /** A local-first device account, which proves itself. */
    public data object DeviceAccount : EndpointUser

    public companion object {
        internal fun toJson(user: EndpointUser?): String? = when (user) {
            null -> null
            DeviceAccount -> """{"kind":"deviceAccount"}"""
            is User -> buildJsonObject {
                put("kind", "user")
                put("userId", user.userId)
                put("expiresAt", user.expiresAt)
                user.claims?.let { put("claims", it) }
            }.toString()
        }
    }
}

/**
 * A backend's Jazz endpoint (TS `createJazz`'s handler): end users' apps
 * sign in and sync through it. Serve it from your HTTP server under its
 * base path: [handle] each request, except those [isSocket] says to
 * upgrade to a WebSocket and serve with [socket].
 */
public class JazzEndpoint internal constructor(private val native: NativeEndpoint, private val jazz: Jazz) {
    /** Whether [path] is the socket path, to upgrade to a WebSocket. */
    public fun isSocket(path: String): Boolean = native.isSocket(path)

    /** Answers an HTTP request (404 for a path it doesn't serve). */
    public suspend fun handle(request: JazzRequest): JazzResponse =
        jazz.ioCall { JazzResponse.parse(native.handle(request.toJson())) }

    /** A handle acting as [request]'s user (TS `forReqUser`), for your own routes. */
    public suspend fun forUser(request: JazzRequest): Jazz = jazz.ioCall { jazz.derive(native.forUser(request.toJson())) }

    /**
     * Serves an upgraded socket: [upgrade] is the upgrade request; [send]
     * gets each binary message for the client, [close] the code and reason
     * to close it with, once. Pass the client's messages to
     * [JazzSocket.receive], and call [JazzSocket.close] when it closes.
     */
    public suspend fun socket(upgrade: JazzRequest, send: (ByteArray) -> Unit, close: (code: Int, reason: String) -> Unit): JazzSocket =
        jazz.ioCall { JazzSocket(native.socket(upgrade.toJson(), send, close)) }
}

/** A socket a [JazzEndpoint] serves. */
public class JazzSocket internal constructor(private val native: NativeSocket) {
    /** A binary message from the client. */
    public fun receive(message: ByteArray): Unit = native.push(message)

    /** The client's socket closed. */
    public fun close(): Unit = native.close()
}

/** jazz-ffi's endpoint: see [JazzEndpoint]. */
public interface NativeEndpoint {
    public fun isSocket(path: String): Boolean
    public fun handle(requestJson: String): String
    public fun forUser(requestJson: String): Native
    public fun socket(upgradeJson: String, onMessage: (ByteArray) -> Unit, onClose: (Int, String) -> Unit): NativeSocket
}

public interface NativeSocket {
    public fun push(message: ByteArray)
    public fun close()
}

internal fun authenticateJson(authenticate: suspend (JazzRequest) -> EndpointUser?): (String) -> String? = { json ->
    // Called on a thread of the native core's own: blocking it is fine.
    EndpointUser.toJson(runBlocking { authenticate(JazzRequest.parse(json)) })
}
