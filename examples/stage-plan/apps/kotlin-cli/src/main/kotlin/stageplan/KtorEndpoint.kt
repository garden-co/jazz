// #region backend-endpoint-kotlin
package stageplan

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.httpMethod
import io.ktor.server.request.receiveChannel
import io.ktor.server.request.uri
import io.ktor.server.response.header
import io.ktor.server.response.respondBytes
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.utils.io.toByteArray
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import tools.jazz.JazzEndpoint
import tools.jazz.JazzRequest

/**
 * Serves a backend's [JazzEndpoint] at `/jazz` with Ktor: end users' apps
 * sign in and sync through it, as through TS `createJazz`'s handler.
 */
fun serveJazz(endpoint: JazzEndpoint, port: Int): EmbeddedServer<*, *> =
    embeddedServer(CIO, port = port) {
        install(WebSockets)
        routing {
            // The sync socket: upgraded here, then served by the endpoint.
            webSocket("/jazz/{rest...}") {
                val upgrade = call.toJazzRequest(body = null)
                if (!endpoint.isSocket(upgrade.path)) return@webSocket close(CloseReason(CloseReason.Codes.CANNOT_ACCEPT, "not the socket path"))
                val outgoing = Channel<Frame>(Channel.UNLIMITED)
                val socket = endpoint.socket(
                    upgrade,
                    send = { bytes -> outgoing.trySend(Frame.Binary(true, bytes)) },
                    close = { code, reason -> outgoing.trySend(Frame.Close(CloseReason(code.toShort(), reason))); outgoing.close() },
                )
                val sender = launch { for (frame in outgoing) send(frame) }
                try {
                    // Every data message, binary or text (TS clients send their prelude as text).
                    for (frame in incoming) if (frame is Frame.Binary || frame is Frame.Text) socket.receive(frame.data)
                } finally {
                    socket.close()
                    sender.cancel()
                }
            }
            // Everything else under /jazz: sign-in and account routes.
            route("/jazz/{rest...}") {
                handle {
                    val response = endpoint.handle(call.toJazzRequest(body = call.receiveChannel().toByteArray()))
                    for ((name, value) in response.headers) {
                        if (!name.equals("content-type", ignoreCase = true) && !name.equals("content-length", ignoreCase = true)) call.response.header(name, value)
                    }
                    val type = response.headers.firstOrNull { it.first.equals("content-type", ignoreCase = true) }?.second
                    call.respondBytes(response.body, type?.let(ContentType::parse), HttpStatusCode.fromValue(response.status))
                }
            }
        }
    }

private fun ApplicationCall.toJazzRequest(body: ByteArray?): JazzRequest = JazzRequest(
    method = request.httpMethod.value,
    path = request.uri,
    headers = request.headers.entries().flatMap { (name, values) -> values.map { name to it } },
    body = body?.takeIf { it.isNotEmpty() },
)
// #endregion backend-endpoint-kotlin
