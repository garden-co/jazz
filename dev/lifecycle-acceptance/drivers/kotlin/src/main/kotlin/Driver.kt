// The Kotlin/JVM lifecycle driver: drivers/PROTOCOL.md on jazz-kotlin's public API.
package lifecycle

import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import tools.jazz.Jazz
import tools.jazz.JazzAuth
import tools.jazz.JazzConfig
import tools.jazz.ReadTier
import tools.jazz.Storage
import tools.jazz.WriteTier
import tools.jazz.jazzJson

private class Unsupported(op: String) : Exception(op)
private class Bad(message: String) : Exception(message)

private val clients = ConcurrentHashMap<String, Jazz>()
private val running = ConcurrentHashMap<Long, Job>()

private fun JsonObject.text(name: String): String = (this[name] as? JsonPrimitive)?.contentOrNull ?: ""

private suspend fun perform(r: JsonObject): JsonElement {
    val h = r.text("h")
    fun client() = clients[h] ?: throw Bad("no handle")
    when (val op = r.text("op")) {
        "genkey" -> return JsonPrimitive(JazzAuth.generateDeviceKey())
        "open" -> {
            val key = r.text("key")
            val token = r.text("token")
            val local = (r["local"] as? JsonPrimitive)?.content == "true"
            val auth = if (local) null else if (key.isNotEmpty()) JazzAuth.DeviceAccount(key) else JazzAuth.Bearer { token.ifEmpty { null } }
            clients[h] = jazzapp.open(
                JazzConfig(
                    appId = r.text("app"),
                    serverUrl = if (local) null else r.text("url"),
                    storage = if (r.text("storage") == "memory") Storage.Memory else Storage.Persistent,
                    dataDir = r.text("dir").takeIf { r.text("storage") != "memory" },
                    auth = auth,
                ),
            )
        }
        "account" -> {
            // What a Kotlin app has: the session's user.
            val state = client().session.value as? tools.jazz.SessionState.SignedIn ?: return JsonNull
            return JsonPrimitive(state.user.account)
        }
        "write" -> {
            val tier = if (r.text("tier") == "global") WriteTier.Global else null
            return JsonPrimitive(client().insert(jazzapp.Notes, jazzapp.NewNote(r.text("title"), r.text("owner")), tier).id)
        }
        "read" -> {
            val tier = if (r.text("tier") == "remote") ReadTier.Remote else ReadTier.Local
            return JsonArray(client().all(jazzapp.Notes.query(), tier).map { it.title }.sorted().map(::JsonPrimitive))
        }
        "sync" -> client().waitForSync()
        // close() blocks: an app calls it off its main thread.
        // jazz-kotlin's close() takes no timeout.
        "close" -> if (r.containsKey("ms")) throw Unsupported("close with a timeout") else withContext(Dispatchers.IO) { client().close() }
        "delete" -> client().deleteClientStorage()
        "disconnect" -> client().disconnect()
        "reconnect" -> client().reconnect()
        "signout" -> client().signOut()
        "hrefresh" -> client().refreshSession()
        "cancel" -> running[r["target"]!!.jsonPrimitive.long]?.cancel()
        // jazz-kotlin has no JazzSession.
        else -> throw Unsupported(op)
    }
    return JsonNull
}

private fun answer(line: JsonObject) = synchronized(System.out) {
    println(line.toString())
    System.out.flush()
}

private fun failure(id: Long, kind: String, message: String) = buildJsonObject {
    put("id", id)
    put("ok", false)
    put("kind", kind)
    put("msg", message)
}

fun main() {
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    answer(buildJsonObject { put("ready", true); put("pid", ProcessHandle.current().pid()) })
    while (true) {
        val line = readLine() ?: break
        val request = jazzJson.parseToJsonElement(line) as JsonObject
        val id = request["id"]!!.jsonPrimitive.long
        val job = scope.launch(start = CoroutineStart.LAZY) {
            val reply = try {
                val value = perform(request)
                buildJsonObject { put("id", id); put("ok", true); put("v", value) }
            } catch (e: CancellationException) {
                failure(id, "driver.cancelled", e.message ?: "cancelled")
            } catch (e: Unsupported) {
                failure(id, "driver.unsupported", e.message ?: "")
            } catch (e: Bad) {
                failure(id, "driver.bad", e.message ?: "")
            } catch (e: Throwable) {
                failure(id, e::class.java.simpleName, e.message ?: e.toString())
            }
            running.remove(id)
            answer(reply)
        }
        running[id] = job
        job.start()
    }
    // End of input: exit as a process does, closing nothing.
    Runtime.getRuntime().halt(0)
}
