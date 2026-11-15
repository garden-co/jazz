package tools.jazz

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.util.UUID

/** An in-memory stand-in for jazz-ffi: eq filters, orderBy, limit/offset, and subscriptions. */
class FakeNative : Native {
    val tables = HashMap<String, LinkedHashMap<String, JsonObject>>()
    val queries = ArrayList<String>()
    val subs = java.util.Collections.synchronizedList(ArrayList<Pair<JsonObject, (String) -> Unit>>())
    var closed = false
    var failSubscriptionsWith: String? = null

    override fun account(): String = "00000000-0000-0000-0000-00000000c0de"

    private fun matches(q: JsonObject): List<JsonObject> {
        var rows = tables[q["table"]!!.jsonPrimitive.content]?.values?.toList() ?: emptyList()
        for (w in q["where"]?.jsonArray ?: JsonArray(emptyList())) {
            val c = w.jsonObject
            val col = c["column"]!!.jsonPrimitive.content
            val v = c["value"]
            rows = rows.filter { r ->
                when (c["op"]!!.jsonPrimitive.content) {
                    "eq" -> r[col] == v
                    "ne" -> r[col] != v
                    else -> true
                }
            }
        }
        val query = Query(q["table"]!!.jsonPrimitive.content, JsonObject.serializer(), orders = (q["orderBy"]?.jsonArray ?: JsonArray(emptyList())).map {
            Order(it.jsonObject["column"]!!.jsonPrimitive.content, if (it.jsonObject["direction"]!!.jsonPrimitive.content == "asc") Direction.Asc else Direction.Desc)
        })
        rows = query.sort(rows)
        val offset = q["offset"]?.jsonPrimitive?.content?.toInt() ?: 0
        val limit = q["limit"]?.jsonPrimitive?.content?.toInt() ?: Int.MAX_VALUE
        return rows.drop(offset).take(limit)
    }

    override fun all(queryJson: String): String {
        queries += queryJson
        return JsonArray(matches(jazzJson.parseToJsonElement(queryJson).jsonObject)).toString()
    }

    override fun one(queryJson: String): String? =
        matches(jazzJson.parseToJsonElement(queryJson).jsonObject).firstOrNull()?.toString()

    override fun insert(table: String, rowJson: String): String {
        val id = UUID.randomUUID().toString()
        val row = JsonObject(mapOf<String, JsonElement>("id" to JsonPrimitive(id)) + jazzJson.parseToJsonElement(rowJson).jsonObject)
        tables.getOrPut(table) { LinkedHashMap() }[id] = row
        notify(table, added = listOf(row))
        return row.toString()
    }

    override fun update(table: String, id: String, patchJson: String) {
        val rows = tables[table] ?: throw JazzException.Refused("no row $id")
        val old = rows[id] ?: throw JazzException.Refused("no row $id")
        val row = JsonObject(old + jazzJson.parseToJsonElement(patchJson).jsonObject)
        rows[id] = row
        notify(table, updated = listOf(row))
    }

    override fun delete(table: String, id: String) {
        tables[table]?.remove(id)
        notify(table, removed = listOf(id))
    }

    private fun notify(table: String, added: List<JsonObject> = emptyList(), updated: List<JsonObject> = emptyList(), removed: List<String> = emptyList()) {
        for ((q, cb) in subs.toList()) {
            if (q["table"]!!.jsonPrimitive.content != table) continue
            cb(delta(added, updated, removed, false))
        }
    }

    private fun delta(added: List<JsonObject>, updated: List<JsonObject>, removed: List<String>, reset: Boolean) = buildJsonObject {
        put("added", JsonArray(added))
        put("updated", JsonArray(updated))
        put("removed", JsonArray(removed.map { JsonPrimitive(it) }))
        put("reset", reset)
        put("coalesced", false)
    }.toString()

    override fun subscribe(queryJson: String, onDelta: (String) -> Unit, onError: (String) -> Unit): () -> Unit {
        val q = jazzJson.parseToJsonElement(queryJson).jsonObject
        val entry = q to onDelta
        subs += entry
        onDelta(delta(matches(q), emptyList(), emptyList(), true))
        failSubscriptionsWith?.let(onError)
        return { subs.remove(entry) }
    }

    override fun close() {
        closed = true
    }
}
