package tools.jazz

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * A relation `jazz.app.json` declares, from rows of [Owner] to rows of
 * [Target], as generated table objects expose it (`Tasks.assignee`,
 * `Tasks.comments`). Pass it to [Query.include]; read what it brought with
 * [JazzRow.get].
 */
public sealed class Relation<Owner : Any, Target : Any>(
    /** The relation's name in `jazz.app.json`. */
    public val name: String,
    target: () -> BaseTable<Target, *>,
) {
    /** The table the relation leads to (resolved lazily: table objects refer to each other). */
    public val target: BaseTable<Target, *> by lazy(target)

    internal abstract fun spec(): JsonObject

    internal abstract fun nestedIncludes(): List<Relation<*, *>>
}

/** A forward relation: this row's column references at most one [Target] row. */
public class One<Owner : Any, Target : Any> private constructor(
    name: String,
    private val targetFn: () -> BaseTable<Target, *>,
    private val isRequired: Boolean,
    private val columns: List<String>?,
    private val includes: List<Relation<Target, *>>,
) : Relation<Owner, Target>(name, targetFn) {
    public constructor(name: String, target: () -> BaseTable<Target, *>) :
        this(name, target, false, null, emptyList())

    /** Keep only rows whose referenced row exists and is readable (TS's `requireIncludes`). */
    public fun required(): One<Owner, Target> = One(name, targetFn, true, columns, includes)

    /** Bring only these columns of the referenced row (and its `id`). */
    public fun select(vararg columns: Column<*>): One<Owner, Target> =
        One(name, targetFn, isRequired, columns.map { it.name }, includes)

    /** Bring the referenced row's own related rows too. */
    public fun include(vararg relations: Relation<Target, *>): One<Owner, Target> =
        One(name, targetFn, isRequired, columns, includes + relations)

    override fun spec(): JsonObject = includeSpec(name, isRequired, subQuery(emptyList(), emptyList(), null, columns, includes))

    override fun nestedIncludes(): List<Relation<*, *>> = includes
}

/** A reverse relation: the [Target] rows whose forward relation points at this row. */
public class Many<Owner : Any, Target : Any> private constructor(
    name: String,
    private val targetFn: () -> BaseTable<Target, *>,
    private val conditions: List<Condition>,
    private val orders: List<Order>,
    private val limit: Int?,
    private val columns: List<String>?,
    private val includes: List<Relation<Target, *>>,
) : Relation<Owner, Target>(name, targetFn) {
    public constructor(name: String, target: () -> BaseTable<Target, *>) :
        this(name, target, emptyList(), emptyList(), null, null, emptyList())

    private fun copy(
        conditions: List<Condition> = this.conditions,
        orders: List<Order> = this.orders,
        limit: Int? = this.limit,
        columns: List<String>? = this.columns,
        includes: List<Relation<Target, *>> = this.includes,
    ) = Many<Owner, Target>(name, targetFn, conditions, orders, limit, columns, includes)

    /** Only the related rows matching all [conditions]. */
    public fun where(vararg conditions: Condition): Many<Owner, Target> = copy(conditions = this.conditions + conditions)

    /** The related rows in this order. */
    public fun orderBy(vararg orders: Order): Many<Owner, Target> = copy(orders = this.orders + orders)

    /** At most [limit] related rows per row. */
    public fun limit(limit: Int): Many<Owner, Target> = copy(limit = limit)

    /** Bring only these columns of each related row (and its `id`). */
    public fun select(vararg columns: Column<*>): Many<Owner, Target> = copy(columns = columns.map { it.name })

    /** Bring each related row's own related rows too. */
    public fun include(vararg relations: Relation<Target, *>): Many<Owner, Target> = copy(includes = includes + relations)

    override fun spec(): JsonObject = includeSpec(name, false, subQuery(conditions, orders, limit, columns, includes))

    override fun nestedIncludes(): List<Relation<*, *>> = includes
}

private fun includeSpec(name: String, required: Boolean, query: JsonObject?): JsonObject = buildJsonObject {
    put("relation", name)
    if (required) put("required", true)
    query?.let { put("query", it) }
}

private fun subQuery(
    conditions: List<Condition>,
    orders: List<Order>,
    limit: Int?,
    columns: List<String>?,
    includes: List<Relation<*, *>>,
): JsonObject? {
    if (conditions.isEmpty() && orders.isEmpty() && limit == null && columns == null && includes.isEmpty()) return null
    return buildJsonObject {
        if (conditions.isNotEmpty()) put("where", whereJson(conditions))
        if (orders.isNotEmpty()) put("orderBy", orderJson(orders))
        limit?.let { put("limit", it) }
        columns?.let { c -> put("select", buildJsonArray { c.forEach { add(JsonPrimitive(it)) } }) }
        if (includes.isNotEmpty()) put("include", JsonArray(includes.map { it.spec() }))
    }
}

/**
 * A row read with [Query.include] or [Query.select]: its columns, and the
 * related rows its query brought.
 *
 * ```kotlin
 * for (task in jazz.all(Tasks.query().include(Tasks.assignee, Tasks.comments))) {
 *     val title: String = task[Tasks.title]
 *     val assignee: Crew? = task[Tasks.assignee]
 *     val comments: List<Comment> = task[Tasks.comments]
 * }
 * ```
 */
public class JazzRow<Row : Any> internal constructor(
    /** The row as the native core returned it. */
    public val json: JsonObject,
    private val serializer: KSerializer<Row>,
) {
    /** The row's id. */
    public val id: String get() = (json["id"] as JsonPrimitive).content

    /** The row itself, without what was included; fails for a row read with [Query.select]. */
    public val value: Row by lazy { jazzJson.decodeFromJsonElement(serializer, json) }

    /** Column [column]'s value. */
    public operator fun <T> get(column: Column<T>): T =
        jazzJson.decodeFromJsonElement(column.serializer, json[column.name] ?: JsonNull)

    /** The row [relation] references, or `null`; [relation] must have been included. */
    public operator fun <T : Any> get(relation: One<Row, T>): T? = row(relation)?.value

    /** The rows [relation] leads to; [relation] must have been included. */
    public operator fun <T : Any> get(relation: Many<Row, T>): List<T> = rows(relation).map { it.value }

    /** The row [relation] references, with what its include brought in turn. */
    public fun <T : Any> row(relation: One<Row, T>): JazzRow<T>? = when (val v = included(relation)) {
        is JsonObject -> JazzRow(v, relation.target.rowSerializer)
        else -> null
    }

    /** The rows [relation] leads to, with what its include brought in turn. */
    public fun <T : Any> rows(relation: Many<Row, T>): List<JazzRow<T>> =
        (included(relation) as? JsonArray ?: JsonArray(emptyList())).map {
            JazzRow(it as JsonObject, relation.target.rowSerializer)
        }

    private fun included(relation: Relation<Row, *>): JsonElement? {
        if (relation.name !in json) {
            throw IllegalStateException("relation `${relation.name}` wasn't included in the query that read this row")
        }
        return json[relation.name]
    }

    override fun equals(other: Any?): Boolean = other is JazzRow<*> && other.json == json
    override fun hashCode(): Int = json.hashCode()
    override fun toString(): String = "JazzRow($json)"
}

internal class JazzRowSerializer<Row : Any>(private val base: KSerializer<Row>) : KSerializer<JazzRow<Row>> {
    override val descriptor: SerialDescriptor = JsonObject.serializer().descriptor

    override fun deserialize(decoder: Decoder): JazzRow<Row> =
        JazzRow((decoder as JsonDecoder).decodeJsonElement() as JsonObject, base)

    override fun serialize(encoder: Encoder, value: JazzRow<Row>): Unit =
        (encoder as JsonEncoder).encodeJsonElement(value.json)
}
