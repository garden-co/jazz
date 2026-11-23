package tools.jazz

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.put

/**
 * A table, as generated bindings declare it: its name, how its rows,
 * inserts and patches (de)serialize, and (in the generated object) its
 * typed columns.
 */
public abstract class Table<Row : Any, Insert : Any, P : Patch>(
    public val tableName: String,
    public val rowSerializer: KSerializer<Row>,
    public val insertSerializer: KSerializer<Insert>,
    public val newPatch: () -> P,
) {
    /** Every row of the table; narrow it with [Query.where]. */
    public fun query(): Query<Row> = Query(tableName, rowSerializer, source = this)

    /**
     * A row as the native core returned it, as the row class reads it:
     * transforms applied (stored → view) and computed fields added, with
     * the app's `JazzComputed` ([implementations]). Generated for apps that
     * declare any.
     */
    public open fun fromStored(row: JsonObject, implementations: Any?): JsonObject = row

    /** An insert's or patch's values as the native core stores them: transforms applied (view → stored). */
    public open fun toStored(values: JsonObject, implementations: Any?): JsonObject = values

    /** `query().where(...)`. */
    public fun where(vararg conditions: Condition): Query<Row> = query().where(*conditions)

    /** `query().orderBy(...)`. */
    public fun orderBy(vararg orders: Order): Query<Row> = query().orderBy(*orders)

    /** `query().include(...)`. */
    public fun include(vararg relations: Relation<Row, *>): Query<JazzRow<Row>> = query().include(*relations)
}

/** What a query's `select` can read: a column ([Column]), or a part of a large one ([Column.range], [Column.at]). */
public interface Selection {
    /** The column it reads. */
    public val name: String

    /** Its `select` entry. */
    public fun selectJson(): JsonElement
}

/** A column of type [T], to filter and sort by. */
public class Column<T>(override val name: String, public val serializer: KSerializer<T>) : Selection {
    override fun selectJson(): JsonElement = JsonPrimitive(name)

    /**
     * Only part of this text or `bytea` column's value: `[from, to)` in
     * [units] (`row[column]` then reads that part). Root queries only. Text
     * positions are UTF-16 code units by default, `String` indices; pass
     * [SpliceUnits.Bytes] for a `bytea` column.
     */
    public fun range(from: Long, to: Long, units: SpliceUnits = SpliceUnits.Utf16): Selection = PartSelection(
        name,
        buildJsonObject {
            put("column", name)
            put("from", from)
            put("to", to)
            put("units", units.wire)
        },
    )

    /** Only the value at JSON pointer [pointer] (e.g. `"/color"`) inside this JSON column. Root queries only. */
    public fun at(pointer: String): Selection = PartSelection(
        name,
        buildJsonObject {
            put("column", name)
            put("at", pointer)
        },
    )

    private fun value(value: T): JsonElement = jazzJson.encodeToJsonElement(serializer, value)

    public infix fun eq(value: T): Condition = Condition(name, "eq", value(value))
    public infix fun ne(value: T): Condition = Condition(name, "ne", value(value))
    public infix fun lt(value: T): Condition = Condition(name, "lt", value(value))
    public infix fun lte(value: T): Condition = Condition(name, "lte", value(value))
    public infix fun gt(value: T): Condition = Condition(name, "gt", value(value))
    public infix fun gte(value: T): Condition = Condition(name, "gte", value(value))

    /** A string column containing [value], or an array column containing it as an element. */
    public infix fun contains(value: Any): Condition = Condition(name, "contains", anyJson(value))

    /** One of [values]. */
    public infix fun inList(values: Collection<T>): Condition =
        Condition(name, "in", jazzJson.encodeToJsonElement(ListSerializer(serializer), values.toList()))

    public fun isNull(): Condition = Condition(name, "isNull", null)
    public fun isNotNull(): Condition = Condition(name, "isNotNull", null)

    public fun asc(): Order = Order(name, Direction.Asc)
    public fun desc(): Order = Order(name, Direction.Desc)
}

private fun anyJson(value: Any): JsonElement = when (value) {
    is JsonElement -> value
    is String -> JsonPrimitive(value)
    is Number -> JsonPrimitive(value)
    is Boolean -> JsonPrimitive(value)
    else -> throw IllegalArgumentException("contains takes a string, number or boolean")
}

/** A `where` condition: conditions of one query are ANDed. */
public data class Condition(val column: String, val op: String, val value: JsonElement?)

public enum class Direction { Asc, Desc }

/** An `orderBy` entry. */
public data class Order(val column: String, val direction: Direction)

/**
 * A query of rows of type [Row]; immutable, each method returns a new one.
 * [include] and [select] make it a query of [JazzRow]s.
 */
public data class Query<Row : Any>(
    val table: String,
    val rowSerializer: KSerializer<Row>,
    val conditions: List<Condition> = emptyList(),
    val orders: List<Order> = emptyList(),
    val limit: Int? = null,
    val offset: Int? = null,
    val includes: List<Relation<*, *>> = emptyList(),
    val columns: List<JsonElement>? = null,
    /** The table object it reads, for its transforms and computed fields. */
    val source: Table<*, *, *>? = null,
    /** For [hopTo]: the query it hops from (whose rows' relations it follows), and the relations. */
    val hopFrom: Query<*>? = null,
    val hops: List<String> = emptyList(),
    /** For [gather]: the relation followed recursively, and how deep. */
    val gatherRelation: String? = null,
    val gatherDepth: Int? = null,
    /** For [union]: the queries whose rows it reads. */
    val unionOf: List<Query<*>>? = null,
    /** For [history]: the versions the server accepted, not the current rows. */
    val history: Boolean = false,
    /** Deleted rows too. */
    val includeDeleted: Boolean = false,
) {
    public fun where(vararg conditions: Condition): Query<Row> = copy(conditions = this.conditions + conditions)
    public fun orderBy(vararg orders: Order): Query<Row> = copy(orders = this.orders + orders)
    public fun limit(limit: Int): Query<Row> = copy(limit = limit)
    public fun offset(offset: Int): Query<Row> = copy(offset = offset)

    /** Deleted rows too (as TS's `includeDeleted`). */
    public fun includeDeleted(): Query<Row> = copy(includeDeleted = true)

    /** The contract's query JSON. */
    public fun toJson(): String = toJsonObject().toString()

    internal fun toJsonObject(): JsonObject {
        unionOf?.let { arms ->
            return buildJsonObject {
                put("union", JsonArray(arms.map { it.toJsonObject() }))
                if (orders.isNotEmpty()) put("orderBy", orderJson(orders))
                limit?.let { put("limit", it) }
                offset?.let { put("offset", it) }
                if (includeDeleted) put("includeDeleted", true)
            }
        }
        val from = hopFrom
        if (from != null) {
            // The hopped-from query, then `after`: what this query adds to the rows it hops to.
            val base = from.toJsonObject().toMutableMap()
            base["hopTo"] = if (hops.size == 1) JsonPrimitive(hops[0]) else JsonArray(hops.map(::JsonPrimitive))
            val after = buildJsonObject {
                if (conditions.isNotEmpty()) put("where", whereJson(conditions))
                if (orders.isNotEmpty()) put("orderBy", orderJson(orders))
                limit?.let { put("limit", it) }
                offset?.let { put("offset", it) }
                columns?.let { c -> put("select", JsonArray(c)) }
            }
            if (after.isNotEmpty()) base["after"] = after
            if (includeDeleted) base["includeDeleted"] = JsonPrimitive(true)
            return JsonObject(base)
        }
        return buildJsonObject {
            put("table", table)
            if (conditions.isNotEmpty()) put("where", whereJson(conditions))
            if (orders.isNotEmpty()) put("orderBy", orderJson(orders))
            limit?.let { put("limit", it) }
            offset?.let { put("offset", it) }
            columns?.let { c -> put("select", JsonArray(c)) }
            if (includes.isNotEmpty()) put("include", JsonArray(includes.map { it.spec() }))
            gatherRelation?.let { r ->
                put(
                    "gather",
                    buildJsonObject {
                        put("relation", r)
                        gatherDepth?.let { put("maxDepth", it) }
                    },
                )
            }
            if (history) put("history", true)
            if (includeDeleted) put("includeDeleted", true)
        }
    }

    /** Orders raw rows as [orders] says (subscription deltas carry no positions). */
    internal fun sort(rows: List<JsonObject>): List<JsonObject> {
        if (orders.isEmpty()) return rows
        val comparator = Comparator<JsonObject> { a, b ->
            for (o in orders) {
                val c = compareJson(a[o.column], b[o.column])
                if (c != 0) return@Comparator if (o.direction == Direction.Asc) c else -c
            }
            0
        }
        return rows.sortedWith(comparator)
    }
}

/** Nulls first, then numbers, booleans and strings by value. */
internal fun compareJson(a: JsonElement?, b: JsonElement?): Int {
    val x = a.takeUnless { it == null || it is JsonNull }
    val y = b.takeUnless { it == null || it is JsonNull }
    if (x == null || y == null) return compareValues(x == null, y == null).let { -it }
    if (x is JsonPrimitive && y is JsonPrimitive) {
        val (dx, dy) = x.doubleOrNull to y.doubleOrNull
        if (!x.isString && !y.isString && dx != null && dy != null) return dx.compareTo(dy)
        val (bx, by) = x.booleanOrNull to y.booleanOrNull
        if (!x.isString && !y.isString && bx != null && by != null) return bx.compareTo(by)
        return x.content.compareTo(y.content)
    }
    return x.toString().compareTo(y.toString())
}

/** Changes to a row: generated patch classes set columns through [set]. */
public open class Patch {
    private val fields = LinkedHashMap<String, JsonElement>()

    protected fun <T> set(column: String, value: T, serializer: KSerializer<T>) {
        fields[column] = jazzJson.encodeToJsonElement(serializer, value)
    }

    /** The patch JSON: only the columns set, a `null` kept as `null`. */
    public fun toJson(): String = JsonObject(fields).toString()

    public fun isEmpty(): Boolean = fields.isEmpty()
}

internal fun JsonArray.objects(): List<JsonObject> = map { it as JsonObject }

/** Brings the rows [relations] lead to along with each row: read them with [JazzRow.get]. */
public fun <Row : Any> Query<Row>.include(vararg relations: Relation<Row, *>): Query<JazzRow<Row>> =
    Query(table, JazzRowSerializer(rowSerializer), conditions, orders, limit, offset, includes + relations, columns, source)

/** More relations for a query that already includes some. */
@JvmName("includeMore")
public fun <Row : Any> Query<JazzRow<Row>>.include(vararg relations: Relation<Row, *>): Query<JazzRow<Row>> =
    copy(includes = includes + relations)

/** Only these columns of each row (and its `id`): read them with [JazzRow.get]. */
public fun <Row : Any> Query<Row>.select(vararg columns: Selection): Query<JazzRow<Row>> =
    Query(table, JazzRowSerializer(rowSerializer), conditions, orders, limit, offset, includes, columns.map { it.selectJson() }, source)

/** Only these columns, for a query that already includes relations. */
@JvmName("selectMore")
public fun <Row : Any> Query<JazzRow<Row>>.select(vararg columns: Selection): Query<JazzRow<Row>> =
    copy(columns = columns.map { it.selectJson() })

private class PartSelection(override val name: String, private val json: JsonObject) : Selection {
    override fun selectJson(): JsonElement = json
}

internal fun whereJson(conditions: List<Condition>): JsonArray = buildJsonArray {
    for (c in conditions) {
        add(
            buildJsonObject {
                put("column", c.column)
                put("op", c.op)
                c.value?.let { put("value", it) }
            },
        )
    }
}

internal fun orderJson(orders: List<Order>): JsonArray = buildJsonArray {
    for (o in orders) {
        add(
            buildJsonObject {
                put("column", o.column)
                put("direction", if (o.direction == Direction.Asc) "asc" else "desc")
            },
        )
    }
}

/** [row] through [table]'s transforms and computed fields, and each included row through its own. */
internal fun prepareRow(table: Table<*, *, *>?, includes: List<Relation<*, *>>, row: JsonObject, implementations: Any?): JsonObject {
    if (table == null) return row
    var out = table.fromStored(row, implementations)
    if (includes.isEmpty()) return out
    val fields = out.toMutableMap()
    for (relation in includes) {
        val target = relation.target
        val nested = relation.nestedIncludes()
        when (val value = fields[relation.name]) {
            is JsonObject -> fields[relation.name] = prepareRow(target, nested, value, implementations)
            is JsonArray -> fields[relation.name] =
                JsonArray(value.map { if (it is JsonObject) prepareRow(target, nested, it, implementations) else it })
            else -> {}
        }
    }
    out = JsonObject(fields)
    return out
}

/**
 * The rows [relation] leads to from this query's rows (TS's `hopTo`): e.g.
 * the crew assigned to a show's open tasks,
 * `Tasks.where(Tasks.showId eq id, Tasks.status ne Done).hopTo(Tasks.assignee)`.
 * Filters, sorting and limits after the hop apply to those rows.
 */
public fun <Row : Any, Target : Any> Query<Row>.hopTo(relation: Relation<Row, Target>): Query<Target> {
    val target = relation.target
    // Hop again straight from a hop with nothing after it: one hop path.
    if (hopFrom != null && conditions.isEmpty() && orders.isEmpty() && limit == null && offset == null && columns == null) {
        return Query(target.tableName, target.rowSerializer, source = target, hopFrom = hopFrom, hops = hops + relation.name)
    }
    return Query(target.tableName, target.rowSerializer, source = target, hopFrom = this, hops = listOf(relation.name))
}

/**
 * These rows and, recursively, the rows [relation] leads to from them, up to
 * [maxDepth] hops (TS's `gather`): e.g. a task with all its subtasks.
 */
public fun <Row : Any> Query<Row>.gather(relation: Relation<Row, Row>, maxDepth: Int? = null): Query<Row> =
    copy(gatherRelation = relation.name, gatherDepth = maxDepth)

/** Every row of this query and of [others], of the same table; sort and limit the union with [Query.orderBy]/[Query.limit]. */
public fun <Row : Any> Query<Row>.union(vararg others: Query<Row>): Query<Row> =
    Query(table, rowSerializer, source = source, unionOf = listOf(this) + others)

/** A version of a row the server accepted: the row as it was, and when. */
public data class HistoryRow<Row : Any>(val row: Row, val transactionId: String, val acceptedAt: Long)

internal class HistoryRowSerializer<Row : Any>(private val base: KSerializer<Row>) : KSerializer<HistoryRow<Row>> {
    override val descriptor: kotlinx.serialization.descriptors.SerialDescriptor = JsonObject.serializer().descriptor

    override fun deserialize(decoder: kotlinx.serialization.encoding.Decoder): HistoryRow<Row> {
        val json = (decoder as kotlinx.serialization.json.JsonDecoder).decodeJsonElement() as JsonObject
        val meta = json["\$history"] as? JsonObject
            ?: throw kotlinx.serialization.SerializationException("a history row needs \"\$history\": $json")
        return HistoryRow(
            row = jazzJson.decodeFromJsonElement(base, JsonObject(json - "\$history")),
            transactionId = (meta["transactionId"] as JsonPrimitive).content,
            acceptedAt = (meta["acceptedAt"] as JsonPrimitive).content.toLong(),
        )
    }

    override fun serialize(encoder: kotlinx.serialization.encoding.Encoder, value: HistoryRow<Row>): Unit =
        throw kotlinx.serialization.SerializationException("history rows are read only")
}

/**
 * The versions of this query's rows the server accepted, oldest first
 * (sort by [JazzColumns.acceptedAt]): read them at [ReadTier.Remote]. With jazz-ffi's result keys, `watch` on
 * them keeps each version.
 */
public fun <Row : Any> Query<Row>.history(): Query<HistoryRow<Row>> =
    Query(table, HistoryRowSerializer(rowSerializer), conditions, orders, limit, offset, source = source, history = true, includeDeleted = includeDeleted)

/** Columns every table has: when a row was created and last updated (ms since the epoch), and, for [history] rows, when the server accepted a version. */
public object JazzColumns {
    public val createdAt: Column<Long> = Column("\$createdAt", Long.serializer())
    public val updatedAt: Column<Long> = Column("\$updatedAt", Long.serializer())
    public val acceptedAt: Column<Long> = Column("\$acceptedAt", Long.serializer())
}
