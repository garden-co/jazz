package tools.jazz

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.ListSerializer
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
    public fun query(): Query<Row> = Query(tableName, rowSerializer)

    /** `query().where(...)`. */
    public fun where(vararg conditions: Condition): Query<Row> = query().where(*conditions)

    /** `query().orderBy(...)`. */
    public fun orderBy(vararg orders: Order): Query<Row> = query().orderBy(*orders)

    /** `query().include(...)`. */
    public fun include(vararg relations: Relation<Row, *>): Query<JazzRow<Row>> = query().include(*relations)
}

/** A column of type [T], to filter and sort by. */
public class Column<T>(public val name: String, public val serializer: KSerializer<T>) {
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
    val columns: List<String>? = null,
) {
    public fun where(vararg conditions: Condition): Query<Row> = copy(conditions = this.conditions + conditions)
    public fun orderBy(vararg orders: Order): Query<Row> = copy(orders = this.orders + orders)
    public fun limit(limit: Int): Query<Row> = copy(limit = limit)
    public fun offset(offset: Int): Query<Row> = copy(offset = offset)

    /** The contract's query JSON. */
    public fun toJson(): String = buildJsonObject {
        put("table", table)
        if (conditions.isNotEmpty()) put("where", whereJson(conditions))
        if (orders.isNotEmpty()) put("orderBy", orderJson(orders))
        limit?.let { put("limit", it) }
        offset?.let { put("offset", it) }
        columns?.let { c -> put("select", buildJsonArray { c.forEach { add(JsonPrimitive(it)) } }) }
        if (includes.isNotEmpty()) put("include", JsonArray(includes.map { it.spec() }))
    }.toString()

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
    Query(table, JazzRowSerializer(rowSerializer), conditions, orders, limit, offset, includes + relations, columns)

/** More relations for a query that already includes some. */
@JvmName("includeMore")
public fun <Row : Any> Query<JazzRow<Row>>.include(vararg relations: Relation<Row, *>): Query<JazzRow<Row>> =
    copy(includes = includes + relations)

/** Only these columns of each row (and its `id`): read them with [JazzRow.get]. */
public fun <Row : Any> Query<Row>.select(vararg columns: Column<*>): Query<JazzRow<Row>> =
    Query(table, JazzRowSerializer(rowSerializer), conditions, orders, limit, offset, includes, columns.map { it.name })

/** Only these columns, for a query that already includes relations. */
@JvmName("selectMore")
public fun <Row : Any> Query<JazzRow<Row>>.select(vararg columns: Column<*>): Query<JazzRow<Row>> =
    copy(columns = columns.map { it.name })

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
