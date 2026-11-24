package tools.jazz

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

/**
 * What a text or bytes position counts. Kotlin's positions are [Utf16], the
 * default wherever there is one: `String` and `CharSequence` indices, as
 * Android's `EditText` selections and `Spannable` ranges count. [Utf8] is
 * for byte offsets from elsewhere.
 */
public enum class SpliceUnits(internal val wire: String) {
    /** UTF-8 bytes of a text value. */
    Utf8("utf8"),

    /** UTF-16 code units of a text value: Kotlin's `String` indices (the default). */
    Utf16("utf16"),

    /** Bytes of a `bytea` value. */
    Bytes("bytes"),
}

/** One splice: at [at] (relative to the edited range's start), delete [delete] units, insert [insert]. */
public class Splice private constructor(internal val at: Long, internal val delete: Long, internal val insert: JsonPrimitive) {
    public companion object {
        /** A splice of text. */
        public fun text(at: Long, delete: Long, insert: String): Splice = Splice(at, delete, JsonPrimitive(insert))

        /** A splice of bytes. */
        @OptIn(ExperimentalEncodingApi::class)
        public fun bytes(at: Long, delete: Long, insert: ByteArray): Splice = Splice(at, delete, JsonPrimitive(Base64.encode(insert)))
    }
}

/**
 * Partial edits of one row, for [Jazz.edit]: merged text edits, splices of
 * text and bytes, and JSON sets, written together.
 */
public class Edits internal constructor() {
    internal val edits = ArrayList<JsonObject>()

    /**
     * Sets text-merge column [column] (`"merge": "text"`) to [text], an edit
     * of [editedFrom], the value you last read: changes others made since
     * are kept, as the TS client's `editedFrom` does.
     */
    public fun text(column: Column<out String?>, editedFrom: String, text: String) {
        edits += buildJsonObject {
            put("kind", "text")
            put("column", column.name)
            put("text", text)
            put("editedFrom", editedFrom)
        }
    }

    /**
     * Applies text [splices] to [column]'s value within `[from, to)`, in
     * UTF-16 code units (`String` indices, as an `EditText` selection);
     * each splice's `at` and `delete` count from [from] in the same units.
     */
    public fun splice(column: Column<out String?>, from: Long, to: Long, vararg splices: Splice): Unit =
        splice(column, SpliceUnits.Utf16, from, to, *splices)

    /** Applies [splices] to [column]'s value within `[from, to)`, in [units]; each splice's `at` counts from [from]. */
    public fun splice(column: Column<*>, units: SpliceUnits, from: Long, to: Long, vararg splices: Splice) {
        edits += buildJsonObject {
            put("kind", "splice")
            put("column", column.name)
            put(
                "within",
                buildJsonObject {
                    put("units", units.wire)
                    put("from", from)
                    put("to", to)
                },
            )
            put(
                "splices",
                buildJsonArray {
                    for (s in splices) {
                        add(
                            buildJsonObject {
                                put("at", s.at)
                                put("delete", s.delete)
                                put("insert", s.insert)
                            },
                        )
                    }
                },
            )
        }
    }

    /** Sets the value at JSON pointer [at] (e.g. `"/color"`) inside JSON column [column] to [value]. */
    public fun jsonSet(column: Column<out JsonElement?>, at: String, value: JsonElement) {
        edits += buildJsonObject {
            put("kind", "jsonSet")
            put("column", column.name)
            put(
                "edits",
                buildJsonArray {
                    add(
                        buildJsonObject {
                            put("at", at)
                            put("value", value)
                        },
                    )
                },
            )
        }
    }

    internal fun toJson(): String = JsonArray(edits).toString()
}
