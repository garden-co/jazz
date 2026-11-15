// JazzApp.kt: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 2. Regenerate it after changing jazz.app.json.
@file:kotlin.Suppress("unused", "RedundantVisibilityModifier")

package jazzapp

/** `sha256:…` of the `jazz.app.json` these bindings were generated from. */
public const val SOURCE: kotlin.String = "sha256:13613b0132c17b0e3b37a865eceb5d2c4ed6b8c77a6bda454869edfa3df26d4a";

/** The app's `jazz.app.json`: its schema, every earlier version and the migrations between them. */
public val APP: kotlin.String = kotlin.arrayOf(
    "{\n",
    "  \"\$schema\": \"https://jazz.tools/schemas/jazz.app.schema.json\",\n",
    "  \"format\": 1,\n",
    "  \"tables\": {\n",
    "    \"results\": {\n",
    "      \"columns\": {\n",
    "        \"tools\": \"string\",\n",
    "        \"kotlin\": \"int?\",\n",
    "        \"kotlinx\": \"boolean\",\n",
    "        \"java\": \"float\",\n",
    "        \"query\": \"string?\",\n",
    "        \"object\": \"string?\",\n",
    "        \"due_at\": \"timestamp?\"\n",
    "      }\n",
    "    },\n",
    "    \"units\": {\n",
    "      \"columns\": { \"tableName\": \"string\", \"result\": \"uuid?\", \"tags\": { \"type\": { \"array\": \"string\" } } }\n",
    "    }\n",
    "  }\n",
    "}\n",
).joinToString("")

/**
 * Opens the app. A backend (`JazzConfig.permissions` set to its
 * `jazz.permissions.json`) first publishes the app's permissions bundle
 * and pins the session it acts in to it.
 */
public suspend fun open(config: tools.jazz.JazzConfig): tools.jazz.Jazz =
    tools.jazz.Jazz.open(APP, config)

/** A row of `results`. */
@kotlinx.serialization.Serializable
public data class Result(
    val id: kotlin.String,
    val tools: kotlin.String,
    val kotlin: kotlin.Int? = null,
    val kotlinx: kotlin.Boolean,
    val java: kotlin.Double,
    val query: kotlin.String? = null,
    val `object`: kotlin.String? = null,
    val due_at: kotlin.Long? = null,
)

/** A new row of `results`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewResult(
    val tools: kotlin.String,
    val kotlin: kotlin.Int? = null,
    val kotlinx: kotlin.Boolean,
    val java: kotlin.Double,
    val query: kotlin.String? = null,
    val `object`: kotlin.String? = null,
    val due_at: kotlin.Long? = null,
)

/** Changes to a row of `results`: columns not set are left as they are. */
public class ResultPatch : tools.jazz.Patch() {
    /** Sets `tools`. */
    public fun tools(value: kotlin.String): ResultPatch = apply { set("tools", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `kotlin`. */
    public fun kotlin(value: kotlin.Int?): ResultPatch = apply { set("kotlin", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Int>())) }
    /** Sets `kotlinx`. */
    public fun kotlinx(value: kotlin.Boolean): ResultPatch = apply { set("kotlinx", value, kotlinx.serialization.serializer<kotlin.Boolean>()) }
    /** Sets `java`. */
    public fun java(value: kotlin.Double): ResultPatch = apply { set("java", value, kotlinx.serialization.serializer<kotlin.Double>()) }
    /** Sets `query`. */
    public fun query(value: kotlin.String?): ResultPatch = apply { set("query", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
    /** Sets `object`. */
    public fun `object`(value: kotlin.String?): ResultPatch = apply { set("object", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
    /** Sets `due_at`. */
    public fun due_at(value: kotlin.Long?): ResultPatch = apply { set("due_at", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Long>())) }
}

/** Table `results`: its columns, and queries with `query()`. */
public object Results : tools.jazz.Table<Result, NewResult, ResultPatch>(
    "results",
    Result.serializer(),
    NewResult.serializer(),
    ::ResultPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `tools`. */
    public val toolsColumn: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("tools", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `kotlin`. */
    public val kotlinColumn: tools.jazz.Column<kotlin.Int?> =
        tools.jazz.Column("kotlin", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Int>()))
    /** Column `kotlinx`. */
    public val kotlinxColumn: tools.jazz.Column<kotlin.Boolean> =
        tools.jazz.Column("kotlinx", kotlinx.serialization.serializer<kotlin.Boolean>())
    /** Column `java`. */
    public val javaColumn: tools.jazz.Column<kotlin.Double> =
        tools.jazz.Column("java", kotlinx.serialization.serializer<kotlin.Double>())
    /** Column `query`. */
    public val queryColumn: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("query", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
    /** Column `object`. */
    public val `object`: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("object", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
    /** Column `due_at`. */
    public val due_at: tools.jazz.Column<kotlin.Long?> =
        tools.jazz.Column("due_at", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Long>()))
}

/** A row of `units`. */
@kotlinx.serialization.Serializable
public data class Unit(
    val id: kotlin.String,
    val tableName: kotlin.String,
    val result: kotlin.String? = null,
    val tags: kotlin.collections.List<kotlin.String>,
)

/** A new row of `units`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewUnit(
    val tableName: kotlin.String,
    val result: kotlin.String? = null,
    val tags: kotlin.collections.List<kotlin.String>,
)

/** Changes to a row of `units`: columns not set are left as they are. */
public class UnitPatch : tools.jazz.Patch() {
    /** Sets `tableName`. */
    public fun tableName(value: kotlin.String): UnitPatch = apply { set("tableName", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `result`. */
    public fun result(value: kotlin.String?): UnitPatch = apply { set("result", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
    /** Sets `tags`. */
    public fun tags(value: kotlin.collections.List<kotlin.String>): UnitPatch = apply { set("tags", value, kotlinx.serialization.builtins.ListSerializer(kotlinx.serialization.serializer<kotlin.String>())) }
}

/** Table `units`: its columns, and queries with `query()`. */
public object Units : tools.jazz.Table<Unit, NewUnit, UnitPatch>(
    "units",
    Unit.serializer(),
    NewUnit.serializer(),
    ::UnitPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `tableName`. */
    public val tableNameColumn: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("tableName", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `result`. */
    public val result: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("result", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
    /** Column `tags`. */
    public val tags: tools.jazz.Column<kotlin.collections.List<kotlin.String>> =
        tools.jazz.Column("tags", kotlinx.serialization.builtins.ListSerializer(kotlinx.serialization.serializer<kotlin.String>()))
}
