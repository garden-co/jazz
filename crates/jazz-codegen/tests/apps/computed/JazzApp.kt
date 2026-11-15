// JazzApp.kt: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 2. Regenerate it after changing jazz.app.json.
@file:kotlin.Suppress("unused", "RedundantVisibilityModifier")

package jazzapp

/** `sha256:…` of the `jazz.app.json` these bindings were generated from. */
public const val SOURCE: kotlin.String = "sha256:4587bb8b3c405a2df2cdfaa9ad19e3bcaf0d60aa8f66a474f1d2952f750ee113";

/** The app's `jazz.app.json`: its schema, every earlier version and the migrations between them. */
public val APP: kotlin.String = kotlin.arrayOf(
    "{\n",
    "  \"\$schema\": \"https://jazz.tools/schemas/jazz.app.schema.json\",\n",
    "  \"format\": 1,\n",
    "  \"tables\": {\n",
    "    \"tasks\": {\n",
    "      \"columns\": {\n",
    "        \"title\": \"string\",\n",
    "        \"score\": { \"type\": \"int\", \"default\": 0 },\n",
    "        \"dueAt\": { \"type\": \"timestamp\", \"optional\": true, \"transform\": \"isoDate\" }\n",
    "      },\n",
    "      \"computed\": {\n",
    "        \"priority\": { \"type\": { \"enum\": [\"low\", \"high\"] }, \"from\": [\"score\"] },\n",
    "        \"label\": { \"type\": \"string?\", \"from\": [\"title\", \"dueAt\"] }\n",
    "      }\n",
    "    },\n",
    "    \"people\": {\n",
    "      \"columns\": { \"name\": \"string\", \"email\": { \"type\": \"string\", \"transform\": \"lowercase\" } }\n",
    "    }\n",
    "  },\n",
    "  \"transforms\": {\n",
    "    \"isoDate\": { \"stored\": \"timestamp\", \"view\": \"string\" },\n",
    "    \"lowercase\": { \"stored\": \"string\", \"view\": \"string\" }\n",
    "  }\n",
    "}\n",
).joinToString("")

/**
 * Opens the app: locally, or as an end user of your backend's Jazz
 * endpoint (`JazzConfig.serverUrl` and `JazzConfig.auth`).
 */
public suspend fun open(config: tools.jazz.JazzConfig, computed: JazzComputed): tools.jazz.Jazz =
    tools.jazz.Jazz.open(APP, config, computed)

/**
 * What the app's code computes: each computed field `jazz.app.json`
 * declares, and each transform both ways. [open] takes an implementation.
 */
public interface JazzComputed {
    /** `tasks.priority`. */
    public fun tasksPriority(score: kotlin.Int): TaskPriority

    /** `tasks.label`. */
    public fun tasksLabel(title: kotlin.String, dueAt: kotlin.String?): kotlin.String?

    /** Transform `isoDate`: a stored value, as the app reads it. */
    public fun isoDateView(stored: kotlin.Long): kotlin.String

    /** Transform `isoDate`: a value the app writes, as stored. */
    public fun isoDateStored(view: kotlin.String): kotlin.Long

    /** Transform `lowercase`: a stored value, as the app reads it. */
    public fun lowercaseView(stored: kotlin.String): kotlin.String

    /** Transform `lowercase`: a value the app writes, as stored. */
    public fun lowercaseStored(view: kotlin.String): kotlin.String
}

/** The values of computed field `tasks.priority`. */
@kotlinx.serialization.Serializable
public enum class TaskPriority {
    @kotlinx.serialization.SerialName("low") Low,
    @kotlinx.serialization.SerialName("high") High,
}

/** A row of `people`. */
@kotlinx.serialization.Serializable
public data class Person(
    val id: kotlin.String,
    val name: kotlin.String,
    val email: kotlin.String,
)

/** A new row of `people`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewPerson(
    val name: kotlin.String,
    val email: kotlin.String,
)

/** Changes to a row of `people`: columns not set are left as they are. */
public class PersonPatch : tools.jazz.Patch() {
    /** Sets `name`. */
    public fun name(value: kotlin.String): PersonPatch = apply { set("name", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `email`. */
    public fun email(value: kotlin.String): PersonPatch = apply { set("email", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `people`: its columns, and queries with `query()`. */
public object People : tools.jazz.Table<Person, NewPerson, PersonPatch>(
    "people",
    Person.serializer(),
    NewPerson.serializer(),
    ::PersonPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `name`. */
    public val name: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("name", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `email`. Compared as stored, before its transform. */
    public val email: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("email", kotlinx.serialization.serializer<kotlin.String>())
    override fun fromStored(row: kotlinx.serialization.json.JsonObject, implementations: kotlin.Any?): kotlinx.serialization.json.JsonObject {
        val computed = implementations as JazzComputed
        val out = row.toMutableMap()
        tools.jazz.transformColumn(out, "email", kotlinx.serialization.serializer<kotlin.String>(), kotlinx.serialization.serializer<kotlin.String>()) { computed.lowercaseView(it) }
        return kotlinx.serialization.json.JsonObject(out)
    }
    override fun toStored(values: kotlinx.serialization.json.JsonObject, implementations: kotlin.Any?): kotlinx.serialization.json.JsonObject {
        val computed = implementations as JazzComputed
        val out = values.toMutableMap()
        tools.jazz.transformColumn(out, "email", kotlinx.serialization.serializer<kotlin.String>(), kotlinx.serialization.serializer<kotlin.String>()) { computed.lowercaseStored(it) }
        return kotlinx.serialization.json.JsonObject(out)
    }
}

/** A row of `tasks`. */
@kotlinx.serialization.Serializable
public data class Task(
    val id: kotlin.String,
    val title: kotlin.String,
    val score: kotlin.Int,
    val dueAt: kotlin.String? = null,
    /** Computed from `score`. */
    val priority: TaskPriority,
    /** Computed from `title`, `dueAt`. */
    val label: kotlin.String? = null,
)

/** A new row of `tasks`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewTask(
    val title: kotlin.String,
    val score: kotlin.Int? = null,
    val dueAt: kotlin.String? = null,
)

/** Changes to a row of `tasks`: columns not set are left as they are. */
public class TaskPatch : tools.jazz.Patch() {
    /** Sets `title`. */
    public fun title(value: kotlin.String): TaskPatch = apply { set("title", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `score`. */
    public fun score(value: kotlin.Int): TaskPatch = apply { set("score", value, kotlinx.serialization.serializer<kotlin.Int>()) }
    /** Sets `dueAt`. */
    public fun dueAt(value: kotlin.String?): TaskPatch = apply { set("dueAt", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
}

/** Table `tasks`: its columns, and queries with `query()`. */
public object Tasks : tools.jazz.Table<Task, NewTask, TaskPatch>(
    "tasks",
    Task.serializer(),
    NewTask.serializer(),
    ::TaskPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `title`. */
    public val title: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("title", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `score`. */
    public val score: tools.jazz.Column<kotlin.Int> =
        tools.jazz.Column("score", kotlinx.serialization.serializer<kotlin.Int>())
    /** Column `dueAt`. Compared as stored, before its transform. */
    public val dueAt: tools.jazz.Column<kotlin.Long?> =
        tools.jazz.Column("dueAt", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Long>()))
    override fun fromStored(row: kotlinx.serialization.json.JsonObject, implementations: kotlin.Any?): kotlinx.serialization.json.JsonObject {
        val computed = implementations as JazzComputed
        val out = row.toMutableMap()
        tools.jazz.transformColumn(out, "dueAt", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Long>()), tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) { it?.let(computed::isoDateView) }
        tools.jazz.computeField(out, "priority", TaskPriority.serializer(), kotlin.collections.listOf("score" to kotlinx.serialization.serializer<kotlin.Int>())) { args -> computed.tasksPriority(args[0] as kotlin.Int) }
        tools.jazz.computeField(out, "label", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()), kotlin.collections.listOf("title" to kotlinx.serialization.serializer<kotlin.String>(), "dueAt" to tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))) { args -> computed.tasksLabel(args[0] as kotlin.String, args[1] as kotlin.String?) }
        return kotlinx.serialization.json.JsonObject(out)
    }
    override fun toStored(values: kotlinx.serialization.json.JsonObject, implementations: kotlin.Any?): kotlinx.serialization.json.JsonObject {
        val computed = implementations as JazzComputed
        val out = values.toMutableMap()
        tools.jazz.transformColumn(out, "dueAt", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()), tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Long>())) { it?.let(computed::isoDateStored) }
        return kotlinx.serialization.json.JsonObject(out)
    }
}
