// JazzApp.kt: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 2. Regenerate it after changing jazz.app.json.
@file:kotlin.Suppress("unused", "RedundantVisibilityModifier")

package jazzapp

/** `sha256:…` of the `jazz.app.json` these bindings were generated from. */
public const val SOURCE: kotlin.String = "sha256:019eebe8a4aa9f75919d6cfd18bbc36487719aef45b19c6b77175373bd416977";

/** The app's `jazz.app.json`: its schema, every earlier version and the migrations between them. */
public val APP: kotlin.String = kotlin.arrayOf(
    "{\n",
    "  \"\$schema\": \"https://jazz.tools/schemas/jazz.app.schema.json\",\n",
    "  \"format\": 1,\n",
    "  \"tables\": {\n",
    "    \"projects\": {\n",
    "      \"columns\": { \"name\": \"string\", \"budget\": \"bigint?\", \"logo\": \"bytes?\" },\n",
    "      \"relations\": { \"tasksViaProject\": { \"reverse\": \"tasks\", \"relation\": \"project\" } }\n",
    "    },\n",
    "    \"tasks\": {\n",
    "      \"columns\": {\n",
    "        \"title\": { \"type\": \"string\", \"merge\": \"text\" },\n",
    "        \"projectId\": \"uuid\",\n",
    "        \"status\": { \"enum\": [\"todo\", \"doing\", \"done\"], \"default\": \"todo\" },\n",
    "        \"score\": { \"type\": \"int\", \"default\": 0, \"merge\": \"counter\" },\n",
    "        \"tags\": { \"array\": \"string\" },\n",
    "        \"dueAt\": { \"type\": \"timestamp\", \"optional\": true, \"transform\": \"isoDate\" },\n",
    "        \"meta\": {\n",
    "          \"json\": {\n",
    "            \"type\": \"object\",\n",
    "            \"properties\": {\n",
    "              \"color\": { \"type\": \"string\" },\n",
    "              \"size\": { \"enum\": [\"s\", \"m\", \"l\"] },\n",
    "              \"links\": { \"type\": \"array\", \"items\": { \"type\": \"string\" } }\n",
    "            },\n",
    "            \"required\": [\"color\"],\n",
    "            \"additionalProperties\": false\n",
    "          },\n",
    "          \"optional\": true\n",
    "        },\n",
    "        \"extra\": { \"json\": true, \"optional\": true },\n",
    "        \"assigneeIds\": { \"array\": \"uuid\" }\n",
    "      },\n",
    "      \"relations\": {\n",
    "        \"project\": { \"rel\": \"projects\", \"column\": \"projectId\" },\n",
    "        \"assignees\": { \"rel\": \"people\", \"column\": \"assigneeIds\" }\n",
    "      },\n",
    "      \"indexes\": [\"projectId\", [\"projectId\", \"\$createdAt\"]],\n",
    "      \"computed\": {\n",
    "        \"priority\": { \"type\": { \"enum\": [\"low\", \"high\"] }, \"from\": [\"score\", \"status\"] },\n",
    "        \"label\": { \"type\": \"string?\", \"from\": [\"title\", \"dueAt\"] }\n",
    "      }\n",
    "    },\n",
    "    \"people\": {\n",
    "      \"columns\": { \"name\": \"string\", \"email\": { \"type\": \"string\", \"transform\": \"lowercase\" } },\n",
    "      \"relations\": { \"tasksViaAssignees\": { \"reverse\": \"tasks\", \"relation\": \"assignees\" } },\n",
    "      \"history\": \"latest\"\n",
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
    public fun tasksPriority(score: kotlin.Int, status: TaskStatus): TaskPriority

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

/** The values of column `tasks.status`. */
@kotlinx.serialization.Serializable
public enum class TaskStatus {
    @kotlinx.serialization.SerialName("todo") Todo,
    @kotlinx.serialization.SerialName("doing") Doing,
    @kotlinx.serialization.SerialName("done") Done,
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
    /** Relation `tasksViaAssignees`: the `tasks` rows whose `assignees` is this row. */
    public val tasksViaAssignees: tools.jazz.Many<Person, Task> =
        tools.jazz.Many("tasksViaAssignees") { Tasks }
}

/** A row of `projects`. */
@kotlinx.serialization.Serializable
public data class Project(
    val id: kotlin.String,
    val name: kotlin.String,
    val budget: @kotlinx.serialization.Serializable(with = tools.jazz.JazzBigInt::class) kotlin.Long? = null,
    val logo: @kotlinx.serialization.Serializable(with = tools.jazz.JazzBytes::class) kotlin.ByteArray? = null,
)

/** A new row of `projects`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewProject(
    val name: kotlin.String,
    val budget: @kotlinx.serialization.Serializable(with = tools.jazz.JazzBigInt::class) kotlin.Long? = null,
    val logo: @kotlinx.serialization.Serializable(with = tools.jazz.JazzBytes::class) kotlin.ByteArray? = null,
)

/** Changes to a row of `projects`: columns not set are left as they are. */
public class ProjectPatch : tools.jazz.Patch() {
    /** Sets `name`. */
    public fun name(value: kotlin.String): ProjectPatch = apply { set("name", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `budget`. */
    public fun budget(value: kotlin.Long?): ProjectPatch = apply { set("budget", value, tools.jazz.nullable(tools.jazz.JazzBigInt)) }
    /** Sets `logo`. */
    public fun logo(value: kotlin.ByteArray?): ProjectPatch = apply { set("logo", value, tools.jazz.nullable(tools.jazz.JazzBytes)) }
}

/** Table `projects`: its columns, and queries with `query()`. */
public object Projects : tools.jazz.Table<Project, NewProject, ProjectPatch>(
    "projects",
    Project.serializer(),
    NewProject.serializer(),
    ::ProjectPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `name`. */
    public val name: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("name", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `budget`. */
    public val budget: tools.jazz.Column<kotlin.Long?> =
        tools.jazz.Column("budget", tools.jazz.nullable(tools.jazz.JazzBigInt))
    /** Column `logo`. */
    public val logo: tools.jazz.Column<kotlin.ByteArray?> =
        tools.jazz.Column("logo", tools.jazz.nullable(tools.jazz.JazzBytes))
    /** Relation `tasksViaProject`: the `tasks` rows whose `project` is this row. */
    public val tasksViaProject: tools.jazz.Many<Project, Task> =
        tools.jazz.Many("tasksViaProject") { Tasks }
}

/** A row of `tasks`. */
@kotlinx.serialization.Serializable
public data class Task(
    val id: kotlin.String,
    val title: kotlin.String,
    val projectId: kotlin.String,
    val status: TaskStatus,
    val score: kotlin.Int,
    val tags: kotlin.collections.List<kotlin.String>,
    val dueAt: kotlin.String? = null,
    val meta: kotlinx.serialization.json.JsonElement? = null,
    val extra: kotlinx.serialization.json.JsonElement? = null,
    val assigneeIds: kotlin.collections.List<kotlin.String>,
    /** Computed from `score`, `status`. */
    val priority: TaskPriority,
    /** Computed from `title`, `dueAt`. */
    val label: kotlin.String? = null,
)

/** A new row of `tasks`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewTask(
    val title: kotlin.String,
    val projectId: kotlin.String,
    val status: TaskStatus? = null,
    val score: kotlin.Int? = null,
    val tags: kotlin.collections.List<kotlin.String>,
    val dueAt: kotlin.String? = null,
    val meta: kotlinx.serialization.json.JsonElement? = null,
    val extra: kotlinx.serialization.json.JsonElement? = null,
    val assigneeIds: kotlin.collections.List<kotlin.String>,
)

/** Changes to a row of `tasks`: columns not set are left as they are. */
public class TaskPatch : tools.jazz.Patch() {
    /** Sets `title`. */
    public fun title(value: kotlin.String): TaskPatch = apply { set("title", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `projectId`. */
    public fun projectId(value: kotlin.String): TaskPatch = apply { set("projectId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `status`. */
    public fun status(value: TaskStatus): TaskPatch = apply { set("status", value, TaskStatus.serializer()) }
    /** Sets `score`. */
    public fun score(value: kotlin.Int): TaskPatch = apply { set("score", value, kotlinx.serialization.serializer<kotlin.Int>()) }
    /** Sets `tags`. */
    public fun tags(value: kotlin.collections.List<kotlin.String>): TaskPatch = apply { set("tags", value, kotlinx.serialization.builtins.ListSerializer(kotlinx.serialization.serializer<kotlin.String>())) }
    /** Sets `dueAt`. */
    public fun dueAt(value: kotlin.String?): TaskPatch = apply { set("dueAt", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
    /** Sets `meta`. */
    public fun meta(value: kotlinx.serialization.json.JsonElement?): TaskPatch = apply { set("meta", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlinx.serialization.json.JsonElement>())) }
    /** Sets `extra`. */
    public fun extra(value: kotlinx.serialization.json.JsonElement?): TaskPatch = apply { set("extra", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlinx.serialization.json.JsonElement>())) }
    /** Sets `assigneeIds`. */
    public fun assigneeIds(value: kotlin.collections.List<kotlin.String>): TaskPatch = apply { set("assigneeIds", value, kotlinx.serialization.builtins.ListSerializer(kotlinx.serialization.serializer<kotlin.String>())) }
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
    /** Column `projectId`. */
    public val projectId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("projectId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `status`. */
    public val status: tools.jazz.Column<TaskStatus> =
        tools.jazz.Column("status", TaskStatus.serializer())
    /** Column `score`. */
    public val score: tools.jazz.Column<kotlin.Int> =
        tools.jazz.Column("score", kotlinx.serialization.serializer<kotlin.Int>())
    /** Column `tags`. */
    public val tags: tools.jazz.Column<kotlin.collections.List<kotlin.String>> =
        tools.jazz.Column("tags", kotlinx.serialization.builtins.ListSerializer(kotlinx.serialization.serializer<kotlin.String>()))
    /** Column `dueAt`. Compared as stored, before its transform. */
    public val dueAt: tools.jazz.Column<kotlin.Long?> =
        tools.jazz.Column("dueAt", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Long>()))
    /** Column `meta`. */
    public val meta: tools.jazz.Column<kotlinx.serialization.json.JsonElement?> =
        tools.jazz.Column("meta", tools.jazz.nullable(kotlinx.serialization.serializer<kotlinx.serialization.json.JsonElement>()))
    /** Column `extra`. */
    public val extra: tools.jazz.Column<kotlinx.serialization.json.JsonElement?> =
        tools.jazz.Column("extra", tools.jazz.nullable(kotlinx.serialization.serializer<kotlinx.serialization.json.JsonElement>()))
    /** Column `assigneeIds`. */
    public val assigneeIds: tools.jazz.Column<kotlin.collections.List<kotlin.String>> =
        tools.jazz.Column("assigneeIds", kotlinx.serialization.builtins.ListSerializer(kotlinx.serialization.serializer<kotlin.String>()))
    override fun fromStored(row: kotlinx.serialization.json.JsonObject, implementations: kotlin.Any?): kotlinx.serialization.json.JsonObject {
        val computed = implementations as JazzComputed
        val out = row.toMutableMap()
        tools.jazz.transformColumn(out, "dueAt", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Long>()), tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) { it?.let(computed::isoDateView) }
        tools.jazz.computeField(out, "priority", TaskPriority.serializer(), kotlin.collections.listOf("score" to kotlinx.serialization.serializer<kotlin.Int>(), "status" to TaskStatus.serializer())) { args -> computed.tasksPriority(args[0] as kotlin.Int, args[1] as TaskStatus) }
        tools.jazz.computeField(out, "label", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()), kotlin.collections.listOf("title" to kotlinx.serialization.serializer<kotlin.String>(), "dueAt" to tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))) { args -> computed.tasksLabel(args[0] as kotlin.String, args[1] as kotlin.String?) }
        return kotlinx.serialization.json.JsonObject(out)
    }
    override fun toStored(values: kotlinx.serialization.json.JsonObject, implementations: kotlin.Any?): kotlinx.serialization.json.JsonObject {
        val computed = implementations as JazzComputed
        val out = values.toMutableMap()
        tools.jazz.transformColumn(out, "dueAt", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()), tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.Long>())) { it?.let(computed::isoDateStored) }
        return kotlinx.serialization.json.JsonObject(out)
    }
    /** Relation `assignees`: the `people` rows `assigneeIds` references. */
    public val assignees: tools.jazz.Many<Task, Person> =
        tools.jazz.Many("assignees") { People }
    /** Relation `project`: the `projects` row `projectId` references. */
    public val project: tools.jazz.One<Task, Project> =
        tools.jazz.One("project") { Projects }
}
