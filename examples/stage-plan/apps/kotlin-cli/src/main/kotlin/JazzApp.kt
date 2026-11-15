// JazzApp.kt: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Regenerate it after changing jazz.app.json.
@file:Suppress("unused", "RedundantVisibilityModifier")

package jazzapp

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.JsonElement
import tools.jazz.Column
import tools.jazz.Jazz
import tools.jazz.JazzBigInt
import tools.jazz.JazzBytes
import tools.jazz.JazzConfig
import tools.jazz.Patch
import tools.jazz.Table

/** `sha256:…` of the `jazz.app.json` these bindings were generated from. */
public const val SOURCE: String = "sha256:1adfb9c805c18f17655799f7003dd9ac6f75950914995d9c25b0fbcaad7a545f";

/** The app's `jazz.app.json`: its schema, every earlier version and the migrations between them. */
public val APP: String = arrayOf(
    "{\n",
    "  \"\$schema\": \"https://jazz.tools/schemas/jazz.app.schema.json\",\n",
    "  \"format\": 1,\n",
    "  \"tables\": {\n",
    "    \"activity\": {\n",
    "      \"columns\": {\n",
    "        \"showId\": \"uuid\",\n",
    "        \"taskId\": \"uuid\",\n",
    "        \"actorId\": \"uuid\",\n",
    "        \"kind\": { \"enum\": [\"created\", \"moved\", \"assigned\", \"renamed\", \"commented\", \"deleted\"] },\n",
    "        \"detail\": \"string?\"\n",
    "      },\n",
    "      \"relations\": {\n",
    "        \"actor\": { \"rel\": \"crew\", \"column\": \"actorId\" },\n",
    "        \"show\": { \"rel\": \"shows\", \"column\": \"showId\" },\n",
    "        \"task\": { \"rel\": \"tasks\", \"column\": \"taskId\" }\n",
    "      }\n",
    "    },\n",
    "    \"checklistItems\": {\n",
    "      \"columns\": { \"title\": \"string\", \"done\": \"boolean\", \"ownerAccount\": \"uuid\" }\n",
    "    },\n",
    "    \"comments\": {\n",
    "      \"columns\": { \"taskId\": \"uuid\", \"authorId\": \"uuid\", \"body\": \"string\" },\n",
    "      \"relations\": {\n",
    "        \"author\": { \"rel\": \"crew\", \"column\": \"authorId\" },\n",
    "        \"task\": { \"rel\": \"tasks\", \"column\": \"taskId\" }\n",
    "      }\n",
    "    },\n",
    "    \"crew\": {\n",
    "      \"columns\": { \"account\": \"uuid\", \"name\": \"string\" },\n",
    "      \"relations\": {\n",
    "        \"assignedTasks\": { \"reverse\": \"tasks\", \"relation\": \"assignee\" },\n",
    "        \"memberships\": { \"reverse\": \"showCrew\", \"relation\": \"crew\" }\n",
    "      }\n",
    "    },\n",
    "    \"showCrew\": {\n",
    "      \"columns\": {\n",
    "        \"showId\": \"uuid\",\n",
    "        \"crewId\": \"uuid\",\n",
    "        \"account\": \"uuid\",\n",
    "        \"role\": { \"enum\": [\"chief\", \"crew\"] },\n",
    "        \"inviteCode\": \"string?\"\n",
    "      },\n",
    "      \"relations\": {\n",
    "        \"crew\": { \"rel\": \"crew\", \"column\": \"crewId\" },\n",
    "        \"show\": { \"rel\": \"shows\", \"column\": \"showId\" }\n",
    "      }\n",
    "    },\n",
    "    \"showInvites\": {\n",
    "      \"columns\": { \"showId\": \"uuid\", \"code\": \"string\" },\n",
    "      \"relations\": { \"show\": { \"rel\": \"shows\", \"column\": \"showId\" } }\n",
    "    },\n",
    "    \"shows\": {\n",
    "      \"columns\": {\n",
    "        \"name\": \"string\",\n",
    "        \"venue\": \"string\",\n",
    "        \"date\": \"string\",\n",
    "        \"doors\": \"string\",\n",
    "        \"chiefAccount\": \"uuid\"\n",
    "      },\n",
    "      \"relations\": {\n",
    "        \"activity\": { \"reverse\": \"activity\", \"relation\": \"show\" },\n",
    "        \"crew\": { \"reverse\": \"showCrew\", \"relation\": \"show\" },\n",
    "        \"invites\": { \"reverse\": \"showInvites\", \"relation\": \"show\" },\n",
    "        \"tasks\": { \"reverse\": \"tasks\", \"relation\": \"show\" }\n",
    "      }\n",
    "    },\n",
    "    \"tasks\": {\n",
    "      \"columns\": {\n",
    "        \"showId\": \"uuid\",\n",
    "        \"title\": \"string\",\n",
    "        \"status\": { \"enum\": [\"todo\", \"doing\", \"done\", \"blocked\"] },\n",
    "        \"assigneeId\": \"uuid?\",\n",
    "        \"notes\": { \"type\": \"string\", \"optional\": true, \"merge\": \"text\" },\n",
    "        \"rank\": \"float\"\n",
    "      },\n",
    "      \"relations\": {\n",
    "        \"activity\": { \"reverse\": \"activity\", \"relation\": \"task\" },\n",
    "        \"assignee\": { \"rel\": \"crew\", \"column\": \"assigneeId\" },\n",
    "        \"comments\": { \"reverse\": \"comments\", \"relation\": \"task\" },\n",
    "        \"show\": { \"rel\": \"shows\", \"column\": \"showId\" }\n",
    "      }\n",
    "    }\n",
    "  }\n",
    "}\n",
).joinToString("")

/**
 * Opens the app. A backend (`JazzConfig.permissions` set to its
 * `jazz.permissions.json`) first publishes the app's permissions bundle
 * and pins the session it acts in to it.
 */
public suspend fun open(config: JazzConfig): Jazz = Jazz.open(APP, config)

/** The values of column `activity.kind`. */
@Serializable
public enum class ActivityKind {
    @SerialName("created") Created,
    @SerialName("moved") Moved,
    @SerialName("assigned") Assigned,
    @SerialName("renamed") Renamed,
    @SerialName("commented") Commented,
    @SerialName("deleted") Deleted,
}

/** The values of column `showCrew.role`. */
@Serializable
public enum class ShowCrewRole {
    @SerialName("chief") Chief,
    @SerialName("crew") Crew,
}

/** The values of column `tasks.status`. */
@Serializable
public enum class TaskStatus {
    @SerialName("todo") Todo,
    @SerialName("doing") Doing,
    @SerialName("done") Done,
    @SerialName("blocked") Blocked,
}

/** A row of `activity`. */
@Serializable
public data class Activity(
    val id: String,
    val showId: String,
    val taskId: String,
    val actorId: String,
    val kind: ActivityKind,
    val detail: String? = null,
)

/** A new row of `activity`: leave out a column to store its default (or `null`). */
@Serializable
public data class ActivityInsert(
    val showId: String,
    val taskId: String,
    val actorId: String,
    val kind: ActivityKind,
    val detail: String? = null,
)

/** Changes to a row of `activity`: columns not set are left as they are. */
public class ActivityPatch : Patch() {
    /** Sets `showId`. */
    public fun showId(value: String): ActivityPatch = apply { set("showId", value, String.serializer()) }
    /** Sets `taskId`. */
    public fun taskId(value: String): ActivityPatch = apply { set("taskId", value, String.serializer()) }
    /** Sets `actorId`. */
    public fun actorId(value: String): ActivityPatch = apply { set("actorId", value, String.serializer()) }
    /** Sets `kind`. */
    public fun kind(value: ActivityKind): ActivityPatch = apply { set("kind", value, ActivityKind.serializer()) }
    /** Sets `detail`. */
    public fun detail(value: String?): ActivityPatch = apply { set("detail", value, String.serializer().nullable) }
}

/** Table `activity`: its columns, and queries with `query()`. */
public object ActivityTable : Table<Activity, ActivityInsert, ActivityPatch>("activity", Activity.serializer(), ActivityInsert.serializer(), ::ActivityPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `showId`. */
    public val showId: Column<String> = Column("showId", String.serializer())
    /** Column `taskId`. */
    public val taskId: Column<String> = Column("taskId", String.serializer())
    /** Column `actorId`. */
    public val actorId: Column<String> = Column("actorId", String.serializer())
    /** Column `kind`. */
    public val kind: Column<ActivityKind> = Column("kind", ActivityKind.serializer())
    /** Column `detail`. */
    public val detail: Column<String?> = Column("detail", String.serializer().nullable)
}

/** A row of `checklistItems`. */
@Serializable
public data class ChecklistItem(
    val id: String,
    val title: String,
    val done: Boolean,
    val ownerAccount: String,
)

/** A new row of `checklistItems`: leave out a column to store its default (or `null`). */
@Serializable
public data class ChecklistItemInsert(
    val title: String,
    val done: Boolean,
    val ownerAccount: String,
)

/** Changes to a row of `checklistItems`: columns not set are left as they are. */
public class ChecklistItemPatch : Patch() {
    /** Sets `title`. */
    public fun title(value: String): ChecklistItemPatch = apply { set("title", value, String.serializer()) }
    /** Sets `done`. */
    public fun done(value: Boolean): ChecklistItemPatch = apply { set("done", value, Boolean.serializer()) }
    /** Sets `ownerAccount`. */
    public fun ownerAccount(value: String): ChecklistItemPatch = apply { set("ownerAccount", value, String.serializer()) }
}

/** Table `checklistItems`: its columns, and queries with `query()`. */
public object ChecklistItems : Table<ChecklistItem, ChecklistItemInsert, ChecklistItemPatch>("checklistItems", ChecklistItem.serializer(), ChecklistItemInsert.serializer(), ::ChecklistItemPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `title`. */
    public val title: Column<String> = Column("title", String.serializer())
    /** Column `done`. */
    public val done: Column<Boolean> = Column("done", Boolean.serializer())
    /** Column `ownerAccount`. */
    public val ownerAccount: Column<String> = Column("ownerAccount", String.serializer())
}

/** A row of `comments`. */
@Serializable
public data class Comment(
    val id: String,
    val taskId: String,
    val authorId: String,
    val body: String,
)

/** A new row of `comments`: leave out a column to store its default (or `null`). */
@Serializable
public data class CommentInsert(
    val taskId: String,
    val authorId: String,
    val body: String,
)

/** Changes to a row of `comments`: columns not set are left as they are. */
public class CommentPatch : Patch() {
    /** Sets `taskId`. */
    public fun taskId(value: String): CommentPatch = apply { set("taskId", value, String.serializer()) }
    /** Sets `authorId`. */
    public fun authorId(value: String): CommentPatch = apply { set("authorId", value, String.serializer()) }
    /** Sets `body`. */
    public fun body(value: String): CommentPatch = apply { set("body", value, String.serializer()) }
}

/** Table `comments`: its columns, and queries with `query()`. */
public object Comments : Table<Comment, CommentInsert, CommentPatch>("comments", Comment.serializer(), CommentInsert.serializer(), ::CommentPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `taskId`. */
    public val taskId: Column<String> = Column("taskId", String.serializer())
    /** Column `authorId`. */
    public val authorId: Column<String> = Column("authorId", String.serializer())
    /** Column `body`. */
    public val body: Column<String> = Column("body", String.serializer())
}

/** A row of `crew`. */
@Serializable
public data class Crew(
    val id: String,
    val account: String,
    val name: String,
)

/** A new row of `crew`: leave out a column to store its default (or `null`). */
@Serializable
public data class CrewInsert(
    val account: String,
    val name: String,
)

/** Changes to a row of `crew`: columns not set are left as they are. */
public class CrewPatch : Patch() {
    /** Sets `account`. */
    public fun account(value: String): CrewPatch = apply { set("account", value, String.serializer()) }
    /** Sets `name`. */
    public fun name(value: String): CrewPatch = apply { set("name", value, String.serializer()) }
}

/** Table `crew`: its columns, and queries with `query()`. */
public object CrewTable : Table<Crew, CrewInsert, CrewPatch>("crew", Crew.serializer(), CrewInsert.serializer(), ::CrewPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `account`. */
    public val account: Column<String> = Column("account", String.serializer())
    /** Column `name`. */
    public val name: Column<String> = Column("name", String.serializer())
}

/** A row of `showCrew`. */
@Serializable
public data class ShowCrew(
    val id: String,
    val showId: String,
    val crewId: String,
    val account: String,
    val role: ShowCrewRole,
    val inviteCode: String? = null,
)

/** A new row of `showCrew`: leave out a column to store its default (or `null`). */
@Serializable
public data class ShowCrewInsert(
    val showId: String,
    val crewId: String,
    val account: String,
    val role: ShowCrewRole,
    val inviteCode: String? = null,
)

/** Changes to a row of `showCrew`: columns not set are left as they are. */
public class ShowCrewPatch : Patch() {
    /** Sets `showId`. */
    public fun showId(value: String): ShowCrewPatch = apply { set("showId", value, String.serializer()) }
    /** Sets `crewId`. */
    public fun crewId(value: String): ShowCrewPatch = apply { set("crewId", value, String.serializer()) }
    /** Sets `account`. */
    public fun account(value: String): ShowCrewPatch = apply { set("account", value, String.serializer()) }
    /** Sets `role`. */
    public fun role(value: ShowCrewRole): ShowCrewPatch = apply { set("role", value, ShowCrewRole.serializer()) }
    /** Sets `inviteCode`. */
    public fun inviteCode(value: String?): ShowCrewPatch = apply { set("inviteCode", value, String.serializer().nullable) }
}

/** Table `showCrew`: its columns, and queries with `query()`. */
public object ShowCrewTable : Table<ShowCrew, ShowCrewInsert, ShowCrewPatch>("showCrew", ShowCrew.serializer(), ShowCrewInsert.serializer(), ::ShowCrewPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `showId`. */
    public val showId: Column<String> = Column("showId", String.serializer())
    /** Column `crewId`. */
    public val crewId: Column<String> = Column("crewId", String.serializer())
    /** Column `account`. */
    public val account: Column<String> = Column("account", String.serializer())
    /** Column `role`. */
    public val role: Column<ShowCrewRole> = Column("role", ShowCrewRole.serializer())
    /** Column `inviteCode`. */
    public val inviteCode: Column<String?> = Column("inviteCode", String.serializer().nullable)
}

/** A row of `showInvites`. */
@Serializable
public data class ShowInvite(
    val id: String,
    val showId: String,
    val code: String,
)

/** A new row of `showInvites`: leave out a column to store its default (or `null`). */
@Serializable
public data class ShowInviteInsert(
    val showId: String,
    val code: String,
)

/** Changes to a row of `showInvites`: columns not set are left as they are. */
public class ShowInvitePatch : Patch() {
    /** Sets `showId`. */
    public fun showId(value: String): ShowInvitePatch = apply { set("showId", value, String.serializer()) }
    /** Sets `code`. */
    public fun code(value: String): ShowInvitePatch = apply { set("code", value, String.serializer()) }
}

/** Table `showInvites`: its columns, and queries with `query()`. */
public object ShowInvites : Table<ShowInvite, ShowInviteInsert, ShowInvitePatch>("showInvites", ShowInvite.serializer(), ShowInviteInsert.serializer(), ::ShowInvitePatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `showId`. */
    public val showId: Column<String> = Column("showId", String.serializer())
    /** Column `code`. */
    public val code: Column<String> = Column("code", String.serializer())
}

/** A row of `shows`. */
@Serializable
public data class Show(
    val id: String,
    val name: String,
    val venue: String,
    val date: String,
    val doors: String,
    val chiefAccount: String,
)

/** A new row of `shows`: leave out a column to store its default (or `null`). */
@Serializable
public data class ShowInsert(
    val name: String,
    val venue: String,
    val date: String,
    val doors: String,
    val chiefAccount: String,
)

/** Changes to a row of `shows`: columns not set are left as they are. */
public class ShowPatch : Patch() {
    /** Sets `name`. */
    public fun name(value: String): ShowPatch = apply { set("name", value, String.serializer()) }
    /** Sets `venue`. */
    public fun venue(value: String): ShowPatch = apply { set("venue", value, String.serializer()) }
    /** Sets `date`. */
    public fun date(value: String): ShowPatch = apply { set("date", value, String.serializer()) }
    /** Sets `doors`. */
    public fun doors(value: String): ShowPatch = apply { set("doors", value, String.serializer()) }
    /** Sets `chiefAccount`. */
    public fun chiefAccount(value: String): ShowPatch = apply { set("chiefAccount", value, String.serializer()) }
}

/** Table `shows`: its columns, and queries with `query()`. */
public object Shows : Table<Show, ShowInsert, ShowPatch>("shows", Show.serializer(), ShowInsert.serializer(), ::ShowPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `name`. */
    public val name: Column<String> = Column("name", String.serializer())
    /** Column `venue`. */
    public val venue: Column<String> = Column("venue", String.serializer())
    /** Column `date`. */
    public val date: Column<String> = Column("date", String.serializer())
    /** Column `doors`. */
    public val doors: Column<String> = Column("doors", String.serializer())
    /** Column `chiefAccount`. */
    public val chiefAccount: Column<String> = Column("chiefAccount", String.serializer())
}

/** A row of `tasks`. */
@Serializable
public data class Task(
    val id: String,
    val showId: String,
    val title: String,
    val status: TaskStatus,
    val assigneeId: String? = null,
    val notes: String? = null,
    val rank: Double,
)

/** A new row of `tasks`: leave out a column to store its default (or `null`). */
@Serializable
public data class TaskInsert(
    val showId: String,
    val title: String,
    val status: TaskStatus,
    val assigneeId: String? = null,
    val notes: String? = null,
    val rank: Double,
)

/** Changes to a row of `tasks`: columns not set are left as they are. */
public class TaskPatch : Patch() {
    /** Sets `showId`. */
    public fun showId(value: String): TaskPatch = apply { set("showId", value, String.serializer()) }
    /** Sets `title`. */
    public fun title(value: String): TaskPatch = apply { set("title", value, String.serializer()) }
    /** Sets `status`. */
    public fun status(value: TaskStatus): TaskPatch = apply { set("status", value, TaskStatus.serializer()) }
    /** Sets `assigneeId`. */
    public fun assigneeId(value: String?): TaskPatch = apply { set("assigneeId", value, String.serializer().nullable) }
    /** Sets `notes`. */
    public fun notes(value: String?): TaskPatch = apply { set("notes", value, String.serializer().nullable) }
    /** Sets `rank`. */
    public fun rank(value: Double): TaskPatch = apply { set("rank", value, Double.serializer()) }
}

/** Table `tasks`: its columns, and queries with `query()`. */
public object Tasks : Table<Task, TaskInsert, TaskPatch>("tasks", Task.serializer(), TaskInsert.serializer(), ::TaskPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `showId`. */
    public val showId: Column<String> = Column("showId", String.serializer())
    /** Column `title`. */
    public val title: Column<String> = Column("title", String.serializer())
    /** Column `status`. */
    public val status: Column<TaskStatus> = Column("status", TaskStatus.serializer())
    /** Column `assigneeId`. */
    public val assigneeId: Column<String?> = Column("assigneeId", String.serializer().nullable)
    /** Column `notes`. */
    public val notes: Column<String?> = Column("notes", String.serializer().nullable)
    /** Column `rank`. */
    public val rank: Column<Double> = Column("rank", Double.serializer())
}
