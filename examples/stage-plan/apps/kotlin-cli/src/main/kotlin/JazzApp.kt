// JazzApp.kt: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 2. Regenerate it after changing jazz.app.json.
@file:kotlin.Suppress("unused", "RedundantVisibilityModifier")

package jazzapp

/** `sha256:…` of the `jazz.app.json` these bindings were generated from. */
public const val SOURCE: kotlin.String = "sha256:1adfb9c805c18f17655799f7003dd9ac6f75950914995d9c25b0fbcaad7a545f";

/** The app's `jazz.app.json`: its schema, every earlier version and the migrations between them. */
public val APP: kotlin.String = kotlin.arrayOf(
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
public suspend fun open(config: tools.jazz.JazzConfig): tools.jazz.Jazz =
    tools.jazz.Jazz.open(APP, config)

/** The values of column `activity.kind`. */
@kotlinx.serialization.Serializable
public enum class ActivityKind {
    @kotlinx.serialization.SerialName("created") Created,
    @kotlinx.serialization.SerialName("moved") Moved,
    @kotlinx.serialization.SerialName("assigned") Assigned,
    @kotlinx.serialization.SerialName("renamed") Renamed,
    @kotlinx.serialization.SerialName("commented") Commented,
    @kotlinx.serialization.SerialName("deleted") Deleted,
}

/** The values of column `showCrew.role`. */
@kotlinx.serialization.Serializable
public enum class ShowCrewRole {
    @kotlinx.serialization.SerialName("chief") Chief,
    @kotlinx.serialization.SerialName("crew") Crew,
}

/** The values of column `tasks.status`. */
@kotlinx.serialization.Serializable
public enum class TaskStatus {
    @kotlinx.serialization.SerialName("todo") Todo,
    @kotlinx.serialization.SerialName("doing") Doing,
    @kotlinx.serialization.SerialName("done") Done,
    @kotlinx.serialization.SerialName("blocked") Blocked,
}

/** A row of `activity`. */
@kotlinx.serialization.Serializable
public data class Activity(
    val id: kotlin.String,
    val showId: kotlin.String,
    val taskId: kotlin.String,
    val actorId: kotlin.String,
    val kind: ActivityKind,
    val detail: kotlin.String? = null,
)

/** A new row of `activity`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewActivity(
    val showId: kotlin.String,
    val taskId: kotlin.String,
    val actorId: kotlin.String,
    val kind: ActivityKind,
    val detail: kotlin.String? = null,
)

/** Changes to a row of `activity`: columns not set are left as they are. */
public class ActivityPatch : tools.jazz.Patch() {
    /** Sets `showId`. */
    public fun showId(value: kotlin.String): ActivityPatch = apply { set("showId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `taskId`. */
    public fun taskId(value: kotlin.String): ActivityPatch = apply { set("taskId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `actorId`. */
    public fun actorId(value: kotlin.String): ActivityPatch = apply { set("actorId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `kind`. */
    public fun kind(value: ActivityKind): ActivityPatch = apply { set("kind", value, ActivityKind.serializer()) }
    /** Sets `detail`. */
    public fun detail(value: kotlin.String?): ActivityPatch = apply { set("detail", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
}

/** Table `activity`: its columns, and queries with `query()`. */
public object ActivityTable : tools.jazz.Table<Activity, NewActivity, ActivityPatch>(
    "activity",
    Activity.serializer(),
    NewActivity.serializer(),
    ::ActivityPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `showId`. */
    public val showId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("showId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `taskId`. */
    public val taskId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("taskId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `actorId`. */
    public val actorId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("actorId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `kind`. */
    public val kind: tools.jazz.Column<ActivityKind> =
        tools.jazz.Column("kind", ActivityKind.serializer())
    /** Column `detail`. */
    public val detail: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("detail", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
    /** Relation `actor`: the `crew` row `actorId` references. */
    public val actor: tools.jazz.One<Activity, Crew> =
        tools.jazz.One("actor") { CrewTable }
    /** Relation `show`: the `shows` row `showId` references. */
    public val show: tools.jazz.One<Activity, Show> =
        tools.jazz.One("show") { Shows }
    /** Relation `task`: the `tasks` row `taskId` references. */
    public val task: tools.jazz.One<Activity, Task> =
        tools.jazz.One("task") { Tasks }
}

/** A row of `checklistItems`. */
@kotlinx.serialization.Serializable
public data class ChecklistItem(
    val id: kotlin.String,
    val title: kotlin.String,
    val done: kotlin.Boolean,
    val ownerAccount: kotlin.String,
)

/** A new row of `checklistItems`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewChecklistItem(
    val title: kotlin.String,
    val done: kotlin.Boolean,
    val ownerAccount: kotlin.String,
)

/** Changes to a row of `checklistItems`: columns not set are left as they are. */
public class ChecklistItemPatch : tools.jazz.Patch() {
    /** Sets `title`. */
    public fun title(value: kotlin.String): ChecklistItemPatch = apply { set("title", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `done`. */
    public fun done(value: kotlin.Boolean): ChecklistItemPatch = apply { set("done", value, kotlinx.serialization.serializer<kotlin.Boolean>()) }
    /** Sets `ownerAccount`. */
    public fun ownerAccount(value: kotlin.String): ChecklistItemPatch = apply { set("ownerAccount", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `checklistItems`: its columns, and queries with `query()`. */
public object ChecklistItems : tools.jazz.Table<ChecklistItem, NewChecklistItem, ChecklistItemPatch>(
    "checklistItems",
    ChecklistItem.serializer(),
    NewChecklistItem.serializer(),
    ::ChecklistItemPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `title`. */
    public val title: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("title", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `done`. */
    public val done: tools.jazz.Column<kotlin.Boolean> =
        tools.jazz.Column("done", kotlinx.serialization.serializer<kotlin.Boolean>())
    /** Column `ownerAccount`. */
    public val ownerAccount: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("ownerAccount", kotlinx.serialization.serializer<kotlin.String>())
}

/** A row of `comments`. */
@kotlinx.serialization.Serializable
public data class Comment(
    val id: kotlin.String,
    val taskId: kotlin.String,
    val authorId: kotlin.String,
    val body: kotlin.String,
)

/** A new row of `comments`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewComment(
    val taskId: kotlin.String,
    val authorId: kotlin.String,
    val body: kotlin.String,
)

/** Changes to a row of `comments`: columns not set are left as they are. */
public class CommentPatch : tools.jazz.Patch() {
    /** Sets `taskId`. */
    public fun taskId(value: kotlin.String): CommentPatch = apply { set("taskId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `authorId`. */
    public fun authorId(value: kotlin.String): CommentPatch = apply { set("authorId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `body`. */
    public fun body(value: kotlin.String): CommentPatch = apply { set("body", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `comments`: its columns, and queries with `query()`. */
public object Comments : tools.jazz.Table<Comment, NewComment, CommentPatch>(
    "comments",
    Comment.serializer(),
    NewComment.serializer(),
    ::CommentPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `taskId`. */
    public val taskId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("taskId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `authorId`. */
    public val authorId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("authorId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `body`. */
    public val body: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("body", kotlinx.serialization.serializer<kotlin.String>())
    /** Relation `author`: the `crew` row `authorId` references. */
    public val author: tools.jazz.One<Comment, Crew> =
        tools.jazz.One("author") { CrewTable }
    /** Relation `task`: the `tasks` row `taskId` references. */
    public val task: tools.jazz.One<Comment, Task> =
        tools.jazz.One("task") { Tasks }
}

/** A row of `crew`. */
@kotlinx.serialization.Serializable
public data class Crew(
    val id: kotlin.String,
    val account: kotlin.String,
    val name: kotlin.String,
)

/** A new row of `crew`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewCrew(
    val account: kotlin.String,
    val name: kotlin.String,
)

/** Changes to a row of `crew`: columns not set are left as they are. */
public class CrewPatch : tools.jazz.Patch() {
    /** Sets `account`. */
    public fun account(value: kotlin.String): CrewPatch = apply { set("account", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `name`. */
    public fun name(value: kotlin.String): CrewPatch = apply { set("name", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `crew`: its columns, and queries with `query()`. */
public object CrewTable : tools.jazz.Table<Crew, NewCrew, CrewPatch>(
    "crew",
    Crew.serializer(),
    NewCrew.serializer(),
    ::CrewPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `account`. */
    public val account: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("account", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `name`. */
    public val name: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("name", kotlinx.serialization.serializer<kotlin.String>())
    /** Relation `assignedTasks`: the `tasks` rows whose `assignee` is this row. */
    public val assignedTasks: tools.jazz.Many<Crew, Task> =
        tools.jazz.Many("assignedTasks") { Tasks }
    /** Relation `memberships`: the `showCrew` rows whose `crew` is this row. */
    public val memberships: tools.jazz.Many<Crew, ShowCrew> =
        tools.jazz.Many("memberships") { ShowCrewTable }
}

/** A row of `showCrew`. */
@kotlinx.serialization.Serializable
public data class ShowCrew(
    val id: kotlin.String,
    val showId: kotlin.String,
    val crewId: kotlin.String,
    val account: kotlin.String,
    val role: ShowCrewRole,
    val inviteCode: kotlin.String? = null,
)

/** A new row of `showCrew`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewShowCrew(
    val showId: kotlin.String,
    val crewId: kotlin.String,
    val account: kotlin.String,
    val role: ShowCrewRole,
    val inviteCode: kotlin.String? = null,
)

/** Changes to a row of `showCrew`: columns not set are left as they are. */
public class ShowCrewPatch : tools.jazz.Patch() {
    /** Sets `showId`. */
    public fun showId(value: kotlin.String): ShowCrewPatch = apply { set("showId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `crewId`. */
    public fun crewId(value: kotlin.String): ShowCrewPatch = apply { set("crewId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `account`. */
    public fun account(value: kotlin.String): ShowCrewPatch = apply { set("account", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `role`. */
    public fun role(value: ShowCrewRole): ShowCrewPatch = apply { set("role", value, ShowCrewRole.serializer()) }
    /** Sets `inviteCode`. */
    public fun inviteCode(value: kotlin.String?): ShowCrewPatch = apply { set("inviteCode", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
}

/** Table `showCrew`: its columns, and queries with `query()`. */
public object ShowCrewTable : tools.jazz.Table<ShowCrew, NewShowCrew, ShowCrewPatch>(
    "showCrew",
    ShowCrew.serializer(),
    NewShowCrew.serializer(),
    ::ShowCrewPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `showId`. */
    public val showId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("showId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `crewId`. */
    public val crewId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("crewId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `account`. */
    public val account: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("account", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `role`. */
    public val role: tools.jazz.Column<ShowCrewRole> =
        tools.jazz.Column("role", ShowCrewRole.serializer())
    /** Column `inviteCode`. */
    public val inviteCode: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("inviteCode", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
    /** Relation `crew`: the `crew` row `crewId` references. */
    public val crew: tools.jazz.One<ShowCrew, Crew> =
        tools.jazz.One("crew") { CrewTable }
    /** Relation `show`: the `shows` row `showId` references. */
    public val show: tools.jazz.One<ShowCrew, Show> =
        tools.jazz.One("show") { Shows }
}

/** A row of `showInvites`. */
@kotlinx.serialization.Serializable
public data class ShowInvite(
    val id: kotlin.String,
    val showId: kotlin.String,
    val code: kotlin.String,
)

/** A new row of `showInvites`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewShowInvite(
    val showId: kotlin.String,
    val code: kotlin.String,
)

/** Changes to a row of `showInvites`: columns not set are left as they are. */
public class ShowInvitePatch : tools.jazz.Patch() {
    /** Sets `showId`. */
    public fun showId(value: kotlin.String): ShowInvitePatch = apply { set("showId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `code`. */
    public fun code(value: kotlin.String): ShowInvitePatch = apply { set("code", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `showInvites`: its columns, and queries with `query()`. */
public object ShowInvites : tools.jazz.Table<ShowInvite, NewShowInvite, ShowInvitePatch>(
    "showInvites",
    ShowInvite.serializer(),
    NewShowInvite.serializer(),
    ::ShowInvitePatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `showId`. */
    public val showId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("showId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `code`. */
    public val code: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("code", kotlinx.serialization.serializer<kotlin.String>())
    /** Relation `show`: the `shows` row `showId` references. */
    public val show: tools.jazz.One<ShowInvite, Show> =
        tools.jazz.One("show") { Shows }
}

/** A row of `shows`. */
@kotlinx.serialization.Serializable
public data class Show(
    val id: kotlin.String,
    val name: kotlin.String,
    val venue: kotlin.String,
    val date: kotlin.String,
    val doors: kotlin.String,
    val chiefAccount: kotlin.String,
)

/** A new row of `shows`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewShow(
    val name: kotlin.String,
    val venue: kotlin.String,
    val date: kotlin.String,
    val doors: kotlin.String,
    val chiefAccount: kotlin.String,
)

/** Changes to a row of `shows`: columns not set are left as they are. */
public class ShowPatch : tools.jazz.Patch() {
    /** Sets `name`. */
    public fun name(value: kotlin.String): ShowPatch = apply { set("name", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `venue`. */
    public fun venue(value: kotlin.String): ShowPatch = apply { set("venue", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `date`. */
    public fun date(value: kotlin.String): ShowPatch = apply { set("date", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `doors`. */
    public fun doors(value: kotlin.String): ShowPatch = apply { set("doors", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `chiefAccount`. */
    public fun chiefAccount(value: kotlin.String): ShowPatch = apply { set("chiefAccount", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `shows`: its columns, and queries with `query()`. */
public object Shows : tools.jazz.Table<Show, NewShow, ShowPatch>(
    "shows",
    Show.serializer(),
    NewShow.serializer(),
    ::ShowPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `name`. */
    public val name: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("name", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `venue`. */
    public val venue: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("venue", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `date`. */
    public val date: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("date", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `doors`. */
    public val doors: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("doors", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `chiefAccount`. */
    public val chiefAccount: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("chiefAccount", kotlinx.serialization.serializer<kotlin.String>())
    /** Relation `activity`: the `activity` rows whose `show` is this row. */
    public val activity: tools.jazz.Many<Show, Activity> =
        tools.jazz.Many("activity") { ActivityTable }
    /** Relation `crew`: the `showCrew` rows whose `show` is this row. */
    public val crew: tools.jazz.Many<Show, ShowCrew> =
        tools.jazz.Many("crew") { ShowCrewTable }
    /** Relation `invites`: the `showInvites` rows whose `show` is this row. */
    public val invites: tools.jazz.Many<Show, ShowInvite> =
        tools.jazz.Many("invites") { ShowInvites }
    /** Relation `tasks`: the `tasks` rows whose `show` is this row. */
    public val tasks: tools.jazz.Many<Show, Task> =
        tools.jazz.Many("tasks") { Tasks }
}

/** A row of `tasks`. */
@kotlinx.serialization.Serializable
public data class Task(
    val id: kotlin.String,
    val showId: kotlin.String,
    val title: kotlin.String,
    val status: TaskStatus,
    val assigneeId: kotlin.String? = null,
    val notes: kotlin.String? = null,
    val rank: kotlin.Double,
)

/** A new row of `tasks`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewTask(
    val showId: kotlin.String,
    val title: kotlin.String,
    val status: TaskStatus,
    val assigneeId: kotlin.String? = null,
    val notes: kotlin.String? = null,
    val rank: kotlin.Double,
)

/** Changes to a row of `tasks`: columns not set are left as they are. */
public class TaskPatch : tools.jazz.Patch() {
    /** Sets `showId`. */
    public fun showId(value: kotlin.String): TaskPatch = apply { set("showId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `title`. */
    public fun title(value: kotlin.String): TaskPatch = apply { set("title", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `status`. */
    public fun status(value: TaskStatus): TaskPatch = apply { set("status", value, TaskStatus.serializer()) }
    /** Sets `assigneeId`. */
    public fun assigneeId(value: kotlin.String?): TaskPatch = apply { set("assigneeId", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
    /** Sets `notes`. */
    public fun notes(value: kotlin.String?): TaskPatch = apply { set("notes", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
    /** Sets `rank`. */
    public fun rank(value: kotlin.Double): TaskPatch = apply { set("rank", value, kotlinx.serialization.serializer<kotlin.Double>()) }
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
    /** Column `showId`. */
    public val showId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("showId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `title`. */
    public val title: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("title", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `status`. */
    public val status: tools.jazz.Column<TaskStatus> =
        tools.jazz.Column("status", TaskStatus.serializer())
    /** Column `assigneeId`. */
    public val assigneeId: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("assigneeId", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
    /** Column `notes`. */
    public val notes: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("notes", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
    /** Column `rank`. */
    public val rank: tools.jazz.Column<kotlin.Double> =
        tools.jazz.Column("rank", kotlinx.serialization.serializer<kotlin.Double>())
    /** Relation `activity`: the `activity` rows whose `task` is this row. */
    public val activity: tools.jazz.Many<Task, Activity> =
        tools.jazz.Many("activity") { ActivityTable }
    /** Relation `assignee`: the `crew` row `assigneeId` references. */
    public val assignee: tools.jazz.One<Task, Crew> =
        tools.jazz.One("assignee") { CrewTable }
    /** Relation `comments`: the `comments` rows whose `task` is this row. */
    public val comments: tools.jazz.Many<Task, Comment> =
        tools.jazz.Many("comments") { Comments }
    /** Relation `show`: the `shows` row `showId` references. */
    public val show: tools.jazz.One<Task, Show> =
        tools.jazz.One("show") { Shows }
}
