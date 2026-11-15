// JazzApp.kt: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Regenerate it after changing jazz.app.json.
@file:kotlin.Suppress("unused", "RedundantVisibilityModifier")

package jazzapp

/** `sha256:…` of the `jazz.app.json` these bindings were generated from. */
public const val SOURCE: kotlin.String = "sha256:e8cdc8c71ce4a4ec76e542704f1bbcf9be8c2f0f4928d79f93b7d7e36716bd5e";

/** The app's `jazz.app.json`: its schema, every earlier version and the migrations between them. */
public val APP: kotlin.String = kotlin.arrayOf(
    "{\n",
    "  \"\$schema\": \"https://jazz.tools/schemas/jazz.app.schema.json\",\n",
    "  \"format\": 1,\n",
    "  \"tables\": {\n",
    "    \"canvases\": {\n",
    "      \"columns\": { \"chatId\": \"uuid\" },\n",
    "      \"relations\": {\n",
    "        \"chat\": { \"rel\": \"chats\", \"column\": \"chatId\" },\n",
    "        \"strokesViaCanvas\": { \"reverse\": \"strokes\", \"relation\": \"canvas\" }\n",
    "      }\n",
    "    },\n",
    "    \"chatMembers\": {\n",
    "      \"columns\": { \"chatId\": \"uuid\", \"userId\": \"uuid\", \"joinCode\": \"string?\" },\n",
    "      \"relations\": { \"chat\": { \"rel\": \"chats\", \"column\": \"chatId\" } }\n",
    "    },\n",
    "    \"chats\": {\n",
    "      \"columns\": { \"name\": \"string?\", \"isPublic\": \"boolean\", \"joinCode\": \"string?\" },\n",
    "      \"relations\": {\n",
    "        \"canvasesViaChat\": { \"reverse\": \"canvases\", \"relation\": \"chat\" },\n",
    "        \"chatMembersViaChat\": { \"reverse\": \"chatMembers\", \"relation\": \"chat\" },\n",
    "        \"messagesViaChat\": { \"reverse\": \"messages\", \"relation\": \"chat\" }\n",
    "      }\n",
    "    },\n",
    "    \"messages\": {\n",
    "      \"columns\": { \"chatId\": \"uuid\", \"text\": \"string\", \"senderId\": \"uuid\" },\n",
    "      \"relations\": {\n",
    "        \"chat\": { \"rel\": \"chats\", \"column\": \"chatId\" },\n",
    "        \"reactionsViaMessage\": { \"reverse\": \"reactions\", \"relation\": \"message\" },\n",
    "        \"sender\": { \"rel\": \"profiles\", \"column\": \"senderId\" }\n",
    "      }\n",
    "    },\n",
    "    \"profiles\": {\n",
    "      \"columns\": { \"userId\": \"uuid\", \"name\": \"string\", \"avatar\": \"string?\" },\n",
    "      \"relations\": { \"messagesViaSender\": { \"reverse\": \"messages\", \"relation\": \"sender\" } }\n",
    "    },\n",
    "    \"reactions\": {\n",
    "      \"columns\": { \"messageId\": \"uuid\", \"userId\": \"uuid\", \"emoji\": \"string\" },\n",
    "      \"relations\": { \"message\": { \"rel\": \"messages\", \"column\": \"messageId\" } }\n",
    "    },\n",
    "    \"strokes\": {\n",
    "      \"columns\": { \"canvasId\": \"uuid\", \"color\": \"string\", \"width\": \"int\", \"pointsJson\": \"string\" },\n",
    "      \"relations\": { \"canvas\": { \"rel\": \"canvases\", \"column\": \"canvasId\" } }\n",
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

/** A row of `canvases`. */
@kotlinx.serialization.Serializable
public data class Canvas(
    val id: kotlin.String,
    val chatId: kotlin.String,
)

/** A new row of `canvases`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewCanvas(
    val chatId: kotlin.String,
)

/** Changes to a row of `canvases`: columns not set are left as they are. */
public class CanvasPatch : tools.jazz.Patch() {
    /** Sets `chatId`. */
    public fun chatId(value: kotlin.String): CanvasPatch = apply { set("chatId", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `canvases`: its columns, and queries with `query()`. */
public object Canvases : tools.jazz.Table<Canvas, NewCanvas, CanvasPatch>(
    "canvases",
    Canvas.serializer(),
    NewCanvas.serializer(),
    ::CanvasPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `chatId`. */
    public val chatId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("chatId", kotlinx.serialization.serializer<kotlin.String>())
}

/** A row of `chatMembers`. */
@kotlinx.serialization.Serializable
public data class ChatMember(
    val id: kotlin.String,
    val chatId: kotlin.String,
    val userId: kotlin.String,
    val joinCode: kotlin.String? = null,
)

/** A new row of `chatMembers`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewChatMember(
    val chatId: kotlin.String,
    val userId: kotlin.String,
    val joinCode: kotlin.String? = null,
)

/** Changes to a row of `chatMembers`: columns not set are left as they are. */
public class ChatMemberPatch : tools.jazz.Patch() {
    /** Sets `chatId`. */
    public fun chatId(value: kotlin.String): ChatMemberPatch = apply { set("chatId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `userId`. */
    public fun userId(value: kotlin.String): ChatMemberPatch = apply { set("userId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `joinCode`. */
    public fun joinCode(value: kotlin.String?): ChatMemberPatch = apply { set("joinCode", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
}

/** Table `chatMembers`: its columns, and queries with `query()`. */
public object ChatMembers : tools.jazz.Table<ChatMember, NewChatMember, ChatMemberPatch>(
    "chatMembers",
    ChatMember.serializer(),
    NewChatMember.serializer(),
    ::ChatMemberPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `chatId`. */
    public val chatId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("chatId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `userId`. */
    public val userId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("userId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `joinCode`. */
    public val joinCode: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("joinCode", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
}

/** A row of `chats`. */
@kotlinx.serialization.Serializable
public data class Chat(
    val id: kotlin.String,
    val name: kotlin.String? = null,
    val isPublic: kotlin.Boolean,
    val joinCode: kotlin.String? = null,
)

/** A new row of `chats`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewChat(
    val name: kotlin.String? = null,
    val isPublic: kotlin.Boolean,
    val joinCode: kotlin.String? = null,
)

/** Changes to a row of `chats`: columns not set are left as they are. */
public class ChatPatch : tools.jazz.Patch() {
    /** Sets `name`. */
    public fun name(value: kotlin.String?): ChatPatch = apply { set("name", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
    /** Sets `isPublic`. */
    public fun isPublic(value: kotlin.Boolean): ChatPatch = apply { set("isPublic", value, kotlinx.serialization.serializer<kotlin.Boolean>()) }
    /** Sets `joinCode`. */
    public fun joinCode(value: kotlin.String?): ChatPatch = apply { set("joinCode", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
}

/** Table `chats`: its columns, and queries with `query()`. */
public object Chats : tools.jazz.Table<Chat, NewChat, ChatPatch>(
    "chats",
    Chat.serializer(),
    NewChat.serializer(),
    ::ChatPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `name`. */
    public val name: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("name", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
    /** Column `isPublic`. */
    public val isPublic: tools.jazz.Column<kotlin.Boolean> =
        tools.jazz.Column("isPublic", kotlinx.serialization.serializer<kotlin.Boolean>())
    /** Column `joinCode`. */
    public val joinCode: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("joinCode", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
}

/** A row of `messages`. */
@kotlinx.serialization.Serializable
public data class Message(
    val id: kotlin.String,
    val chatId: kotlin.String,
    val text: kotlin.String,
    val senderId: kotlin.String,
)

/** A new row of `messages`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewMessage(
    val chatId: kotlin.String,
    val text: kotlin.String,
    val senderId: kotlin.String,
)

/** Changes to a row of `messages`: columns not set are left as they are. */
public class MessagePatch : tools.jazz.Patch() {
    /** Sets `chatId`. */
    public fun chatId(value: kotlin.String): MessagePatch = apply { set("chatId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `text`. */
    public fun text(value: kotlin.String): MessagePatch = apply { set("text", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `senderId`. */
    public fun senderId(value: kotlin.String): MessagePatch = apply { set("senderId", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `messages`: its columns, and queries with `query()`. */
public object Messages : tools.jazz.Table<Message, NewMessage, MessagePatch>(
    "messages",
    Message.serializer(),
    NewMessage.serializer(),
    ::MessagePatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `chatId`. */
    public val chatId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("chatId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `text`. */
    public val text: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("text", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `senderId`. */
    public val senderId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("senderId", kotlinx.serialization.serializer<kotlin.String>())
}

/** A row of `profiles`. */
@kotlinx.serialization.Serializable
public data class Profile(
    val id: kotlin.String,
    val userId: kotlin.String,
    val name: kotlin.String,
    val avatar: kotlin.String? = null,
)

/** A new row of `profiles`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewProfile(
    val userId: kotlin.String,
    val name: kotlin.String,
    val avatar: kotlin.String? = null,
)

/** Changes to a row of `profiles`: columns not set are left as they are. */
public class ProfilePatch : tools.jazz.Patch() {
    /** Sets `userId`. */
    public fun userId(value: kotlin.String): ProfilePatch = apply { set("userId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `name`. */
    public fun name(value: kotlin.String): ProfilePatch = apply { set("name", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `avatar`. */
    public fun avatar(value: kotlin.String?): ProfilePatch = apply { set("avatar", value, tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>())) }
}

/** Table `profiles`: its columns, and queries with `query()`. */
public object Profiles : tools.jazz.Table<Profile, NewProfile, ProfilePatch>(
    "profiles",
    Profile.serializer(),
    NewProfile.serializer(),
    ::ProfilePatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `userId`. */
    public val userId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("userId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `name`. */
    public val name: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("name", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `avatar`. */
    public val avatar: tools.jazz.Column<kotlin.String?> =
        tools.jazz.Column("avatar", tools.jazz.nullable(kotlinx.serialization.serializer<kotlin.String>()))
}

/** A row of `reactions`. */
@kotlinx.serialization.Serializable
public data class Reaction(
    val id: kotlin.String,
    val messageId: kotlin.String,
    val userId: kotlin.String,
    val emoji: kotlin.String,
)

/** A new row of `reactions`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewReaction(
    val messageId: kotlin.String,
    val userId: kotlin.String,
    val emoji: kotlin.String,
)

/** Changes to a row of `reactions`: columns not set are left as they are. */
public class ReactionPatch : tools.jazz.Patch() {
    /** Sets `messageId`. */
    public fun messageId(value: kotlin.String): ReactionPatch = apply { set("messageId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `userId`. */
    public fun userId(value: kotlin.String): ReactionPatch = apply { set("userId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `emoji`. */
    public fun emoji(value: kotlin.String): ReactionPatch = apply { set("emoji", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `reactions`: its columns, and queries with `query()`. */
public object Reactions : tools.jazz.Table<Reaction, NewReaction, ReactionPatch>(
    "reactions",
    Reaction.serializer(),
    NewReaction.serializer(),
    ::ReactionPatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `messageId`. */
    public val messageId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("messageId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `userId`. */
    public val userId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("userId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `emoji`. */
    public val emoji: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("emoji", kotlinx.serialization.serializer<kotlin.String>())
}

/** A row of `strokes`. */
@kotlinx.serialization.Serializable
public data class Stroke(
    val id: kotlin.String,
    val canvasId: kotlin.String,
    val color: kotlin.String,
    val width: kotlin.Int,
    val pointsJson: kotlin.String,
)

/** A new row of `strokes`: leave out a column to store its default (or `null`). */
@kotlinx.serialization.Serializable
public data class NewStroke(
    val canvasId: kotlin.String,
    val color: kotlin.String,
    val width: kotlin.Int,
    val pointsJson: kotlin.String,
)

/** Changes to a row of `strokes`: columns not set are left as they are. */
public class StrokePatch : tools.jazz.Patch() {
    /** Sets `canvasId`. */
    public fun canvasId(value: kotlin.String): StrokePatch = apply { set("canvasId", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `color`. */
    public fun color(value: kotlin.String): StrokePatch = apply { set("color", value, kotlinx.serialization.serializer<kotlin.String>()) }
    /** Sets `width`. */
    public fun width(value: kotlin.Int): StrokePatch = apply { set("width", value, kotlinx.serialization.serializer<kotlin.Int>()) }
    /** Sets `pointsJson`. */
    public fun pointsJson(value: kotlin.String): StrokePatch = apply { set("pointsJson", value, kotlinx.serialization.serializer<kotlin.String>()) }
}

/** Table `strokes`: its columns, and queries with `query()`. */
public object Strokes : tools.jazz.Table<Stroke, NewStroke, StrokePatch>(
    "strokes",
    Stroke.serializer(),
    NewStroke.serializer(),
    ::StrokePatch,
) {
    /** The row's id. */
    public val id: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("id", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `canvasId`. */
    public val canvasId: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("canvasId", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `color`. */
    public val color: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("color", kotlinx.serialization.serializer<kotlin.String>())
    /** Column `width`. */
    public val width: tools.jazz.Column<kotlin.Int> =
        tools.jazz.Column("width", kotlinx.serialization.serializer<kotlin.Int>())
    /** Column `pointsJson`. */
    public val pointsJson: tools.jazz.Column<kotlin.String> =
        tools.jazz.Column("pointsJson", kotlinx.serialization.serializer<kotlin.String>())
}
