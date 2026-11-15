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
public const val SOURCE: String = "sha256:e8cdc8c71ce4a4ec76e542704f1bbcf9be8c2f0f4928d79f93b7d7e36716bd5e";

/** The app's `jazz.app.json`: its schema, every earlier version and the migrations between them. */
public val APP: String = arrayOf(
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
public suspend fun open(config: JazzConfig): Jazz = Jazz.open(APP, config)

/** A row of `canvases`. */
@Serializable
public data class Canvas(
    val id: String,
    val chatId: String,
)

/** A new row of `canvases`: leave out a column to store its default (or `null`). */
@Serializable
public data class CanvasInsert(
    val chatId: String,
)

/** Changes to a row of `canvases`: columns not set are left as they are. */
public class CanvasPatch : Patch() {
    /** Sets `chatId`. */
    public fun chatId(value: String): CanvasPatch = apply { set("chatId", value, String.serializer()) }
}

/** Table `canvases`: its columns, and queries with `query()`. */
public object Canvases : Table<Canvas, CanvasInsert, CanvasPatch>("canvases", Canvas.serializer(), CanvasInsert.serializer(), ::CanvasPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `chatId`. */
    public val chatId: Column<String> = Column("chatId", String.serializer())
}

/** A row of `chatMembers`. */
@Serializable
public data class ChatMember(
    val id: String,
    val chatId: String,
    val userId: String,
    val joinCode: String? = null,
)

/** A new row of `chatMembers`: leave out a column to store its default (or `null`). */
@Serializable
public data class ChatMemberInsert(
    val chatId: String,
    val userId: String,
    val joinCode: String? = null,
)

/** Changes to a row of `chatMembers`: columns not set are left as they are. */
public class ChatMemberPatch : Patch() {
    /** Sets `chatId`. */
    public fun chatId(value: String): ChatMemberPatch = apply { set("chatId", value, String.serializer()) }
    /** Sets `userId`. */
    public fun userId(value: String): ChatMemberPatch = apply { set("userId", value, String.serializer()) }
    /** Sets `joinCode`. */
    public fun joinCode(value: String?): ChatMemberPatch = apply { set("joinCode", value, String.serializer().nullable) }
}

/** Table `chatMembers`: its columns, and queries with `query()`. */
public object ChatMembers : Table<ChatMember, ChatMemberInsert, ChatMemberPatch>("chatMembers", ChatMember.serializer(), ChatMemberInsert.serializer(), ::ChatMemberPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `chatId`. */
    public val chatId: Column<String> = Column("chatId", String.serializer())
    /** Column `userId`. */
    public val userId: Column<String> = Column("userId", String.serializer())
    /** Column `joinCode`. */
    public val joinCode: Column<String?> = Column("joinCode", String.serializer().nullable)
}

/** A row of `chats`. */
@Serializable
public data class Chat(
    val id: String,
    val name: String? = null,
    val isPublic: Boolean,
    val joinCode: String? = null,
)

/** A new row of `chats`: leave out a column to store its default (or `null`). */
@Serializable
public data class ChatInsert(
    val name: String? = null,
    val isPublic: Boolean,
    val joinCode: String? = null,
)

/** Changes to a row of `chats`: columns not set are left as they are. */
public class ChatPatch : Patch() {
    /** Sets `name`. */
    public fun name(value: String?): ChatPatch = apply { set("name", value, String.serializer().nullable) }
    /** Sets `isPublic`. */
    public fun isPublic(value: Boolean): ChatPatch = apply { set("isPublic", value, Boolean.serializer()) }
    /** Sets `joinCode`. */
    public fun joinCode(value: String?): ChatPatch = apply { set("joinCode", value, String.serializer().nullable) }
}

/** Table `chats`: its columns, and queries with `query()`. */
public object Chats : Table<Chat, ChatInsert, ChatPatch>("chats", Chat.serializer(), ChatInsert.serializer(), ::ChatPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `name`. */
    public val name: Column<String?> = Column("name", String.serializer().nullable)
    /** Column `isPublic`. */
    public val isPublic: Column<Boolean> = Column("isPublic", Boolean.serializer())
    /** Column `joinCode`. */
    public val joinCode: Column<String?> = Column("joinCode", String.serializer().nullable)
}

/** A row of `messages`. */
@Serializable
public data class Message(
    val id: String,
    val chatId: String,
    val text: String,
    val senderId: String,
)

/** A new row of `messages`: leave out a column to store its default (or `null`). */
@Serializable
public data class MessageInsert(
    val chatId: String,
    val text: String,
    val senderId: String,
)

/** Changes to a row of `messages`: columns not set are left as they are. */
public class MessagePatch : Patch() {
    /** Sets `chatId`. */
    public fun chatId(value: String): MessagePatch = apply { set("chatId", value, String.serializer()) }
    /** Sets `text`. */
    public fun text(value: String): MessagePatch = apply { set("text", value, String.serializer()) }
    /** Sets `senderId`. */
    public fun senderId(value: String): MessagePatch = apply { set("senderId", value, String.serializer()) }
}

/** Table `messages`: its columns, and queries with `query()`. */
public object Messages : Table<Message, MessageInsert, MessagePatch>("messages", Message.serializer(), MessageInsert.serializer(), ::MessagePatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `chatId`. */
    public val chatId: Column<String> = Column("chatId", String.serializer())
    /** Column `text`. */
    public val text: Column<String> = Column("text", String.serializer())
    /** Column `senderId`. */
    public val senderId: Column<String> = Column("senderId", String.serializer())
}

/** A row of `profiles`. */
@Serializable
public data class Profile(
    val id: String,
    val userId: String,
    val name: String,
    val avatar: String? = null,
)

/** A new row of `profiles`: leave out a column to store its default (or `null`). */
@Serializable
public data class ProfileInsert(
    val userId: String,
    val name: String,
    val avatar: String? = null,
)

/** Changes to a row of `profiles`: columns not set are left as they are. */
public class ProfilePatch : Patch() {
    /** Sets `userId`. */
    public fun userId(value: String): ProfilePatch = apply { set("userId", value, String.serializer()) }
    /** Sets `name`. */
    public fun name(value: String): ProfilePatch = apply { set("name", value, String.serializer()) }
    /** Sets `avatar`. */
    public fun avatar(value: String?): ProfilePatch = apply { set("avatar", value, String.serializer().nullable) }
}

/** Table `profiles`: its columns, and queries with `query()`. */
public object Profiles : Table<Profile, ProfileInsert, ProfilePatch>("profiles", Profile.serializer(), ProfileInsert.serializer(), ::ProfilePatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `userId`. */
    public val userId: Column<String> = Column("userId", String.serializer())
    /** Column `name`. */
    public val name: Column<String> = Column("name", String.serializer())
    /** Column `avatar`. */
    public val avatar: Column<String?> = Column("avatar", String.serializer().nullable)
}

/** A row of `reactions`. */
@Serializable
public data class Reaction(
    val id: String,
    val messageId: String,
    val userId: String,
    val emoji: String,
)

/** A new row of `reactions`: leave out a column to store its default (or `null`). */
@Serializable
public data class ReactionInsert(
    val messageId: String,
    val userId: String,
    val emoji: String,
)

/** Changes to a row of `reactions`: columns not set are left as they are. */
public class ReactionPatch : Patch() {
    /** Sets `messageId`. */
    public fun messageId(value: String): ReactionPatch = apply { set("messageId", value, String.serializer()) }
    /** Sets `userId`. */
    public fun userId(value: String): ReactionPatch = apply { set("userId", value, String.serializer()) }
    /** Sets `emoji`. */
    public fun emoji(value: String): ReactionPatch = apply { set("emoji", value, String.serializer()) }
}

/** Table `reactions`: its columns, and queries with `query()`. */
public object Reactions : Table<Reaction, ReactionInsert, ReactionPatch>("reactions", Reaction.serializer(), ReactionInsert.serializer(), ::ReactionPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `messageId`. */
    public val messageId: Column<String> = Column("messageId", String.serializer())
    /** Column `userId`. */
    public val userId: Column<String> = Column("userId", String.serializer())
    /** Column `emoji`. */
    public val emoji: Column<String> = Column("emoji", String.serializer())
}

/** A row of `strokes`. */
@Serializable
public data class Stroke(
    val id: String,
    val canvasId: String,
    val color: String,
    val width: Int,
    val pointsJson: String,
)

/** A new row of `strokes`: leave out a column to store its default (or `null`). */
@Serializable
public data class StrokeInsert(
    val canvasId: String,
    val color: String,
    val width: Int,
    val pointsJson: String,
)

/** Changes to a row of `strokes`: columns not set are left as they are. */
public class StrokePatch : Patch() {
    /** Sets `canvasId`. */
    public fun canvasId(value: String): StrokePatch = apply { set("canvasId", value, String.serializer()) }
    /** Sets `color`. */
    public fun color(value: String): StrokePatch = apply { set("color", value, String.serializer()) }
    /** Sets `width`. */
    public fun width(value: Int): StrokePatch = apply { set("width", value, Int.serializer()) }
    /** Sets `pointsJson`. */
    public fun pointsJson(value: String): StrokePatch = apply { set("pointsJson", value, String.serializer()) }
}

/** Table `strokes`: its columns, and queries with `query()`. */
public object Strokes : Table<Stroke, StrokeInsert, StrokePatch>("strokes", Stroke.serializer(), StrokeInsert.serializer(), ::StrokePatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `canvasId`. */
    public val canvasId: Column<String> = Column("canvasId", String.serializer())
    /** Column `color`. */
    public val color: Column<String> = Column("color", String.serializer())
    /** Column `width`. */
    public val width: Column<Int> = Column("width", Int.serializer())
    /** Column `pointsJson`. */
    public val pointsJson: Column<String> = Column("pointsJson", String.serializer())
}
