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
public const val SOURCE: String = "sha256:320aeff40326a9b6076a5196c5e24d44d5eaec47c105abdecae9c0b5d77a49ae";

/** The app's `jazz.app.json`: its schema, every earlier version and the migrations between them. */
public val APP: String = arrayOf(
    "{\n",
    "  \"\$schema\": \"https://jazz.tools/schemas/jazz.app.schema.json\",\n",
    "  \"format\": 1,\n",
    "  \"tables\": {\n",
    "    \"notes\": {\n",
    "      \"columns\": {\n",
    "        \"body\": \"string\",\n",
    "        \"pinned\": { \"type\": \"boolean\", \"default\": false },\n",
    "        \"folderId\": \"uuid?\"\n",
    "      },\n",
    "      \"relations\": { \"folder\": { \"rel\": \"folders\", \"column\": \"folderId\" } }\n",
    "    },\n",
    "    \"folders\": {\n",
    "      \"columns\": { \"name\": \"string\" },\n",
    "      \"relations\": { \"notesViaFolder\": { \"reverse\": \"notes\", \"relation\": \"folder\" } }\n",
    "    }\n",
    "  },\n",
    "  \"history\": {\n",
    "    \"v1\": {\n",
    "      \"tables\": {\n",
    "        \"notes\": { \"columns\": { \"text\": \"string\" } }\n",
    "      }\n",
    "    }\n",
    "  },\n",
    "  \"migrations\": [\n",
    "    {\n",
    "      \"name\": \"folders\",\n",
    "      \"from\": \"v1\",\n",
    "      \"createTables\": [\"folders\"],\n",
    "      \"migrate\": {\n",
    "        \"notes\": {\n",
    "          \"body\": { \"renameFrom\": \"text\" },\n",
    "          \"pinned\": { \"add\": \"boolean\", \"default\": false },\n",
    "          \"folderId\": { \"add\": \"uuid\", \"default\": null }\n",
    "        }\n",
    "      }\n",
    "    }\n",
    "  ]\n",
    "}\n",
).joinToString("")

/**
 * Opens the app. A backend (`JazzConfig.permissions` set to its
 * `jazz.permissions.json`) first publishes the app's permissions bundle
 * and pins the session it acts in to it.
 */
public suspend fun open(config: JazzConfig): Jazz = Jazz.open(APP, config)

/** A row of `folders`. */
@Serializable
public data class Folder(
    val id: String,
    val name: String,
)

/** A new row of `folders`: leave out a column to store its default (or `null`). */
@Serializable
public data class FolderInsert(
    val name: String,
)

/** Changes to a row of `folders`: columns not set are left as they are. */
public class FolderPatch : Patch() {
    /** Sets `name`. */
    public fun name(value: String): FolderPatch = apply { set("name", value, String.serializer()) }
}

/** Table `folders`: its columns, and queries with `query()`. */
public object Folders : Table<Folder, FolderInsert, FolderPatch>("folders", Folder.serializer(), FolderInsert.serializer(), ::FolderPatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `name`. */
    public val name: Column<String> = Column("name", String.serializer())
}

/** A row of `notes`. */
@Serializable
public data class Note(
    val id: String,
    val body: String,
    val pinned: Boolean,
    val folderId: String? = null,
)

/** A new row of `notes`: leave out a column to store its default (or `null`). */
@Serializable
public data class NoteInsert(
    val body: String,
    val pinned: Boolean? = null,
    val folderId: String? = null,
)

/** Changes to a row of `notes`: columns not set are left as they are. */
public class NotePatch : Patch() {
    /** Sets `body`. */
    public fun body(value: String): NotePatch = apply { set("body", value, String.serializer()) }
    /** Sets `pinned`. */
    public fun pinned(value: Boolean): NotePatch = apply { set("pinned", value, Boolean.serializer()) }
    /** Sets `folderId`. */
    public fun folderId(value: String?): NotePatch = apply { set("folderId", value, String.serializer().nullable) }
}

/** Table `notes`: its columns, and queries with `query()`. */
public object Notes : Table<Note, NoteInsert, NotePatch>("notes", Note.serializer(), NoteInsert.serializer(), ::NotePatch) {
    /** The row's id. */
    public val id: Column<String> = Column("id", String.serializer())
    /** Column `body`. */
    public val body: Column<String> = Column("body", String.serializer())
    /** Column `pinned`. */
    public val pinned: Column<Boolean> = Column("pinned", Boolean.serializer())
    /** Column `folderId`. */
    public val folderId: Column<String?> = Column("folderId", String.serializer().nullable)
}
