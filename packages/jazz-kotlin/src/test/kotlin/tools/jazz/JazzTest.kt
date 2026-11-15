package tools.jazz

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

// What generated bindings declare for a small table.
@Serializable
enum class ItemKind { @SerialName("a-b") AB, @SerialName("c") C }

@Serializable
data class Item(
    val id: String,
    val title: String,
    val rank: Double,
    val kind: ItemKind,
    val note: String? = null,
    val big: @Serializable(with = JazzBigInt::class) Long? = null,
    val tags: List<@Serializable(with = JazzBigInt::class) Long>? = null,
)

@Serializable
data class ItemInsert(
    val title: String,
    val rank: Double,
    val kind: ItemKind,
    val note: String? = null,
    val big: @Serializable(with = JazzBigInt::class) Long? = null,
)

class ItemPatch : Patch() {
    fun title(value: String): ItemPatch = apply { set("title", value, String.serializer()) }
    fun note(value: String?): ItemPatch = apply { set("note", value, String.serializer().nullable) }
}

object Items : Table<Item, ItemInsert, ItemPatch>("items", Item.serializer(), ItemInsert.serializer(), ::ItemPatch) {
    val id: Column<String> = Column("id", String.serializer())
    val title: Column<String> = Column("title", String.serializer())
    val rank: Column<Double> = Column("rank", Double.serializer())
    val kind: Column<ItemKind> = Column("kind", ItemKind.serializer())
    val note: Column<String?> = Column("note", String.serializer().nullable)
    val tags: Column<List<Long>?> = Column("tags", ListSerializer(JazzBigInt).nullable)
}

class JazzTest {
    private suspend fun open(fake: FakeNative = FakeNative()): Jazz {
        Jazz.opener = Native.Opener { fake }
        return Jazz.open("{}", JazzConfig())
    }

    @Test
    fun queries_emit_the_contract_json() {
        val q = Items.where(Items.kind eq ItemKind.AB, Items.note.isNull(), Items.rank gte 1.5)
            .orderBy(Items.rank.desc())
            .limit(10)
            .offset(20)
        assertEquals(
            """{"table":"items","where":[{"column":"kind","op":"eq","value":"a-b"},{"column":"note","op":"isNull"},""" +
                """{"column":"rank","op":"gte","value":1.5}],"orderBy":[{"column":"rank","direction":"desc"}],"limit":10,"offset":20}""",
            q.toJson(),
        )
        assertEquals(
            """{"table":"items","where":[{"column":"title","op":"in","value":["x","y"]},{"column":"tags","op":"contains","value":"3"}]}""",
            Items.where(Items.title inList listOf("x", "y"), Items.tags contains "3").toJson(),
        )
    }

    @Test
    fun config_json_names_the_contract_fields() {
        assertEquals(
            """{"app":"A","permissions":"P","appId":"id","serverUrl":"http://s","storage":"memory","backendSecret":"b",""" +
                """"adminSecret":"a","session":{"issuer":"jazz:backend","subject":"u1"}}""",
            JazzConfig(appId = "id", serverUrl = "http://s", permissions = "P", backendSecret = "b", adminSecret = "a", sessionSubject = "u1").toJson("A"),
        )
    }

    @Test
    fun crud_round_trips_through_json() = runTest {
        val fake = FakeNative()
        val jazz = open(fake)
        val item = jazz.insert(Items, ItemInsert(title = "one", rank = 2.0, kind = ItemKind.C, big = 12345678901234))
        assertEquals("""{"id":"${item.id}","title":"one","rank":2.0,"kind":"c","big":"12345678901234"}""", fake.tables["items"]!![item.id].toString())
        assertEquals(12345678901234, item.big)
        jazz.update(Items, item.id) { title("uno"); note("n") }
        assertEquals("n", jazz.one(Items.where(Items.id eq item.id))!!.note)
        jazz.update(Items, item.id) { note(null) }
        assertNull(jazz.one(Items.where(Items.id eq item.id))!!.note)
        assertEquals(listOf("uno"), jazz.all(Items.query()).map { it.title })
        jazz.delete(Items, item.id)
        assertEquals(emptyList(), jazz.all(Items.query()))
    }

    @Test
    fun watch_keeps_the_result_in_order() = runBlocking {
        val jazz = open()
        val a = jazz.insert(Items, ItemInsert(title = "a", rank = 2.0, kind = ItemKind.C))
        val results = Channel<List<String>>(Channel.UNLIMITED)
        val job = launch { jazz.watch(Items.orderBy(Items.rank.asc())).collect { list -> results.send(list.map { it.title }) } }
        assertEquals(listOf("a"), results.receive())
        jazz.insert(Items, ItemInsert(title = "b", rank = 1.0, kind = ItemKind.C))
        assertEquals(listOf("b", "a"), results.receive())
        jazz.update(Items, a.id) { title("a2") }
        assertEquals(listOf("b", "a2"), results.receive())
        jazz.delete(Items, a.id)
        assertEquals(listOf("b"), results.receive())
        job.cancel()
        assertEquals(true, jazz.subscribe(Items.query()).first().reset)
    }
}
