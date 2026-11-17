package tools.jazz

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlin.test.Test
import kotlinx.serialization.json.jsonObject
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.SerializationException

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
            """{"app":"A","contract":2,"appId":"id","serverUrl":"https://app/jazz","dataDir":"d","storage":"persistent","auth":{"deviceKey":"jazz-auth-v1:k"}}""",
            JazzConfig(appId = "id", serverUrl = "https://app/jazz", storage = Storage.Persistent, dataDir = "d", auth = JazzAuth.DeviceAccount("jazz-auth-v1:k")).toJson("A"),
        )
        assertEquals(
            """{"app":"A","contract":2,"serverUrl":"https://app/jazz","storage":"memory","auth":{"bearer":true}}""",
            JazzConfig(serverUrl = "https://app/jazz", auth = JazzAuth.Bearer { "t" }).toJson("A"),
        )
    }

    @Test
    fun a_server_without_auth_is_refused_kotlin_is_a_client(): Unit = runBlocking {
        val error = assertFailsWith<JazzException.Refused> { Jazz.open("{}", JazzConfig(serverUrl = "https://app/jazz")) }
        assertTrue(error.message!!.contains("Kotlin is for apps"), error.message)
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

    @Test
    fun open_errors_are_not_wrapped() {
        val error = assertFailsWith<JazzException.Refused> { openReflectively(RefusingNative::class.java.name, "{}") }
        assertEquals("no app", error.message)
    }

    @Test
    fun cancelling_a_collection_unsubscribes_and_close_reaches_the_native_core() = runBlocking {
        val fake = FakeNative()
        val jazz = open(fake)
        assertEquals(true, jazz.subscribe(Items.query()).first().reset)
        withTimeout(5_000) { while (fake.subs.isNotEmpty()) delay(10) }
        jazz.close()
        assertTrue(fake.closed)
    }

    @Test
    fun subscription_errors_end_the_flow() = runBlocking {
        val fake = FakeNative().apply { failSubscriptionsWith = "lost the server" }
        val jazz = open(fake)
        val error = assertFailsWith<JazzException.Refused> { jazz.subscribe(Items.query()).collect {} }
        assertEquals("lost the server", error.message)
    }

    @Test
    fun watch_drops_no_delta_however_slow_its_collector() = runBlocking {
        val fake = FakeNative()
        val jazz = open(fake)
        val sizes = Channel<Int>(Channel.UNLIMITED)
        val job = launch(Dispatchers.Default) {
            jazz.watch(Items.query()).collect { rows ->
                delay(1)
                sizes.send(rows.size)
            }
        }
        assertEquals(0, sizes.receive())
        // 500 deltas at once, far more than a default Flow buffer holds.
        repeat(500) { fake.insert("items", """{"title":"t$it","rank":$it,"kind":"c"}""") }
        withTimeout(10_000) { while (sizes.receive() != 500) Unit }
        job.cancel()
    }

    @Test
    fun bigint_is_a_decimal_string_both_ways() = runBlocking {
        val fake = FakeNative()
        val jazz = open(fake)
        val item = jazz.insert(Items, ItemInsert(title = "x", rank = 0.0, kind = ItemKind.C, big = Long.MAX_VALUE))
        assertEquals(Long.MAX_VALUE, item.big)
        assertTrue(fake.tables["items"]!![item.id].toString().contains("\"big\":\"9223372036854775807\""))
        assertEquals(-5L, jazzJson.decodeFromString(Item.serializer(), """{"id":"i","title":"x","rank":0,"kind":"c","big":"-5"}""").big)
        assertEquals(
            """{"table":"items","where":[{"column":"tags","op":"contains","value":"3"}]}""",
            Items.where(Items.tags contains "3").toJson(),
        )
    }

    @Test
    fun non_finite_doubles_are_refused_before_they_reach_the_core(): Unit = runBlocking {
        val jazz = open()
        assertFailsWith<SerializationException> { jazz.insert(Items, ItemInsert(title = "x", rank = Double.NaN, kind = ItemKind.C)) }
        assertFailsWith<SerializationException> { Items.where(Items.rank gt Double.POSITIVE_INFINITY).toJson() }
        // jazz-ffi reads a stored non-finite float as null: a non-null Double column then fails to decode.
        assertFailsWith<SerializationException> {
            jazzJson.decodeFromString(Item.serializer(), """{"id":"i","title":"x","rank":null,"kind":"c"}""")
        }
    }

    @Test
    fun close_is_idempotent_and_ends_watches_with_closed(): Unit = runBlocking {
        val fake = FakeNative()
        val jazz = open(fake)
        jazz.insert(Items, ItemInsert(title = "a", rank = 1.0, kind = ItemKind.C))
        val results = Channel<Int>(Channel.UNLIMITED)
        val watching = async { runCatching { jazz.watch(Items.query()).collect { results.send(it.size) } } }
        assertEquals(1, results.receive())
        jazz.close()
        jazz.close()
        assertTrue(withTimeout(5_000) { watching.await() }.exceptionOrNull() is JazzException.Closed)
        assertFailsWith<JazzException.Closed> { jazz.all(Items.query()) }
    }

    @Test
    fun cancelling_while_subscribing_still_unsubscribes(): Unit = runBlocking {
        val fake = FakeNative()
        val gate = java.util.concurrent.CountDownLatch(1)
        fake.subscribeGate = gate
        val jazz = open(fake)
        val job = launch(Dispatchers.Default) { jazz.subscribe(Items.query()).collect {} }
        // The collector is cancelled while the native subscribe is still running.
        withTimeout(5_000) { while (fake.subs.isEmpty() && fake.errorCallbacks.isEmpty()) { delay(5); if (gate.count == 1L) break } }
        job.cancel()
        gate.countDown()
        job.join()
        withTimeout(5_000) { while (fake.subs.isNotEmpty()) delay(10) }
    }

    @Test
    fun a_refused_delete_client_storage_leaves_the_app_open(): Unit = runBlocking {
        val fake = object : Native by FakeNative() {
            override fun deleteClientStorage(): Unit = throw JazzException.Refused("another open app holds the store")
        }
        Jazz.opener = Native.Opener { fake }
        val jazz = Jazz.open("{}", JazzConfig())
        assertFailsWith<JazzException.Refused> { jazz.deleteClientStorage() }
        jazz.insert(Items, ItemInsert(title = "still open", rank = 1.0, kind = ItemKind.C))
        assertEquals(listOf("still open"), jazz.all(Items.query()).map { it.title })
    }

    @Test
    fun sign_out_reads_signed_out_even_when_the_watch_closes_first(): Unit = runBlocking {
        // As jazz-ffi can: signing out ends the session watch with `closed`
        // before (or without) a final `signedOut` state.
        val listeners = java.util.Collections.synchronizedList(ArrayList<(String) -> Unit>())
        val native = object : Native by FakeNative() {
            override fun sessionState(): String =
                """{"status":"signedIn","user":{"account":"a","kind":"backend","issuer":"i","subject":"alice"}}"""
            override fun watch(sync: Boolean, onChange: (String) -> Unit): () -> Unit {
                if (!sync) listeners += onChange
                return {}
            }
            override fun signOut() {
                listeners.toList().forEach { it("""{"kind":"closed"}""") }
            }
        }
        Jazz.opener = Native.Opener { native }
        val jazz = Jazz.open("{}", JazzConfig())
        assertTrue(jazz.session.value is SessionState.SignedIn)
        jazz.signOut()
        assertEquals(SessionState.SignedOut, jazz.session.value)
    }

    @Test
    fun text_positions_are_utf16_by_default(): Unit = runBlocking {
        // Kotlin's String indices (as an EditText selection) are UTF-16 code units.
        val edits = Edits().apply { splice(Items.title, 0, 2, Splice.text(0, 2, "é")) }.edits
        assertEquals("\"utf16\"", edits.single()["within"]!!.jsonObject["units"].toString())
        val explicit = Edits().apply { splice(Items.title, SpliceUnits.Utf8, 0, 2, Splice.text(0, 2, "é")) }.edits
        assertEquals("\"utf8\"", explicit.single()["within"]!!.jsonObject["units"].toString())
        assertEquals("\"utf16\"", Items.title.range(0, 4).selectJson().jsonObject["units"].toString())
    }

    @Test
    fun watch_keeps_results_with_the_same_id_apart_by_key(): Unit = runBlocking {
        // As a history or union subscription delivers them: one row id, two results.
        val native = object : Native by FakeNative() {
            override fun subscribe(queryJson: String, onDelta: (String) -> Unit, onError: (String, String) -> Unit): () -> Unit {
                val v = { k: String, t: String -> """{"id":"i","${'$'}key":"$k","title":"$t","rank":1,"kind":"c"}""" }
                onDelta("""{"added":[${v("v1", "one")},${v("v2", "two")}],"updated":[],"removed":[],"reset":true}""")
                onDelta("""{"added":[],"updated":[],"removed":["i"],"removedKeys":["v1"],"reset":false}""")
                return {}
            }
        }
        Jazz.opener = Native.Opener { native }
        val jazz = Jazz.open("{}", JazzConfig())
        val results = jazz.watch(Items.query()).take(2).toList()
        assertEquals(listOf(listOf("one", "two"), listOf("two")), results.map { list -> list.map { it.title } })
    }
}

object RefusingNative {
    @JvmStatic
    fun open(config: String): Native = throw JazzException.Refused("no app")

    @Test
    fun hops_gather_union_history_and_deleted_rows_emit_the_contract_json() {
        val parentOf = Many<Item, Item>("children") { Items }
        val tagOwner = One<Item, Item>("parent") { Items }
        assertEquals(
            """{"table":"items","where":[{"column":"kind","op":"eq","value":"c"}],"hopTo":"parent","after":{"where":[{"column":"title","op":"ne","value":"x"}],"limit":5}}""",
            Items.where(Items.kind eq ItemKind.C).hopTo(tagOwner).where(Items.title ne "x").limit(5).toJson(),
        )
        assertEquals(
            """{"table":"items","hopTo":["parent","parent"]}""",
            Items.query().hopTo(tagOwner).hopTo(tagOwner).toJson(),
        )
        assertEquals(
            """{"table":"items","where":[{"column":"title","op":"eq","value":"root"}],"gather":{"relation":"children","maxDepth":3}}""",
            Items.where(Items.title eq "root").gather(parentOf, maxDepth = 3).toJson(),
        )
        assertEquals(
            """{"union":[{"table":"items","where":[{"column":"kind","op":"eq","value":"c"}]},{"table":"items","where":[{"column":"note","op":"isNull"}]}],"orderBy":[{"column":"rank","direction":"asc"}],"limit":10}""",
            Items.where(Items.kind eq ItemKind.C).union(Items.where(Items.note.isNull())).orderBy(Items.rank.asc()).limit(10).toJson(),
        )
        assertEquals(
            """{"table":"items","orderBy":[{"column":"${'$'}acceptedAt","direction":"desc"}],"history":true}""",
            Items.query().orderBy(JazzColumns.acceptedAt.desc()).history().toJson(),
        )
        assertEquals("""{"table":"items","includeDeleted":true}""", Items.query().includeDeleted().toJson())
        val h = jazzJson.decodeFromString(
            HistoryRowSerializer(Item.serializer()),
            """{"id":"i","title":"a","rank":1,"kind":"c","${'$'}history":{"transactionId":"t1","acceptedAt":42}}""",
        )
        assertEquals(HistoryRow(Item("i", "a", 1.0, ItemKind.C), "t1", 42), h)
    }
}
