package tools.jazz

import jazzapp.JazzComputed
import jazzapp.NewPerson
import jazzapp.NewTask
import jazzapp.People
import jazzapp.TaskPriority
import jazzapp.Tasks
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The app's implementation of what jazz.app.json declares: dates as ISO strings, emails lowercased. */
object Computed : JazzComputed {
    override fun tasksPriority(score: Int): TaskPriority = if (score >= 10) TaskPriority.High else TaskPriority.Low
    override fun tasksLabel(title: String, dueAt: String?): String? = dueAt?.let { "$title (due $it)" }
    override fun isoDateView(stored: Long): String = Instant.ofEpochMilli(stored).toString()
    override fun isoDateStored(view: String): Long = Instant.parse(view).toEpochMilli()
    override fun lowercaseView(stored: String): String = stored
    override fun lowercaseStored(view: String): String = view.lowercase()
}

class ComputedTest {
    @Test
    fun transforms_and_computed_fields_go_both_ways() = runBlocking {
        val fake = FakeNative()
        Jazz.opener = Native.Opener { fake }
        val jazz = jazzapp.open(JazzConfig(), Computed)

        val task = jazz.insert(Tasks, NewTask(title = "Load in", score = 12, dueAt = "2026-11-16T09:00:00Z"))
        // Stored as the transform's stored type; read back as its view, with computed fields.
        assertEquals(Instant.parse("2026-11-16T09:00:00Z").toEpochMilli().toString(), fake.tables["tasks"]!![task.id]!!["dueAt"].toString())
        assertEquals("2026-11-16T09:00:00Z", task.dueAt)
        assertEquals(TaskPriority.High, task.priority)
        assertEquals("Load in (due 2026-11-16T09:00:00Z)", task.label)

        jazz.update(Tasks, task.id) { score(1); dueAt(null) }
        val updated = jazz.one(Tasks.where(Tasks.id eq task.id))!!
        assertEquals(TaskPriority.Low, updated.priority)
        assertEquals(null, updated.label)

        val ada = jazz.insert(People, NewPerson(name = "Ada", email = "Ada@Example.COM"))
        assertEquals("ada@example.com", ada.email)
        assertEquals("ada@example.com", jazz.watch(People.query()).first().single().email)
        assertTrue(jazz.all(Tasks.query().select(Tasks.title)).single()[Tasks.title] == "Load in")
    }
}
