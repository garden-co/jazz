package tools.jazz

import jazzapp.JazzComputed
import jazzapp.People
import jazzapp.Task
import jazzapp.TaskPriority
import jazzapp.TaskReview
import jazzapp.TaskStatus
import jazzapp.Tasks
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals

object Computed : JazzComputed {
    override fun tasksPriority(score: Int, status: TaskStatus): TaskPriority = if (score > 5) TaskPriority.High else TaskPriority.Low
    override fun tasksLabel(title: String, dueAt: String?): String? = null
    override fun isoDateView(stored: Long): String = stored.toString()
    override fun isoDateStored(view: String): Long = view.toLong()
    override fun lowercaseView(stored: String): String = stored
    override fun lowercaseStored(view: String): String = view.lowercase()
}

class TasksTest {
    private fun row(review: String) = jazzJson.parseToJsonElement(
        """{"id":"t","title":"x","projectId":"p","status":"todo","score":7,"tags":[],"assigneeIds":["a","b"],"review":$review}""",
    ) as JsonObject

    @Test
    fun payload_enums_read_as_sealed_classes(): Unit = runBlocking {
        val fake = FakeNative()
        Jazz.opener = Native.Opener { fake }
        val jazz = jazzapp.open(JazzConfig(), Computed)
        fake.tables["tasks"] = linkedMapOf(
            "t" to row("""{"case":"approved","fields":{"by":"u1"}}"""),
            "u" to JsonObject(row("""{"case":"pending","fields":{}}""") + ("id" to JsonPrimitive("u"))),
        )
        val tasks: List<Task> = jazz.all(Tasks.query())
        assertEquals(TaskReview.Approved(by = "u1", note = null), tasks[0].review)
        assertEquals(TaskReview.Pending, tasks[1].review)
        assertEquals(TaskPriority.High, tasks[0].priority)
        // And they write as { case, fields }.
        assertEquals(
            """{"case":"rejected","fields":{"reason":"late"}}""",
            jazzJson.encodeToString(TaskReview.Serializer, TaskReview.Rejected("late")),
        )
        // A uuid[] forward relation is a Many handle.
        val q = Tasks.query().include(Tasks.assignees)
        assertEquals("""{"table":"tasks","include":[{"relation":"assignees"}]}""", q.toJson())
        assertEquals(People.tableName, Tasks.assignees.target.tableName)
    }
}
