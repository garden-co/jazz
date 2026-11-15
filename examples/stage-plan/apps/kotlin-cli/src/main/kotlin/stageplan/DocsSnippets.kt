// Documentation snippet sources, compiled with the example and run by its
// tests. The docs include each `#region` by name.
package stageplan

import jazzapp.NewTask
import jazzapp.TaskStatus
import jazzapp.Tasks
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import tools.jazz.Jazz
import tools.jazz.JazzConfig
import tools.jazz.JazzRow
import tools.jazz.include
import tools.jazz.select
import jazzapp.Comments
import jazzapp.Crew
import jazzapp.Shows
import jazzapp.Task

object DocsSnippets {
    suspend fun openBackend(serverUrl: String, appId: String, backendSecret: String, adminSecret: String, user: String): Jazz {
        // #region context-setup-kotlin-backend
        val jazz = jazzapp.open(
            JazzConfig(
                appId = appId,
                serverUrl = serverUrl,
                // The text of jazz.permissions.json: this makes it a backend.
                permissions = permissions(),
                backendSecret = backendSecret,
                adminSecret = adminSecret,
                // The user the backend acts as.
                sessionSubject = user,
            ),
        )
        val me: String? = jazz.account()
        // #endregion context-setup-kotlin-backend
        checkNotNull(me)
        return jazz
    }

    suspend fun oneShot(jazz: Jazz, showId: String): List<Task> {
        // #region reading-oneshot-kotlin
        val tasks: List<Task> = jazz.all(Tasks.where(Tasks.showId eq showId))
        val first: Task? = jazz.one(Tasks.where(Tasks.showId eq showId))
        // #endregion reading-oneshot-kotlin
        check(first == null || first in tasks)
        return tasks
    }

    suspend fun subscriptions(jazz: Jazz, showId: String): List<List<String>> {
        // #region reading-subscriptions-kotlin
        // The whole result, re-sorted on each change; collecting subscribes,
        // and cancelling the collection (here: after two results) unsubscribes.
        val titles: Flow<List<Task>> = jazz.watch(Tasks.where(Tasks.showId eq showId).orderBy(Tasks.rank.asc()))
        val firstTwo = titles.take(2).toList()

        // Or the deltas themselves: the first is a reset with the full result.
        val delta = jazz.subscribe(Tasks.where(Tasks.showId eq showId)).first()
        println("added ${delta.added.size}, updated ${delta.updated.size}, removed ${delta.removed.size}")
        // #endregion reading-subscriptions-kotlin
        return firstTwo.map { list -> list.map { it.title } }
    }

    suspend fun filters(jazz: Jazz, showId: String): List<Task> {
        // #region reading-filters-kotlin
        // Conditions are ANDed.
        val open = jazz.all(
            Tasks.where(
                Tasks.showId eq showId,
                Tasks.status inList listOf(TaskStatus.Todo, TaskStatus.Doing),
                Tasks.assigneeId.isNull(),
            ),
        )
        // #endregion reading-filters-kotlin
        return open
    }

    suspend fun sorting(jazz: Jazz, showId: String): List<Task> {
        // #region reading-sorting-kotlin
        val byRank = jazz.all(Tasks.where(Tasks.showId eq showId).orderBy(Tasks.rank.asc()))
        val newestRankFirst = jazz.all(Tasks.where(Tasks.showId eq showId).orderBy(Tasks.rank.desc(), Tasks.title.asc()))
        // #endregion reading-sorting-kotlin
        check(byRank.reversed().map { it.rank } == newestRankFirst.map { it.rank })
        return byRank
    }

    suspend fun pagination(jazz: Jazz, showId: String): List<Task> {
        // #region reading-pagination-kotlin
        val pageSize = 20
        val page = 2
        val tasks = jazz.all(
            Tasks.where(Tasks.showId eq showId)
                .orderBy(Tasks.rank.asc())
                .limit(pageSize)
                .offset((page - 1) * pageSize),
        )
        // #endregion reading-pagination-kotlin
        return tasks
    }

    suspend fun crud(jazz: Jazz, showId: String) {
        // #region writing-crud-kotlin
        val task = jazz.insert(
            Tasks,
            NewTask(showId = showId, title = "Load in", status = TaskStatus.Todo, rank = 1.0),
        )
        jazz.update(Tasks, task.id) {
            title("Load in the set")
            status(TaskStatus.Doing)
        }
        jazz.delete(Tasks, task.id)
        // #endregion writing-crud-kotlin
    }

    suspend fun nullableUpdate(jazz: Jazz, taskId: String) {
        // #region writing-nullable-update-kotlin
        // Setting a nullable column to null clears it; columns not set are left alone.
        jazz.update(Tasks, taskId) { assigneeId(null) }
        // #endregion writing-nullable-update-kotlin
    }

    suspend fun whereOperators(jazz: Jazz, showId: String): List<List<Task>> {
        // #region where-eq-ne-kotlin
        val blocked = jazz.all(Tasks.where(Tasks.showId eq showId, Tasks.status eq TaskStatus.Blocked))
        val notDone = jazz.all(Tasks.where(Tasks.showId eq showId, Tasks.status ne TaskStatus.Done))
        // #endregion where-eq-ne-kotlin
        // #region where-numeric-kotlin
        val topTen = jazz.all(Tasks.where(Tasks.showId eq showId, Tasks.rank lte 10.0))
        val later = jazz.all(Tasks.where(Tasks.showId eq showId, Tasks.rank gt 10.0))
        // #endregion where-numeric-kotlin
        // #region where-contains-kotlin
        val soundTasks = jazz.all(Tasks.where(Tasks.showId eq showId, Tasks.title contains "sound"))
        // #endregion where-contains-kotlin
        // #region where-null-kotlin
        val unassigned = jazz.all(Tasks.where(Tasks.showId eq showId, Tasks.assigneeId.isNull()))
        val assigned = jazz.all(Tasks.where(Tasks.showId eq showId, Tasks.assigneeId.isNotNull()))
        // #endregion where-null-kotlin
        return listOf(blocked, notDone, topTen, later, soundTasks, unassigned, assigned)
    }

    suspend fun includes(jazz: Jazz, showId: String): List<String> {
        // #region reading-includes-kotlin
        // Each task with the crew member it's assigned to and its comments, each
        // comment with its author: typed by the relations jazz.app.json declares.
        val tasks: List<JazzRow<Task>> = jazz.all(
            Tasks.where(Tasks.showId eq showId)
                .orderBy(Tasks.rank.asc())
                .include(
                    Tasks.assignee,
                    Tasks.comments.orderBy(Comments.body.asc()).include(Comments.author),
                ),
        )
        val lines = tasks.map { task ->
            val assignee: Crew? = task[Tasks.assignee]
            val comments = task.rows(Tasks.comments).map { comment ->
                "${comment[Comments.author]?.name}: ${comment[Comments.body]}"
            }
            "${task[Tasks.title]} (${assignee?.name ?: "unassigned"}) $comments"
        }
        // #endregion reading-includes-kotlin
        return lines
    }

    suspend fun reverseRelation(jazz: Jazz, showId: String): Map<String, Int> {
        // #region reading-reverse-relation-kotlin
        // A show's rows with the tasks that point at it (the reverse of `tasks.show`).
        val shows = jazz.all(Shows.where(Shows.id eq showId).include(Shows.tasks.where(Tasks.status ne TaskStatus.Done)))
        val openTasks: Map<String, Int> = shows.associate { it[Shows.name] to it[Shows.tasks].size }
        // #endregion reading-reverse-relation-kotlin
        return openTasks
    }

    suspend fun select(jazz: Jazz, showId: String): List<Pair<String, TaskStatus>> {
        // #region reading-select-kotlin
        // Only the columns a list needs; `id` comes along always.
        val rows = jazz.all(Tasks.where(Tasks.showId eq showId).select(Tasks.title, Tasks.status))
        val titles = rows.map { it[Tasks.title] to it[Tasks.status] }
        // #endregion reading-select-kotlin
        return titles
    }
}
