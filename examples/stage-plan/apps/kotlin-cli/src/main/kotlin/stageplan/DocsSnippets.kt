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
import tools.jazz.JazzAuth
import tools.jazz.JazzConfig
import tools.jazz.SessionState
import tools.jazz.Storage
import tools.jazz.SyncStatus
import tools.jazz.JazzException
import tools.jazz.JazzRow
import tools.jazz.ReadTier
import tools.jazz.WriteTier
import tools.jazz.include
import tools.jazz.select
import jazzapp.Comment
import jazzapp.Comments
import jazzapp.NewComment
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

    suspend fun transaction(jazz: Jazz, showId: String): Boolean {
        // #region writing-transaction-kotlin
        // Move the show's first open task to "doing", reading and writing in one
        // transaction: it commits when the block returns, and rolls back if it throws.
        val moved: Boolean = jazz.transaction { tx ->
            val next = tx.one(
                Tasks.where(Tasks.showId eq showId, Tasks.status eq TaskStatus.Todo).orderBy(Tasks.rank.asc()),
            ) ?: return@transaction false
            tx.update(Tasks, next.id) { status(TaskStatus.Doing) }
            true
        }
        // #endregion writing-transaction-kotlin
        return moved
    }

    suspend fun writeTier(jazz: Jazz, showId: String): Task {
        // #region writing-durability-tier-kotlin
        // Returns once the sync server accepted the write; a write its permissions
        // refuse throws JazzException.Permission instead of quietly rolling back.
        val task = jazz.insert(
            Tasks,
            NewTask(showId = showId, title = "Strike", status = TaskStatus.Todo, rank = 99.0),
            tier = WriteTier.Global,
        )
        jazz.update(Tasks, task.id, tier = WriteTier.Global) { status(TaskStatus.Doing) }
        // Or write locally, then wait for everything so far to reach the server.
        jazz.waitForSync(WriteTier.Global)
        // #endregion writing-durability-tier-kotlin
        return task
    }

    suspend fun readTier(jazz: Jazz, showId: String): List<Task> {
        // #region reading-durability-tier-kotlin
        // As the sync server has it, rather than what this process has locally.
        val tasks = jazz.all(Tasks.where(Tasks.showId eq showId), tier = ReadTier.Remote)
        // #endregion reading-durability-tier-kotlin
        return tasks
    }

    suspend fun requireIncludes(jazz: Jazz, taskId: String): List<JazzRow<Comment>> {
        // #region reading-require-includes-kotlin
        // Only comments whose author this user can read: `required()` drops the others,
        // so `comment[Comments.author]` is there for every row it returns.
        val comments = jazz.all(Comments.where(Comments.taskId eq taskId).include(Comments.author.required()))
        // #endregion reading-require-includes-kotlin
        return comments
    }

    suspend fun transactionErrors(jazz: Jazz, taskId: String): String {
        // #region writing-transaction-errors-kotlin
        // A block that throws rolls the transaction back and rethrows; a commit the
        // server refuses throws too (at WriteTier.Global), with nothing applied.
        val outcome = try {
            jazz.transaction(tier = WriteTier.Global) { tx ->
                tx.update(Tasks, taskId) { status(TaskStatus.Blocked) }
                tx.insert(Comments, NewComment(taskId = taskId, authorId = java.util.UUID.randomUUID().toString(), body = "blocked"))
            }
            "committed"
        } catch (e: JazzException.Permission) {
            "refused: ${e.message}"
        }
        // #endregion writing-transaction-errors-kotlin
        return outcome
    }

    suspend fun mutationErrors(jazz: Jazz, taskId: String, crewId: String): String {
        // #region writing-mutation-errors-kotlin
        // Local-first writes return at once; one the server refuses rolls back later.
        // To see the refusal, write at WriteTier.Global (or call waitForSync).
        val result = try {
            jazz.update(Tasks, taskId, tier = WriteTier.Global) { assigneeId(crewId) }
            "saved"
        } catch (e: JazzException.Permission) {
            "not allowed: ${e.message}"
        } catch (e: JazzException.Refused) {
            "invalid: ${e.message}"
        }
        // #endregion writing-mutation-errors-kotlin
        return result
    }

    suspend fun editText(jazz: Jazz, task: Task): String? {
        // #region writing-text-edit-kotlin
        // tasks.notes is "merge": "text": write it as an edit of what you read, and
        // concurrent edits by others merge instead of one overwriting the other.
        val before: String = task.notes ?: ""
        jazz.editText(Tasks, task.id, Tasks.notes, editedFrom = before, text = "$before\nBring gaffer tape.")
        // #endregion writing-text-edit-kotlin
        return jazz.one(Tasks.where(Tasks.id eq task.id))?.notes
    }

    suspend fun permissionAdvice(jazz: Jazz, task: Task, crewId: String): List<Boolean?> {
        // #region reading-permission-advice-kotlin
        // Ask before offering an action: true, false, or null when no server can say.
        val canRename = jazz.canUpdate(Tasks, task.id) { title("Renamed") }
        val canAssign = jazz.canUpdate(Tasks, task.id) { assigneeId(crewId) }
        val canDelete = jazz.canDelete(Tasks, task.id)
        val canAdd = jazz.canInsert(Tasks, NewTask(showId = task.showId, title = "New", status = TaskStatus.Todo, rank = 1.0))
        // #endregion reading-permission-advice-kotlin
        return listOf(canRename, canAssign, canDelete, canAdd)
    }

    suspend fun openClient(endpoint: String, appId: String, deviceKey: String): Jazz {
        // #region context-setup-kotlin-client
        // An end user's app: it syncs through your backend's Jazz endpoint, signed in
        // with a local-first device account (keep the key in the platform keychain).
        val jazz = jazzapp.open(
            JazzConfig(
                appId = appId,
                serverUrl = endpoint, // e.g. "https://app.example/jazz"
                storage = Storage.Persistent,
                dataDir = System.getProperty("java.io.tmpdir") + "/stage-plan-" + appId,
                auth = JazzAuth.DeviceAccount(deviceKey), // JazzAuth.generateDeviceKey() once
            ),
        )
        // #endregion context-setup-kotlin-client
        return jazz
    }

    suspend fun signIn(endpoint: String, appId: String, fetchToken: suspend (fresh: Boolean) -> String?): Jazz {
        // #region auth-bearer-kotlin
        // A user your backend signs in: Jazz asks for their token when connecting,
        // before it expires, and after the server refuses one.
        val jazz = jazzapp.open(
            JazzConfig(
                appId = appId,
                serverUrl = endpoint,
                auth = JazzAuth.Bearer { reason -> fetchToken(reason.fresh) },
            ),
        )
        val me: String? = (jazz.session.value as? SessionState.SignedIn)?.user?.account
        // #endregion auth-bearer-kotlin
        checkNotNull(me)
        return jazz
    }

    suspend fun sessionState(jazz: Jazz): List<String> {
        val seen = ArrayList<String>()
        // #region auth-session-kotlin
        // The session and sync status as StateFlows: collect them in your UI.
        when (val session = jazz.session.value) {
            is SessionState.SignedIn -> seen += "signed in as ${session.user.subject}"
            is SessionState.Expired -> seen += "session expired (${session.reason}): sign in again"
            SessionState.SignedOut, null -> seen += "signed out"
        }
        if (jazz.syncStatus.value is SyncStatus.Offline) seen += "offline"
        jazz.refreshSession() // ask for a fresh token now
        jazz.signOut() // also closes the app
        // #endregion auth-session-kotlin
        return seen
    }
}
