// Documentation snippet sources, compiled with the example and run by its
// tests. The docs include each `#region` by name.
package stageplan

import jazzapp.TaskStatus
import jazzapp.Tasks
import tools.jazz.Jazz
import tools.jazz.JazzAuth
import tools.jazz.HistoryRow
import tools.jazz.JazzColumns
import tools.jazz.JazzConfig
import tools.jazz.hopTo
import tools.jazz.history
import tools.jazz.union
import tools.jazz.chunks
import tools.jazz.Link
import tools.jazz.SessionState
import tools.jazz.Storage
import tools.jazz.SyncStatus
import tools.jazz.JazzRow
import tools.jazz.ReadTier
import tools.jazz.include
import tools.jazz.select
import jazzapp.Comment
import jazzapp.Comments
import jazzapp.Crew
import jazzapp.CrewTable
import jazzapp.Task

object DocsSnippets {
    suspend fun requireIncludes(jazz: Jazz, taskId: String): List<JazzRow<Comment>> {
        // #region reading-require-includes-kotlin
        // Only comments whose author this user can read: `required()` drops the others,
        // so `comment[Comments.author]` is there for every row it returns.
        val comments = jazz.all(Comments.where(Comments.taskId eq taskId).include(Comments.author.required()))
        // #endregion reading-require-includes-kotlin
        return comments
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

    suspend fun openClient(endpoint: String, appId: String, deviceKey: String): Jazz {
        // #region context-setup-kotlin-client
        // An end user's app: it syncs through your backend's Jazz endpoint, signed in
        // with a device account (keep the key in the platform keychain).
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
        val sync: SyncStatus = jazz.syncStatus.value
        if (sync.link is Link.Offline) seen += "offline"
        if (!sync.synced) seen += "${sync.pendingUploads} changes to upload"
        jazz.refreshSession() // ask for a fresh token now
        jazz.signOut() // also closes the app
        // #endregion auth-session-kotlin
        return seen
    }

    suspend fun largeValues(jazz: Jazz, taskId: String, notes: java.io.InputStream): String? {
        // #region writing-streaming-upload-kotlin
        // Stream a large value in, chunk by chunk, without holding it in memory:
        // nothing is written if the stream fails or the call is cancelled.
        jazz.updateStreaming(Tasks, taskId, Tasks.notes, notes.chunks())
        // #endregion writing-streaming-upload-kotlin
        // #region reading-partial-value-kotlin
        // Read only the part of a large value you show: here, the first 80 characters.
        val preview: String? = jazz.one(
            Tasks.where(Tasks.id eq taskId).select(Tasks.title, Tasks.notes.range(0, 80)),
        )?.get(Tasks.notes)
        // #endregion reading-partial-value-kotlin
        return preview
    }

    suspend fun hopTo(jazz: Jazz, showId: String): List<String> {
        // #region reading-hop-to-kotlin
        // The crew members assigned to the show's unfinished tasks: follow the tasks'
        // `assignee` relation; what comes after the hop filters the crew.
        val busy: List<Crew> = jazz.all(
            Tasks.where(Tasks.showId eq showId, Tasks.status ne TaskStatus.Done)
                .hopTo(Tasks.assignee)
                .orderBy(CrewTable.name.asc()),
        )
        // #endregion reading-hop-to-kotlin
        return busy.map { it.name }
    }

    suspend fun union(jazz: Jazz, showId: String): List<Task> {
        // #region reading-union-kotlin
        // Blocked tasks and unassigned tasks, as one list.
        val attention = jazz.all(
            Tasks.where(Tasks.showId eq showId, Tasks.status eq TaskStatus.Blocked)
                .union(Tasks.where(Tasks.showId eq showId, Tasks.assigneeId.isNull()))
                .orderBy(Tasks.rank.asc()),
        )
        // #endregion reading-union-kotlin
        return attention
    }

    suspend fun history(jazz: Jazz, taskId: String): List<HistoryRow<Task>> {
        // #region reading-history-kotlin
        // Every version of a task the server accepted, newest first.
        val versions: List<HistoryRow<Task>> = jazz.all(
            Tasks.where(Tasks.id eq taskId).orderBy(JazzColumns.acceptedAt.desc()).history(),
            tier = ReadTier.Remote,
        )
        // #endregion reading-history-kotlin
        return versions
    }

    suspend fun includeDeleted(jazz: Jazz, showId: String): List<Task> {
        // #region reading-include-deleted-kotlin
        // Deleted rows too, e.g. for an undo list.
        val withDeleted = jazz.all(Tasks.where(Tasks.showId eq showId).includeDeleted())
        // #endregion reading-include-deleted-kotlin
        return withDeleted
    }
}
