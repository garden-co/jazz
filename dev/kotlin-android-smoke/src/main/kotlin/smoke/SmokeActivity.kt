package smoke

import android.app.Activity
import android.os.Bundle
import android.util.Log
import jazzapp.NewShow
import jazzapp.NewTask
import jazzapp.Shows
import jazzapp.TaskStatus
import jazzapp.Tasks
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import tools.jazz.Jazz
import tools.jazz.JazzAuth
import tools.jazz.JazzConfig
import tools.jazz.JazzException
import tools.jazz.ReadTier
import tools.jazz.SessionState
import tools.jazz.Storage
import tools.jazz.WriteTier
import java.io.File

private const val TAG = "JazzSmoke"

/**
 * One phase of the smoke test per launch (dev/scripts/test-kotlin-android-emulator.sh
 * force-stops the app between them), ending in one log line: `<run> <phase> PASS`
 * or `<run> <phase> FAIL`.
 *
 * - `write`: opens a persistent app, writes, watches and syncs, is refused as
 *   another user, writes offline and closes.
 * - `restart`: a new process reads it all from the store, syncs the offline
 *   write to a second client, and deletes the store.
 *
 * Everything is called from the main thread, as an app would: the calls say
 * they are main-safe. [Jazz.close] is the exception, so it is moved off it.
 */
class SmokeActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        fun extra(name: String): String = requireNotNull(intent.getStringExtra(name)) { "no $name extra" }
        val run = extra("run")
        val phase = extra("phase")
        val smoke = Smoke(filesDir, extra("endpoint"), extra("appId"))
        // Not the activity's scope: a phase ends only by passing or failing.
        CoroutineScope(Dispatchers.Main).launch {
            try {
                withTimeout(120_000) {
                    when (phase) {
                        "write" -> smoke.write()
                        "restart" -> smoke.restart()
                        else -> error("no phase $phase")
                    }
                }
                Log.i(TAG, "$run $phase PASS")
            } catch (e: Throwable) {
                Log.e(TAG, "$run $phase FAIL", e)
            }
        }
    }
}

private class Smoke(private val files: File, private val endpoint: String, private val appId: String) {
    private val store = File(files, "jazz")
    private val savedShow = File(files, "show")

    private suspend fun open(auth: JazzAuth, dataDir: File? = null): Jazz = jazzapp.open(
        JazzConfig(
            appId = appId,
            serverUrl = endpoint,
            storage = if (dataDir == null) Storage.Memory else Storage.Persistent,
            dataDir = dataDir?.path,
            auth = auth,
        ),
    )

    private fun user(name: String) = JazzAuth.Bearer { "token-$name" }

    private suspend fun <T> using(jazz: Jazz, block: suspend (Jazz) -> T): T =
        try {
            block(jazz)
        } finally {
            withContext(Dispatchers.IO) { jazz.close() }
        }

    private fun task(show: String, title: String, rank: Double) =
        NewTask(showId = show, title = title, status = TaskStatus.Todo, rank = rank)

    private fun step(what: String) {
        Log.i(TAG, "ok: $what")
    }

    suspend fun write() {
        check(store.mkdirs()) { "$store is left from another run" }
        using(open(user("smoke"), store)) { jazz ->
            val me = (jazz.session.value as SessionState.SignedIn).user
            check(me.subject == "smoke") { "signed in as $me" }
            step("open, signed in through the token callback")

            val show = jazz.insert(
                Shows,
                NewShow(name = "Smoke", venue = "Emulator", date = "TBA", doors = "TBA", chiefAccount = me.account),
                WriteTier.Global,
            ).id
            savedShow.writeText(show)
            step("write, accepted by the server")

            val tasks = Tasks.where(Tasks.showId eq show)
            coroutineScope {
                val watched = async { jazz.watch(tasks).first { rows -> rows.any { it.title == "Synced" } } }
                jazz.insert(Tasks, task(show, "Synced", 1.0), WriteTier.Global)
                watched.await()
            }
            check(jazz.all(tasks, ReadTier.Remote).map { it.title } == listOf("Synced"))
            step("watch and read")

            // A device account (key generation is reflective), and a refusal's
            // exception type: both are what R8 would break.
            using(open(JazzAuth.DeviceAccount(JazzAuth.generateDeviceKey()))) { stranger ->
                check(stranger.all(tasks, ReadTier.Remote).isEmpty()) { "another user read the show" }
                try {
                    stranger.insert(Tasks, task(show, "Sneaky", 9.0), WriteTier.Global)
                    error("another user wrote into the show")
                } catch (e: JazzException.Permission) {
                    step("device account, refused as Permission")
                }
            }

            jazz.disconnect()
            jazz.insert(Tasks, task(show, "Offline", 2.0))
            step("write offline")
        }
        step("close")
    }

    suspend fun restart() {
        val show = savedShow.readText()
        val tasks = Tasks.where(Tasks.showId eq show)
        val expected = setOf("Synced", "Offline")
        val jazz = open(user("smoke"), store)
        using(jazz) {
            val stored = jazz.all(tasks, ReadTier.Local).map { it.title }.toSet()
            check(stored == expected) { "after a restart the store has $stored" }
            step("read from the store after a process restart")

            using(open(user("smoke"))) { second ->
                second.watch(tasks, ReadTier.Remote).first { rows -> rows.map { it.title }.toSet() == expected }
            }
            step("the offline write synced to a second client")

            jazz.deleteClientStorage()
            check(!File(store, "jazz.sqlite").exists()) { "deleteClientStorage left the store" }
            step("delete the store")
        }
    }
}
