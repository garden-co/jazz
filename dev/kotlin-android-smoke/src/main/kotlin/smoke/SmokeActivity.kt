package smoke

import android.app.Activity
import android.content.Intent
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
import kotlinx.coroutines.delay
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

internal const val TAG = "JazzSmoke"

/**
 * One phase of the smoke test per launch (dev/scripts/test-kotlin-android-emulator.sh
 * kills the app between them and runs every phase, whatever failed before),
 * ending in one log line: `<run> <phase> PASS` or `<run> <phase> FAIL`.
 *
 * - `memory`: memory storage, on its own: signs in, writes, watches, syncs to a
 *   second client, and is refused as another user.
 * - `write`: persistent storage: writes, closes, reopens and reads, writes
 *   offline, and is left open to be killed.
 * - `restart`: a new process reads what the killed one wrote and syncs it, then
 *   the store's lock: two opens coexist, a delete is refused while another app
 *   of this process or of a second one ([HolderService]) holds it, a killed
 *   holder lets go, and a deleted store opens again.
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
        val endpoint = extra("endpoint")
        val appId = extra("appId")
        val smoke = Smoke(filesDir, endpoint, appId)
        val holder = Intent(this, HolderService::class.java).putExtra("endpoint", endpoint).putExtra("appId", appId)
        // Not the activity's scope: a phase ends only by passing or failing.
        CoroutineScope(Dispatchers.Main).launch {
            try {
                withTimeout(120_000) {
                    when (phase) {
                        "memory" -> smoke.memory()
                        "write" -> smoke.write()
                        "restart" -> smoke.restart(hold = { startService(holder) }, letGo = { stopService(holder) })
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

internal class Smoke(private val files: File, private val endpoint: String, private val appId: String) {
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

    /** The signed-in user's app on the persistent store. */
    suspend fun openPersistent(): Jazz = open(user("smoke"), store)

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

    private fun me(jazz: Jazz): String = (jazz.session.value as SessionState.SignedIn).user.account

    private suspend fun newShow(jazz: Jazz, tier: WriteTier? = null): String = jazz.insert(
        Shows,
        NewShow(name = "Smoke", venue = "Emulator", date = "TBA", doors = "TBA", chiefAccount = me(jazz)),
        tier,
    ).id

    suspend fun memory() {
        using(open(user("memory"))) { jazz ->
            val user = (jazz.session.value as SessionState.SignedIn).user
            check(user.subject == "memory") { "signed in as $user" }
            step("open in memory, signed in through the token callback")

            val show = newShow(jazz, WriteTier.Global)
            step("write, accepted by the server")

            val tasks = Tasks.where(Tasks.showId eq show)
            coroutineScope {
                val watched = async { jazz.watch(tasks).first { rows -> rows.any { it.title == "Synced" } } }
                jazz.insert(Tasks, task(show, "Synced", 1.0), WriteTier.Global)
                watched.await()
            }
            check(jazz.all(tasks).map { it.title } == listOf("Synced"))
            step("watch and read")

            using(open(user("memory"))) { second ->
                check(second.all(tasks, ReadTier.Remote).map { it.title } == listOf("Synced"))
            }
            step("sync to a second client")

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
        }
        step("close")
    }

    suspend fun write() {
        check(store.mkdirs()) { "$store is left from another run" }
        val show = using(openPersistent()) { jazz ->
            val show = newShow(jazz, WriteTier.Global)
            jazz.insert(Tasks, task(show, "Synced", 1.0), WriteTier.Global)
            step("persistent open and write")
            show
        }
        val tasks = Tasks.where(Tasks.showId eq show)
        // Not closed: the script kills the process with it open.
        val reopened = openPersistent().also { leftOpen = it }
        check(reopened.all(tasks, ReadTier.Local).map { it.title } == listOf("Synced")) { "not there after a reopen" }
        step("close, reopen and read")

        reopened.disconnect()
        reopened.insert(Tasks, task(show, "Offline", 2.0))
        savedShow.writeText(show)
        step("write offline, left open to be killed")
    }

    suspend fun restart(hold: () -> Unit, letGo: () -> Unit) {
        val show = savedShow.readText()
        val tasks = Tasks.where(Tasks.showId eq show)
        val expected = setOf("Synced", "Offline")
        val sqlite = File(store, "jazz.sqlite")
        using(openPersistent()) { jazz ->
            val stored = jazz.all(tasks, ReadTier.Local).map { it.title }.toSet()
            check(stored == expected) { "after the process was killed the store has $stored" }
            step("read from the store after the process was killed")

            using(open(user("smoke"))) { second ->
                second.watch(tasks, ReadTier.Remote).first { rows -> rows.map { it.title }.toSet() == expected }
            }
            step("the offline write synced to a second client")

            using(openPersistent()) { beside ->
                check(beside.all(tasks, ReadTier.Local).isNotEmpty())
                step("two apps open one store")
                try {
                    jazz.deleteClientStorage()
                    error("deleted the store another app of this process holds")
                } catch (e: JazzException.Refused) {
                    check(sqlite.exists()) { "a refused delete removed the store" }
                    check(jazz.all(tasks, ReadTier.Local).isNotEmpty()) { "that refusal closed the app" }
                    step("delete refused in the same process, still open")
                }
            }

            val held = File(files, HolderService.HELD)
            hold()
            while (!held.exists()) delay(100)
            step("a second process opens the store")
            try {
                jazz.deleteClientStorage()
                error("deleted the store another process holds")
            } catch (e: JazzException.Refused) {
                check(sqlite.exists()) { "a refused delete removed the store" }
                step("delete refused while another process holds the store")
            }
            letGo()
            while (held.exists()) delay(100)
        }
        // The refusal closed that app, and the holder was killed with its app
        // open: that takes a moment.
        while (true) {
            try {
                using(openPersistent()) { it.deleteClientStorage() }
                break
            } catch (e: JazzException.Refused) {
                delay(200)
            }
        }
        check(!sqlite.exists()) { "deleteClientStorage left the store" }
        step("a killed holder lets go, and the delete succeeds")

        using(openPersistent()) { fresh ->
            check(fresh.all(tasks, ReadTier.Local).isEmpty()) { "the deleted store still has rows" }
            newShow(fresh, WriteTier.Global)
        }
        step("reopen after the delete")
    }

    private companion object {
        /** Kept reachable until the process is killed. */
        @Volatile var leftOpen: Jazz? = null
    }
}
