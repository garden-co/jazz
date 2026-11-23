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
import kotlinx.coroutines.NonCancellable
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
 * One phase of the smoke test per launch. dev/scripts/test-kotlin-android-emulator.sh
 * kills the app between phases and runs every phase, whatever failed before.
 * Each rule logs one line, `<run> RULE <id> PASS|FAIL|BLOCKED: <detail>`.
 * BLOCKED means something the rule needs failed first. Each phase ends with
 * `<run> <phase> DONE`.
 *
 * - `memory`: memory storage. Signs in, writes, watches, syncs to a second
 *   client, and is refused as another user.
 * - `write`: persistent stores for a bearer user and a device account. Writes,
 *   closes, reopens and reads, writes offline, and is left open to be killed.
 * - `offline`: run with the server unreachable (`adb reverse --remove`): each
 *   store opens and reads what the killed process wrote.
 * - `restart`: the server is back. The offline write syncs, then the store's
 *   rules (one app per store directory): a second open is refused in this
 *   process and in another ([HolderService]), a killed holder lets go, and the
 *   store reopens after a close, a failed open and a delete.
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
        val smoke = Smoke(filesDir, endpoint, appId, Rules(run))
        val holder = Intent(this, HolderService::class.java).putExtra("endpoint", endpoint).putExtra("appId", appId)
        // Not the activity's scope: a phase ends only by finishing.
        CoroutineScope(Dispatchers.Main).launch {
            try {
                withTimeout(120_000) {
                    when (phase) {
                        "memory" -> smoke.memory()
                        "write" -> smoke.write()
                        "offline" -> smoke.offline()
                        "restart" -> smoke.restart(hold = { startService(holder) }, letGo = { stopService(holder) })
                        else -> error("no phase $phase")
                    }
                }
            } catch (e: Throwable) {
                Log.e(TAG, "$run $phase stopped early", e)
            }
            Log.i(TAG, "$run $phase DONE")
        }
    }
}

/** Logs one line per rule; the script scores them. */
internal class Rules(private val run: String) {
    private fun line(id: String, outcome: String, detail: String) {
        val text = "$run RULE $id $outcome: $detail"
        if (outcome == "PASS") Log.i(TAG, text) else Log.e(TAG, text)
    }

    /** Runs [check] as rule [id]: PASS if it returns, FAIL if it throws. */
    suspend fun <T> rule(id: String, check: suspend () -> T): T? = try {
        check().also { line(id, "PASS", "ok") }
    } catch (e: Throwable) {
        line(id, "FAIL", "${e::class.java.simpleName}: ${e.message}")
        null
    }

    fun blocked(id: String, why: String) = line(id, "BLOCKED", why)
}

internal class Smoke(
    private val files: File,
    private val endpoint: String,
    private val appId: String,
    private val rules: Rules,
) {
    private val store = File(files, "jazz")
    private val deviceStore = File(files, "device")
    private val savedShow = File(files, "show")
    private val savedDeviceShow = File(files, "device-show")
    private val savedKey = File(files, "device.key")

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

    private suspend fun openDevice(): Jazz = open(JazzAuth.DeviceAccount(savedKey.readText()), deviceStore)

    private fun user(name: String) = JazzAuth.Bearer { "token-$name" }

    private suspend fun <T> using(jazz: Jazz, block: suspend (Jazz) -> T): T =
        try {
            block(jazz)
        } finally {
            // Closed even if the caller is cancelled: the store is let go of.
            withContext(NonCancellable + Dispatchers.IO) { jazz.close() }
        }

    private fun task(show: String, title: String, rank: Double) =
        NewTask(showId = show, title = title, status = TaskStatus.Todo, rank = rank)

    private fun me(jazz: Jazz): String = (jazz.session.value as SessionState.SignedIn).user.account

    private suspend fun newShow(jazz: Jazz, tier: WriteTier? = null): String = jazz.insert(
        Shows,
        NewShow(name = "Smoke", venue = "Emulator", date = "TBA", doors = "TBA", chiefAccount = me(jazz)),
        tier,
    ).id

    private fun titles(jazz: Jazz, show: String): suspend () -> Set<String> =
        { jazz.all(Tasks.where(Tasks.showId eq show), ReadTier.Local).map { it.title }.toSet() }

    private suspend fun refused(what: String, open: suspend () -> Jazz) {
        val opened = try {
            open()
        } catch (e: JazzException.Refused) {
            return
        }
        withContext(NonCancellable + Dispatchers.IO) { opened.close() }
        error("$what opened")
    }

    suspend fun memory() {
        rules.rule("android.memory") {
            using(open(user("memory"))) { jazz ->
                val user = (jazz.session.value as SessionState.SignedIn).user
                check(user.subject == "memory") { "signed in as $user" }
                val show = newShow(jazz, WriteTier.Global)
                val tasks = Tasks.where(Tasks.showId eq show)
                coroutineScope {
                    val watched = async { jazz.watch(tasks).first { rows -> rows.any { it.title == "Synced" } } }
                    jazz.insert(Tasks, task(show, "Synced", 1.0), WriteTier.Global)
                    watched.await()
                }
                check(jazz.all(tasks).map { it.title } == listOf("Synced")) { "watch and read" }
                using(open(user("memory"))) { second ->
                    check(second.all(tasks, ReadTier.Remote).map { it.title } == listOf("Synced")) { "second client" }
                }
                // A device account (key generation is reflective), and a
                // refusal's exception type: both are what R8 would break.
                using(open(JazzAuth.DeviceAccount(JazzAuth.generateDeviceKey()))) { stranger ->
                    check(stranger.all(tasks, ReadTier.Remote).isEmpty()) { "another user read the show" }
                    try {
                        stranger.insert(Tasks, task(show, "Sneaky", 9.0), WriteTier.Global)
                        error("another user wrote into the show")
                    } catch (_: JazzException.Permission) {
                    }
                }
            }
        }
    }

    /** One store: written online, reopened, written offline, left open. */
    private suspend fun writeStore(id: String, dir: File, opener: suspend () -> Jazz, saved: File) {
        check(dir.mkdirs()) { "$dir is left from another run" }
        val show = rules.rule("persist.reopen.$id") {
            val show = using(opener()) { jazz ->
                newShow(jazz, WriteTier.Global).also { jazz.insert(Tasks, task(it, "Synced", 1.0), WriteTier.Global) }
            }
            val reopened = opener()
            leftOpen += reopened
            check(titles(reopened, show)() == setOf("Synced")) { "not there after a reopen" }
            reopened.disconnect()
            reopened.insert(Tasks, task(show, "Offline", 2.0))
            show
        } ?: return
        saved.writeText(show)
    }

    suspend fun write() {
        writeStore("bearer", store, ::openPersistent, savedShow)
        savedKey.writeText(JazzAuth.generateDeviceKey())
        writeStore("device", deviceStore, ::openDevice, savedDeviceShow)
    }

    /** With the server unreachable, after the process was killed. */
    suspend fun offline() {
        for ((id, opener, saved) in listOf(
            Triple("device", ::openDevice, savedDeviceShow),
            Triple("bearer", ::openPersistent, savedShow),
        )) {
            val rule = "offline.$id-open"
            if (!saved.exists()) {
                rules.blocked(rule, "the write phase didn't write this store")
                continue
            }
            rules.rule(rule) {
                withTimeout(20_000) {
                    using(opener()) { jazz ->
                        val stored = titles(jazz, saved.readText())()
                        check(stored == setOf("Synced", "Offline")) { "the store has $stored" }
                    }
                }
            }
        }
    }

    suspend fun restart(hold: () -> Unit, letGo: () -> Unit) {
        val rulesNeedingTheStore = listOf(
            "persist.killed-process", "sync.offline-write", "store.second-open", "store.reopen",
            "store.second-open.process", "store.killed-holder", "store.reopen-after-failed-open", "store.delete-reopen",
        )
        if (!savedShow.exists()) {
            rulesNeedingTheStore.forEach { rules.blocked(it, "the write phase didn't write the store") }
            return
        }
        val show = savedShow.readText()
        val tasks = Tasks.where(Tasks.showId eq show)
        val expected = setOf("Synced", "Offline")
        val sqlite = File(store, "jazz.sqlite")

        using(openPersistent()) { jazz ->
            rules.rule("persist.killed-process") {
                val stored = titles(jazz, show)()
                check(stored == expected) { "after the process was killed the store has $stored" }
            }
            rules.rule("sync.offline-write") {
                withTimeout(30_000) {
                    using(open(user("smoke"))) { second ->
                        second.watch(tasks, ReadTier.Remote).first { rows -> rows.map { it.title }.toSet() == expected }
                    }
                }
            }
            rules.rule("store.second-open") {
                refused("a second app on the store, in this process", ::openPersistent)
                check(titles(jazz, show)() == expected) { "the first app can't read after the refusal" }
                jazz.insert(Tasks, task(show, "After refusal", 3.0))
            }
        }

        rules.rule("store.reopen") {
            // Right after close returned: no wait, no retry.
            using(openPersistent()) { check(titles(it, show)().containsAll(expected)) }
        }

        val held = File(files, HolderService.HELD)
        val outcome = File(files, HolderService.OUTCOME)
        held.delete()
        outcome.delete()
        val holding = using(openPersistent()) {
            rules.rule("store.second-open.process") {
                hold()
                withTimeout(30_000) { while (!outcome.exists()) delay(100) }
                val seen = outcome.readText()
                check(seen == "refused") { "the second process's open: $seen" }
            }
        }
        if (holding == null) {
            letGo()
            withTimeout(30_000) { while (outcome.exists() || held.exists()) delay(100) }
        } else {
            // This app has closed, so the service now holds the store.
            rules.rule("store.killed-holder") {
                withTimeout(30_000) { while (!held.exists()) delay(100) }
                refused("an app on a store another process holds", ::openPersistent)
                letGo()
                withTimeout(30_000) { while (held.exists()) delay(100) }
                // The process is SIGKILLed after it deletes the file: allow it a moment to die.
                var opened: Jazz? = null
                val until = System.nanoTime() + 5_000_000_000
                while (opened == null) {
                    try {
                        opened = openPersistent()
                    } catch (e: JazzException.Refused) {
                        if (System.nanoTime() > until) throw e
                        delay(100)
                    }
                }
                using(opened) { check(titles(it, show)().containsAll(expected)) }
            }
        }

        rules.rule("store.reopen-after-failed-open") {
            val failing = JazzAuth.Bearer { throw IllegalStateException("no token, on purpose") }
            val opened = runCatching { open(failing, store) }
            opened.getOrNull()?.let { withContext(NonCancellable + Dispatchers.IO) { it.close() } }
            check(opened.isFailure) { "an open whose token callback throws opened" }
            check(opened.exceptionOrNull() !is JazzException.Refused) { "refused, not failed: ${opened.exceptionOrNull()}" }
            using(openPersistent()) { check(titles(it, show)().containsAll(expected)) }
        }

        rules.rule("store.delete-reopen") {
            using(openPersistent()) { it.deleteClientStorage() }
            check(!sqlite.exists()) { "deleteClientStorage left the store" }
            using(openPersistent()) { fresh ->
                check(titles(fresh, show)().isEmpty()) { "the deleted store still has rows" }
                newShow(fresh, WriteTier.Global)
            }
        }
    }

    private companion object {
        /** Kept reachable until the process is killed. */
        @Volatile var leftOpen: List<Jazz> = emptyList()
    }
}
