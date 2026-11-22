package smoke

import android.app.Service
import android.content.Intent
import android.os.IBinder
import android.os.Process
import android.util.Log
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import tools.jazz.JazzException
import java.io.File
import kotlin.concurrent.thread

/**
 * Another process (`android:process` in the manifest) opening the activity's
 * store. Its first open should be refused while the activity holds the store.
 * It writes that outcome to [OUTCOME], then keeps trying until it holds the
 * store ([HELD]), and keeps it until it is stopped and killed.
 */
class HolderService : Service() {
    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val extras = requireNotNull(intent)
        val smoke = Smoke(
            filesDir,
            requireNotNull(extras.getStringExtra("endpoint")),
            requireNotNull(extras.getStringExtra("appId")),
            Rules("holder"),
        )
        thread {
            runBlocking {
                var first = true
                while (open == null) {
                    try {
                        // Never closed: the process ends with it open.
                        open = smoke.openPersistent()
                        if (first) File(filesDir, OUTCOME).writeText("opened")
                    } catch (e: JazzException.Refused) {
                        if (first) File(filesDir, OUTCOME).writeText("refused")
                        delay(100)
                    } catch (e: Throwable) {
                        Log.e(TAG, "the holder process couldn't open the store", e)
                        if (first) File(filesDir, OUTCOME).writeText("failed: $e")
                        return@runBlocking
                    }
                    first = false
                }
                File(filesDir, HELD).writeText("held")
            }
        }
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        super.onDestroy()
        // Reading it here keeps R8 from dropping it, and the GC the app.
        Log.i(TAG, "the holder process stops, holding $open")
        File(filesDir, OUTCOME).delete()
        File(filesDir, HELD).delete()
        // SIGKILL, with the app open: the lock has to go with the process.
        Process.killProcess(Process.myPid())
    }

    @Volatile private var open: tools.jazz.Jazz? = null

    companion object {
        const val HELD = "held"
        const val OUTCOME = "first-open"
    }
}
