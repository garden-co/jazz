package smoke

import android.app.Service
import android.content.Intent
import android.os.IBinder
import android.util.Log
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.concurrent.thread
import kotlin.system.exitProcess

/**
 * Another process with an app open on the same store (`android:process` in
 * the manifest), until it is stopped: what `deleteClientStorage` must refuse.
 */
class HolderService : Service() {
    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val extras = requireNotNull(intent)
        val smoke = Smoke(filesDir, requireNotNull(extras.getStringExtra("endpoint")), requireNotNull(extras.getStringExtra("appId")))
        thread {
            try {
                // Never closed: the process ends with it open.
                open = runBlocking { smoke.openPersistent() }
                File(filesDir, HELD).writeText("held")
            } catch (e: Throwable) {
                Log.e(TAG, "the holder process couldn't open the store", e)
            }
        }
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        super.onDestroy()
        // Reading it here keeps R8 from dropping it, and the GC the app.
        Log.i(TAG, "the holder process stops, holding $open")
        File(filesDir, HELD).delete()
        exitProcess(0)
    }

    @Volatile private var open: tools.jazz.Jazz? = null

    companion object {
        const val HELD = "held"
    }
}
