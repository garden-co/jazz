package com.jazzrn

import android.content.ContentProvider
import android.content.ContentValues
import android.database.Cursor
import android.net.Uri

/**
 * Starts the relay prewarm at process start, off the main thread: the relay the last launch
 * admitted opens while the app is still loading its JavaScript.
 */
class JazzRelayPrewarmProvider : ContentProvider() {
  override fun onCreate(): Boolean {
    val app = context?.applicationContext ?: return true
    Thread({
      try {
        JazzRelayBridge.prewarm(app)
      } catch (error: Throwable) {
        android.util.Log.w("JazzRelay", "relay prewarm failed", error)
      }
    }, "jazz-prewarm").start()
    return true
  }

  override fun query(uri: Uri, projection: Array<out String>?, selection: String?, selectionArgs: Array<out String>?, sortOrder: String?): Cursor? = null
  override fun getType(uri: Uri): String? = null
  override fun insert(uri: Uri, values: ContentValues?): Uri? = null
  override fun delete(uri: Uri, selection: String?, selectionArgs: Array<out String>?): Int = 0
  override fun update(uri: Uri, values: ContentValues?, selection: String?, selectionArgs: Array<out String>?): Int = 0
}
