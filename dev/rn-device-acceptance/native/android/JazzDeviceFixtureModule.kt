package dev.jazz.rndeviceacceptance

import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import com.jazzrn.JazzRelayTrustedAdmission
import com.jazzrn.TrustedRelayScopeConfig
import android.util.Base64
import android.os.Build
import java.io.FileInputStream
import java.security.MessageDigest

/**
 * Test-app-only trusted fixture. It is compiled into the development build,
 * not sourced from Metro, an intent, or an OTA update. JavaScript receives
 * only the random capability; identity, claims, SQLite path, and schema stay
 * native. JAZZ_DEVICE_* values are build-time CI test fixtures only.
 */
class JazzDeviceFixtureModule(context: ReactApplicationContext) : ReactContextBaseJavaModule(context) {
  private var capability: ByteArray? = null
  override fun getName() = "JazzDeviceFixture"

  @ReactMethod fun admittedCapability(promise: Promise) {
    try {
      capability ?: JazzRelayTrustedAdmission.admit(TrustedRelayScopeConfig(
        appNamespace = BuildConfig.JAZZ_DEVICE_APP_NAMESPACE,
        storageNamespace = BuildConfig.JAZZ_DEVICE_STORAGE_NAMESPACE,
        authScope = BuildConfig.JAZZ_DEVICE_AUTH_SCOPE,
        sqlitePath = reactApplicationContext.filesDir.resolve("jazz-device.sqlite").absolutePath,
        schemaJson = BuildConfig.JAZZ_DEVICE_SCHEMA_JSON,
        identityJson = BuildConfig.JAZZ_DEVICE_VERIFIED_IDENTITY_JSON,
        claimsJson = BuildConfig.JAZZ_DEVICE_VERIFIED_CLAIMS_JSON,
      )).also { capability = it }
      promise.resolve(Base64.encodeToString(capability, Base64.NO_WRAP))
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_FIXTURE", error) }
  }

  @ReactMethod fun logout(promise: Promise) {
    capability?.let(JazzRelayTrustedAdmission::revoke)
    capability = null
    promise.resolve(null)
  }

  @ReactMethod fun receiptContext(promise: Promise) {
    try {
      val activity = reactApplicationContext.currentActivity
        ?: error("acceptance activity is unavailable")
      val nonce = activity.intent.getStringExtra("jazzDeviceRunNonce")
        ?: error("acceptance launch did not include a run nonce")
      // Hash the installed package itself, rather than echoing an adb extra.
      val buildFingerprint = sha256File(reactApplicationContext.applicationInfo.sourceDir)
      val deviceIdentifier = Build.FINGERPRINT.takeIf(String::isNotBlank)
        ?: error("Android build fingerprint is unavailable")
      promise.resolve(Arguments.createMap().apply {
        putString("platform", "android")
        putString("deviceIdentifier", deviceIdentifier)
        putString("buildFingerprint", buildFingerprint)
        putString("runNonce", nonce)
      })
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_RECEIPT_CONTEXT", error) }
  }

  private fun sha256File(path: String): String {
    val digest = MessageDigest.getInstance("SHA-256")
    FileInputStream(path).use { input ->
      val buffer = ByteArray(32 * 1024)
      while (true) {
        val count = input.read(buffer)
        if (count < 0) break
        digest.update(buffer, 0, count)
      }
    }
    return digest.digest().joinToString("") { "%02x".format(it) }
  }

  /** See the matching iOS fixture: JavaScript supplies the verified protocol
   * line after its relay proof; this method only persists that line. */
  @ReactMethod fun recordReceipt(receipt: String, promise: Promise) {
    try {
      require(receipt.startsWith("JAZZ_DEVICE_RESULT ") && receipt.length <= 16_384) {
        "invalid device receipt"
      }
      reactApplicationContext.cacheDir.resolve("jazz-device-receipt.ndjson")
        .writeText("$receipt\n")
      promise.resolve(null)
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_RECEIPT", error) }
  }
}
