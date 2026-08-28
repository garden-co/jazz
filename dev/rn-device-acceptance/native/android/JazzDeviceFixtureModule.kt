package dev.jazz.rndeviceacceptance

import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import com.jazzrn.JazzRelayTrustedAdmission
import com.jazzrn.TrustedRelayScopeConfig
import android.util.Base64
import android.provider.Settings

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

  /**
   * Launch evidence is read by trusted host code. In particular, JavaScript
   * does not get to select the nonce, artifact fingerprint, or device id that
   * it later places in a device receipt.
   */
  @ReactMethod fun receiptContext(promise: Promise) {
    try {
      val activity = reactApplicationContext.currentActivity
        ?: error("acceptance activity is unavailable")
      val nonce = activity.intent.getStringExtra("jazzDeviceRunNonce")
        ?: error("acceptance launch did not include a run nonce")
      val buildFingerprint = activity.intent.getStringExtra("jazzDeviceBuildFingerprint")
        ?: error("acceptance launch did not include an APK fingerprint")
      val deviceIdentifier = Settings.Secure.getString(
        reactApplicationContext.contentResolver,
        Settings.Secure.ANDROID_ID,
      ) ?: error("Android secure device identifier is unavailable")
      promise.resolve(Arguments.createMap().apply {
        putString("platform", "android")
        putString("deviceIdentifier", deviceIdentifier)
        putString("buildFingerprint", buildFingerprint)
        putString("runNonce", nonce)
      })
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_RECEIPT_CONTEXT", error) }
  }
}
