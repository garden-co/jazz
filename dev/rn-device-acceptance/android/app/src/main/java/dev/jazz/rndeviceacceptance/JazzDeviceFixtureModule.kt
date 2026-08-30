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
import android.system.Os
import android.util.Log
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.security.MessageDigest

/**
 * Test-app-only trusted fixture. It is compiled into the development build,
 * not sourced from Metro, an intent, or an OTA update. JavaScript receives
 * only the random capability; identity, claims, SQLite path, and schema stay
 * native. JAZZ_DEVICE_* values are build-time CI test fixtures only.
 */
class JazzDeviceFixtureModule(context: ReactApplicationContext) : ReactContextBaseJavaModule(context) {
  private var capability: ByteArray? = null
  private val diagnosticCodes = setOf(
    "fixture-metadata-failed",
    "native-admission-failed",
    "relay-command-abi-failed",
    "relay-open-failed",
    "relay-attach-failed",
    "relay-probe-failed",
    "relay-cleanup-failed",
    "foreground-byte-abi-failed",
    "foreground-install-failed",
    "foreground-open-failed",
    "foreground-probe-failed",
    "foreground-tick-failed",
    "foreground-close-failed",
    "logout-revocation-failed",
    "public-client-seed-failed",
    "public-client-open-failed",
    "public-client-subscribe-failed",
    "public-client-write-failed",
    "public-client-read-failed",
    "public-client-publish-failed",
    "public-client-shutdown-failed",
    "scope-isolation-failed",
    "scope-isolation-open-failed",
    "scope-isolation-write-failed",
    "scope-isolation-writer-read-failed",
    "scope-isolation-read-failed",
    "scope-isolation-assert-failed",
    "auth-switch-failed",
    "foreground-write-failed",
    "same-runtime-subscription-failed",
    "same-runtime-open-failed",
    "same-runtime-subscribe-failed",
    "same-runtime-initial-reset-failed",
    "same-runtime-write-failed",
    "same-runtime-transaction-open-failed",
    "same-runtime-mutation-stage-failed",
    "same-runtime-commit-failed",
    "same-runtime-delta-failed",
    "same-runtime-postcommit-wake-failed",
    "same-runtime-delta-drain-failed",
    "same-runtime-delta-decode-failed",
    "same-runtime-delta-content-failed",
    "same-runtime-delta-row-id-failed",
    "same-runtime-delta-reset-row-id-failed",
    "same-runtime-delta-incremental-row-id-failed",
    "same-runtime-delta-mixed-row-id-failed",
    "same-runtime-unsubscribe-failed",
    "scope-reopen-failed",
    "public-client-restart-failed",
    "receipt-write-failed",
  )
  override fun getName() = "JazzDeviceFixture"

  private fun scopeConfig(authScope: String): TrustedRelayScopeConfig {
    val userB = authScope == "fixture-user-b"
    val node = if (userB) "22222222-2222-4222-8222-222222222222" else "11111111-1111-4111-8111-111111111111"
    return TrustedRelayScopeConfig(
      appNamespace = BuildConfig.JAZZ_DEVICE_APP_NAMESPACE,
      storageNamespace = BuildConfig.JAZZ_DEVICE_STORAGE_NAMESPACE,
      authScope = authScope,
      // Scope-specific files make a switch physically as well as logically
      // distinct. JavaScript cannot select either path.
      sqlitePath = reactApplicationContext.filesDir.resolve("jazz-device-$authScope.sqlite").absolutePath,
      schemaJson = BuildConfig.JAZZ_DEVICE_SCHEMA_JSON,
      identityJson = "{\"node\":\"$node\",\"author\":\"[\\\"https://jazz.device.test\\\",\\\"$authScope\\\"]\"}",
      claimsJson = BuildConfig.JAZZ_DEVICE_VERIFIED_CLAIMS_JSON,
    )
  }

  @ReactMethod fun admittedCapability(promise: Promise) {
    try {
      capability ?: JazzRelayTrustedAdmission.admit(scopeConfig(BuildConfig.JAZZ_DEVICE_AUTH_SCOPE))
        .also { capability = it }
      promise.resolve(Base64.encodeToString(capability, Base64.NO_WRAP))
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_FIXTURE", error) }
  }

  @ReactMethod fun logout(promise: Promise) {
    capability?.let(JazzRelayTrustedAdmission::revoke)
    capability = null
    promise.resolve(null)
  }

  /** Scope B is selected solely by trusted fixture code. Replacing it revokes
   * A before B can be admitted, so stale JS capability bytes cannot cross it. */
  @ReactMethod fun switchAuthScope(promise: Promise) {
    try {
      JazzRelayTrustedAdmission.replace(capability, scopeConfig("fixture-user-b"))
        .also { capability = it }
      promise.resolve(Base64.encodeToString(capability, Base64.NO_WRAP))
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_FIXTURE", error) }
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

  /** Only the host's bounded acceptance phase crosses this boundary.  It
   * cannot select a relay scope, identity, or filesystem path. */
  @ReactMethod fun acceptancePhase(promise: Promise) {
    try {
      val phase = reactApplicationContext.currentActivity
        ?.intent?.getStringExtra("jazzDeviceAcceptancePhase") ?: "seed"
      require(phase == "seed" || phase == "verify") { "invalid acceptance phase" }
      promise.resolve(phase)
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_FIXTURE", error) }
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

  /** Only fixed, non-secret categories may cross from JS into CI diagnostics. */
  @ReactMethod fun recordDiagnostic(code: String, promise: Promise) {
    try {
      require(code in diagnosticCodes) { "invalid device diagnostic" }
      writeAtomicDiagnostic(code)
      Log.e("JazzDeviceAcceptance", code)
      promise.resolve(null)
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_DIAGNOSTIC", error) }
  }

  @ReactMethod fun clearDiagnostic(promise: Promise) {
    try {
      val target = reactApplicationContext.cacheDir.resolve("jazz-device-diagnostic.txt")
      check(!target.exists() || target.delete()) { "failed to clear device diagnostic" }
      promise.resolve(null)
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_DIAGNOSTIC", error) }
  }

  /**
   * A receipt timeout can race process teardown. Never leave a partially
   * written diagnostic for the host to inspect: flush the private temporary
   * file, then rename it within its own cache directory (POSIX-atomic).
   */
  private fun writeAtomicDiagnostic(code: String) {
    val target = reactApplicationContext.cacheDir.resolve("jazz-device-diagnostic.txt")
    val parent = target.parentFile ?: error("device diagnostic has no parent directory")
    val temporary = File.createTempFile(".${target.name}.", ".tmp", parent)
    try {
      FileOutputStream(temporary).use { output ->
        output.write(code.toByteArray(Charsets.UTF_8))
        output.fd.sync()
      }
      Os.rename(temporary.absolutePath, target.absolutePath)
    } catch (failure: Throwable) {
      if (temporary.exists() && !temporary.delete())
        failure.addSuppressed(IOException("failed to remove incomplete device diagnostic"))
      throw failure
    }
  }
}
