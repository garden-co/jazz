package dev.jazz.rndeviceacceptance

import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import com.jazzrn.JazzRelayTrustedAdmission
import android.util.Base64
import android.os.Build
import android.system.Os
import android.util.Log
import android.security.NetworkSecurityPolicy
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.security.MessageDigest
import java.net.HttpURLConnection
import java.net.URL
import org.json.JSONObject

/**
 * Test-app-only trusted fixture. It is compiled into the development build,
 * not sourced from Metro, an intent, or an OTA update. JavaScript receives
 * only the random capability; endpoint and short-lived bearers arrive from
 * the local Edge/Core harness as launch-only native inputs. No bearer,
 * signing material, or trusted generic-admission configuration is checked in.
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
    "foreground-abi-version-failed",
    "foreground-revocation-failed",
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
    "public-client-core-observation-failed",
    "core-observation-cleartext-denied",
    "public-client-shutdown-failed",
    "public-client-relay-readback-failed",
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
    "same-runtime-delta-written-content-row-id-failed",
    "same-runtime-delta-reset-row-id-failed",
    "same-runtime-delta-incremental-row-id-failed",
    "same-runtime-delta-mixed-row-id-failed",
    "same-runtime-unsubscribe-failed",
    "scope-reopen-failed",
    "public-client-restart-failed",
    "receipt-write-failed",
  )
  override fun getName() = "JazzDeviceFixture"

  private data class PrivateSessionInputs(val endpoint: String, val bearer: String)

  /** The harness is the only source of endpoint/bearer material. JavaScript
   * never sees either value; it receives only the capability returned after
   * the Rust private-session and credential-free schema handoff. */
  private fun privateSessionInputs(scope: String): PrivateSessionInputs {
    val activity = reactApplicationContext.currentActivity
      ?: error("acceptance activity is unavailable")
    val endpoint = activity.intent.getStringExtra("jazzDeviceEdgeEndpoint")
      ?: error("acceptance launch did not include a local Edge endpoint")
    val bearerKey = if (scope == "b") "jazzDeviceBearerB" else "jazzDeviceBearerA"
    val bearer = activity.intent.getStringExtra(bearerKey)
      ?: error("acceptance launch did not include an ephemeral bearer")
    require(endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
      "invalid local Edge endpoint"
    }
    require(bearer.length in 16..16_384 && bearer.count { it == '.' } == 2) {
      "invalid ephemeral bearer"
    }
    return PrivateSessionInputs(endpoint, bearer)
  }

  private fun admitPrivateSession(scope: String): ByteArray {
    val inputs = privateSessionInputs(scope)
    val setup: ByteArray = JazzRelayTrustedAdmission.beginPrivateSession(
      reactApplicationContext,
      inputs.endpoint,
      BuildConfig.JAZZ_DEVICE_APP_ID,
      inputs.bearer,
    )
    return JazzRelayTrustedAdmission.attachCanonicalSchema(setup, BuildConfig.JAZZ_DEVICE_SCHEMA_JSON)
  }

  @ReactMethod fun admittedCapability(promise: Promise) {
    try {
      capability ?: admitPrivateSession("a")
        .also { capability = it }
      promise.resolve(Base64.encodeToString(capability, Base64.NO_WRAP))
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_FIXTURE", error) }
  }

  @ReactMethod fun logout(promise: Promise) {
    capability?.let(JazzRelayTrustedAdmission::revoke)
    capability = null
    promise.resolve(null)
  }

  /** The native fixture revokes A before it accepts harness bearer B, so stale
   * JS capability bytes cannot cross the authenticated scope boundary. */
  @ReactMethod fun switchAuthScope(promise: Promise) {
    try {
      capability?.let(JazzRelayTrustedAdmission::revoke)
      capability = null
      admitPrivateSession("b").also { capability = it }
      promise.resolve(Base64.encodeToString(capability, Base64.NO_WRAP))
    } catch (error: Throwable) { promise.reject("E_JAZZ_DEVICE_FIXTURE", error) }
  }

  /** The host acknowledges only after its independent Core reader sees the
   * run's write. No bearer or endpoint is exposed to JavaScript. */
  @ReactMethod fun waitForCoreObservation(promise: Promise) {
    try {
      val activity = reactApplicationContext.currentActivity
        ?: error("acceptance activity is unavailable")
      val endpoint = activity.intent.getStringExtra("jazzDeviceCoreObservationEndpoint")
        ?: error("missing Core observation endpoint")
      val url = URL(endpoint)
      if (url.protocol == "http" && !NetworkSecurityPolicy.getInstance().isCleartextTrafficPermitted(url.host)) {
        Log.e("JazzDeviceAcceptance", "core-observation-cleartext-denied")
        promise.reject("E_JAZZ_DEVICE_CORE", "Core observation fixture host denied by network policy")
        return
      }
      val nonce = activity.intent.getStringExtra("jazzDeviceRunNonce")
        ?: error("missing acceptance run nonce")
      val identity = JSONObject().apply {
        put("platform", "android")
        put("deviceIdentifier", Build.FINGERPRINT)
        put("buildFingerprint", sha256File(reactApplicationContext.applicationInfo.sourceDir))
        put("runNonce", nonce)
      }.toString().toByteArray(Charsets.UTF_8)
      Thread({
        var connection: HttpURLConnection? = null
        var phase = "setup"
        try {
          val request = url.openConnection() as HttpURLConnection
          connection = request
          request.requestMethod = "POST"
          request.connectTimeout = 5_000
          request.readTimeout = 65_000
          request.instanceFollowRedirects = false
          request.doOutput = true
          request.setRequestProperty("Content-Type", "application/json")
          request.setFixedLengthStreamingMode(identity.size)
          phase = "request"
          Log.e("JazzCoreObservation", "request-started")
          request.outputStream.use { it.write(identity) }
          Log.e("JazzCoreObservation", "request-sent")
          phase = "response"
          val status = request.responseCode
          // Log only a bounded status/category. Never log the exception, URL,
          // request identity or body; JS's generic stage retry uses another tag.
          Log.e("JazzCoreObservation", if (status in 100..599) "http-status-$status" else "http-status-invalid")
          check(status == 204) { "Core observation was not acknowledged" }
          phase = "promise"
          promise.resolve(null)
          Log.e("JazzCoreObservation", "promise-resolved")
        } catch (error: Throwable) {
          val category = when (error) {
            is java.net.SocketTimeoutException -> "timeout"
            is java.net.ConnectException -> "connection"
            is java.net.UnknownHostException -> "dns"
            is javax.net.ssl.SSLException -> "tls"
            is java.net.ProtocolException -> "protocol"
            is java.io.IOException -> "io"
            is IllegalStateException -> "state"
            else -> "other"
          }
          Log.e("JazzCoreObservation", "failure-$phase-$category")
          promise.reject("E_JAZZ_DEVICE_CORE", "Core observation was not acknowledged")
        } finally { connection?.disconnect() }
      }, "JazzCoreObservation").start()
    } catch (_: Throwable) {
      promise.reject("E_JAZZ_DEVICE_CORE", "Missing Core observation launch metadata")
    }
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
