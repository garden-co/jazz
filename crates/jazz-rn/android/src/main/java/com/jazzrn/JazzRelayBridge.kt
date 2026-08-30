package com.jazzrn

import android.util.Base64
import com.facebook.react.turbomodule.core.interfaces.BindingsInstallerHolder
import org.json.JSONObject

/** Opaque JNI carrier for the shared C ABI; it owns no Jazz semantics. */
internal object JazzRelayBridge {
  private var host: Long = 0
  private var nextRuntimeToken = 1L
  private val activeRuntimeTokens = mutableSetOf<Long>()
  private val trustedCapabilities = mutableSetOf<String>()

  private fun ensureHost(): Long {
    if (host != 0L) return host
    System.loadLibrary("jazzrelay")
    host = nativeCreate()
    check(host != 0L) { "Jazz native relay failed to create its host" }
    return host
  }

  @Synchronized
  fun acquireRuntime(): Long {
    ensureHost()
    check(nextRuntimeToken > 0) { "Jazz native relay runtime token space exhausted" }
    val token = nextRuntimeToken
    nextRuntimeToken += 1
    check(activeRuntimeTokens.add(token)) { "Jazz native relay duplicated a runtime token" }
    return token
  }

  @Synchronized
  fun releaseRuntime(runtimeToken: Long) {
    check(activeRuntimeTokens.remove(runtimeToken)) {
      "Jazz native relay runtime lease is already released"
    }
    nativeInvalidateForegroundRuntime(host, runtimeToken)
    destroyIfUnused()
  }

  @Synchronized
  fun abiVersion(): Double {
    ensureHost()
    return nativeAbiVersion().toDouble()
  }

  @Synchronized
  fun execute(commandBase64: String): String = Base64.encodeToString(
    nativeExecute(ensureHost(), Base64.decode(commandBase64, Base64.NO_WRAP)),
    Base64.NO_WRAP
  )

  /**
   * React Native invokes this installer while it owns a live JSI runtime. The
   * shared C++ factory closes over only this runtime token's host liveness
   * lease; it never receives trusted scope configuration from JavaScript.
   */
  @Synchronized
  fun foregroundBindingsInstaller(runtimeToken: Long): BindingsInstallerHolder {
    check(activeRuntimeTokens.contains(runtimeToken)) {
      "Jazz native foreground runtime is unavailable for this bridge"
    }
    return nativeForegroundBindingsInstaller(ensureHost(), runtimeToken)
  }

  /**
   * The only Android entry for trusted scope configuration. It is deliberately
   * not part of the TurboModule: JS gets the returned random capability but
   * cannot supply path/schema/identity/claims to the generic command channel.
   */
  @Synchronized
  fun admitTrustedScope(config: TrustedRelayScopeConfig): ByteArray {
    val payload = JSONObject().apply {
      put("scope", JSONObject().apply {
        put("app_namespace", config.appNamespace)
        put("storage_namespace", config.storageNamespace)
        put("auth_scope", config.authScope ?: JSONObject.NULL)
      })
      put("sqlite_path", config.sqlitePath)
      put("schema_json", config.schemaJson)
      put("identity", JSONObject(config.identityJson))
      put("claims", JSONObject(config.claimsJson))
    }.toString().encodeToByteArray()
    return nativeAdmitTrustedScopeJson(ensureHost(), payload).also {
      check(it.size == 32) { "Jazz native relay returned an invalid admission capability" }
      trustedCapabilities += Base64.encodeToString(it, Base64.NO_WRAP)
    }
  }

  @Synchronized
  fun revokeTrustedScope(capability: ByteArray) {
    check(capability.size == 32) { "Jazz admission capabilities are exactly 32 bytes" }
    nativeRevokeTrustedScope(ensureHost(), capability)
    trustedCapabilities -= Base64.encodeToString(capability, Base64.NO_WRAP)
    destroyIfUnused()
  }

  private fun destroyIfUnused() {
    if (host != 0L && activeRuntimeTokens.isEmpty() && trustedCapabilities.isEmpty()) {
      nativeDestroy(host)
      host = 0
    }
  }

  @JvmStatic private external fun nativeCreate(): Long
  @JvmStatic private external fun nativeDestroy(host: Long)
  @JvmStatic private external fun nativeAbiVersion(): Int
  @JvmStatic private external fun nativeExecute(host: Long, command: ByteArray): ByteArray
  @JvmStatic private external fun nativeAdmitTrustedScopeJson(host: Long, admissionJson: ByteArray): ByteArray
  @JvmStatic private external fun nativeRevokeTrustedScope(host: Long, capability: ByteArray)
  @JvmStatic private external fun nativeForegroundBindingsInstaller(
    host: Long,
    runtimeToken: Long,
  ): BindingsInstallerHolder
  @JvmStatic private external fun nativeInvalidateForegroundRuntime(host: Long, runtimeToken: Long)
}

/**
 * Complete, validated input owned by Android application/authentication code.
 * The JSON identity and claims use the Rust relay's typed transport shape and
 * are parsed and validated in Rust; bearer-token claim names are rejected.
 * This type is intentionally Kotlin-only and never generated into JS.
 */
data class TrustedRelayScopeConfig(
  val appNamespace: String,
  val storageNamespace: String,
  val authScope: String?,
  val sqlitePath: String,
  val schemaJson: String,
  val identityJson: String,
  val claimsJson: String = "{}",
)

/**
 * Android auth lifecycle seam. Providers derive the complete configuration
 * after validation, retain only the opaque return value for JS, and revoke the
 * old value before admitting a changed authenticated scope.
 */
object JazzRelayTrustedAdmission {
  fun admit(config: TrustedRelayScopeConfig): ByteArray = JazzRelayBridge.admitTrustedScope(config)

  fun replace(previous: ByteArray?, next: TrustedRelayScopeConfig): ByteArray {
    previous?.let(JazzRelayBridge::revokeTrustedScope)
    return JazzRelayBridge.admitTrustedScope(next)
  }

  fun revoke(capability: ByteArray) = JazzRelayBridge.revokeTrustedScope(capability)
}
