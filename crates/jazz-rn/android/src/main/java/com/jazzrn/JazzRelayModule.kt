package com.jazzrn

import com.facebook.fbreact.specs.NativeJazzRelaySpec
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.module.annotations.ReactModule

/**
 * Android implementation of the generated JazzRelay TurboModule spec.
 *
 * This owns no database, query, or protocol logic. When a release/development
 * package stages the shared relay artifact, it forwards opaque postcard bytes
 * to its host-owned C ABI. Source checkouts without artifacts remain usable
 * for autolinking/prebuild and report the explicit unavailable diagnostic.
 */
@ReactModule(name = JazzRelayModule.NAME)
class JazzRelayModule(reactContext: ReactApplicationContext) : NativeJazzRelaySpec(reactContext) {
  private val bridge = runCatching {
    JazzRelayBridge.acquireRuntime()
    JazzRelayBridge
  }.getOrNull()

  override fun getAbiVersion(): Double = bridge?.abiVersion() ?: 0.0

  override fun execute(encodedCommand: String, promise: Promise) {
    val bridge = bridge
    if (bridge == null) {
      promise.reject("E_JAZZ_RELAY_UNAVAILABLE", "Jazz native relay commands require an Android development or release build containing the shared Rust relay artifact.")
      return
    }
    runCatching { bridge.execute(encodedCommand) }
      .onSuccess(promise::resolve)
      .onFailure { promise.reject("E_JAZZ_RELAY_COMMAND", it) }
  }

  override fun invalidate() {
    // Runtime leases make `nativeDestroy` deterministic without destroying a
    // shared process host while another RN runtime still uses it. Trusted
    // admissions keep their host alive until explicit logout/revocation.
    bridge?.let { JazzRelayBridge.releaseRuntime() }
    super.invalidate()
  }

  companion object {
    const val NAME = "JazzRelay"
  }
}
