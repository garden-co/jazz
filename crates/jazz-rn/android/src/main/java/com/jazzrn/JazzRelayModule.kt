package com.jazzrn

import com.facebook.fbreact.specs.NativeJazzRelaySpec
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.module.annotations.ReactModule

/**
 * Android implementation of the generated JazzRelay TurboModule spec.
 *
 * This intentionally owns no database, query, or protocol logic. The shared
 * Rust native-relay crate remains the future command executor; until its
 * Android artifact is embedded, command execution fails explicitly instead of
 * falling back to the obsolete UniFFI runtime.
 */
@ReactModule(name = JazzRelayModule.NAME)
class JazzRelayModule(reactContext: ReactApplicationContext) : NativeJazzRelaySpec(reactContext) {
  override fun getAbiVersion(): Double = 1.0

  override fun execute(encodedCommand: String, promise: Promise) {
    promise.reject(
      "E_JAZZ_RELAY_UNAVAILABLE",
      "Jazz native relay commands require an Android development or release build containing the shared Rust relay artifact.",
    )
  }

  companion object {
    const val NAME = "JazzRelay"
  }
}
