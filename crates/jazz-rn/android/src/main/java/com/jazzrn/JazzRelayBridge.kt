package com.jazzrn

import android.util.Base64

/** Opaque JNI carrier for the shared C ABI; it owns no Jazz semantics. */
internal object JazzRelayBridge {
  private val host: Long

  init {
    System.loadLibrary("jazzrelay")
    host = nativeCreate()
    check(host != 0L) { "Jazz native relay failed to create its host" }
  }

  fun abiVersion(): Double = nativeAbiVersion().toDouble()

  fun execute(commandBase64: String): String =
    Base64.encodeToString(nativeExecute(host, Base64.decode(commandBase64, Base64.NO_WRAP)), Base64.NO_WRAP)

  @JvmStatic private external fun nativeCreate(): Long
  @JvmStatic private external fun nativeDestroy(host: Long)
  @JvmStatic private external fun nativeAbiVersion(): Int
  @JvmStatic private external fun nativeExecute(host: Long, command: ByteArray): ByteArray
}
