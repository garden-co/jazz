package com.jazzrn

/**
 * Thrown by the JNI bridge when the relay's local store was written in a
 * storage format this build does not read (native status
 * `JAZZ_NATIVE_RELAY_UNSUPPORTED_STORAGE_CODECS`). The module rejects with the
 * stable code `unsupported_storage_codecs` so the app can delete the store and
 * resync from Core.
 */
class JazzRelayUnsupportedStorageCodecsException(message: String) : IllegalStateException(message)
