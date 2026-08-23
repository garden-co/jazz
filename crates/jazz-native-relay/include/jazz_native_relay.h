#ifndef JAZZ_NATIVE_RELAY_H
#define JAZZ_NATIVE_RELAY_H

#include <stdint.h>

/*
 * Return the shared native relay ABI version embedded in this artifact.
 *
 * JNI and other platform wrappers must compare this before decoding or sending
 * a command. This header intentionally exposes no database/query handles.
 */
uint16_t jazz_native_relay_abi_version(void);

#endif
