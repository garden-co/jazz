---
"jazz-tools": patch
---

The device account secret now crosses into the native (napi), wasm and React Native relay builds in its stored text form, `jazz-auth-v1:` followed by 43 unpadded base64url characters, instead of as 64 hexadecimal characters. This covers the encryption runtime's constructors, relay account admission and the automatic recovery with the account secret. The stored form is unchanged: secrets are kept as `jazz-auth-v1:` text, and the hexadecimal form was only ever passed between the JavaScript package and its build, never stored, so there is nothing to migrate. Recovery with a 24-word phrase or a recovery key (`automatic: false`) still passes 64 hexadecimal characters.

The JavaScript package and its native or wasm build must come from the same Jazz version. When they don't, the napi and wasm builds now say so: given 64 hexadecimal characters, the runtime's constructor names the `jazz-auth-v1:` format it expects and the likely version mismatch, and the JavaScript package explains an older build's refusal of the text form as a version mismatch too, from the runtime's readiness and from the automatic recovery (`EncryptionError` code `secret-format-mismatch`). On React Native, relay account admission refuses a secret in the other format, in either direction, with the generic `invalid_command` status, as it does any request it cannot read.
