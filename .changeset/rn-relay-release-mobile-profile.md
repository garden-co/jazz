---
"jazz-rn": patch
---

The React Native native relay is now built with a size-oriented profile (`opt-level = "s"`, fat LTO, one codegen unit, staticlib only). On iOS arm64 the relay adds about 9.9 MB to an app instead of 16.7 MB (stripped), and the device `.a` shrinks from 69.6 MB to 21.1 MB. Panics still unwind as before. Thanks to @boorad.
