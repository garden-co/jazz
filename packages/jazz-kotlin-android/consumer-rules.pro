# Applied to apps that depend on jazz-kotlin-android when they minify with R8.
# Jazz.open reaches tools.jazz.UniffiNative by reflection.
-keep class tools.jazz.UniffiNative { *; }
# The UniFFI bindings talk to libjazz_ffi.so through JNA, which looks up
# structures, callbacks and library interfaces reflectively.
-keep class uniffi.jazz_ffi.** { *; }
-keep class com.sun.jna.** { *; }
-keep class * implements com.sun.jna.** { *; }
-dontwarn java.awt.**
