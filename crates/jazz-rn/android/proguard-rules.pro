# Consumer ProGuard/R8 rules for jazz-rn, named by consumerProguardFiles in
# build.gradle. Gradle fails :jazz-rn:merge*ConsumerProguardFiles when the
# named file is missing, so it must ship even while it holds no rules.
#
# Every JNI entry point is a `native` method on com.jazzrn.JazzRelayBridge, and
# the default Android rules keep classes with native methods. The C++ side
# registers no Java callbacks by name; it only throws this exception class by
# name, so R8 must keep its name and constructor.
-keep class com.jazzrn.JazzRelayUnsupportedStorageCodecsException { <init>(java.lang.String); }
