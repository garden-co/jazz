# Consumer ProGuard/R8 rules for jazz-rn, named by consumerProguardFiles in
# build.gradle. Gradle fails :jazz-rn:merge*ConsumerProguardFiles when the
# named file is missing, so it must ship even while it holds no rules.
#
# No keep rules are needed today: every JNI entry point is a `native` method on
# com.jazzrn.JazzRelayBridge, and the default Android rules keep classes with
# native methods. The C++ side registers no Java callbacks by name.
