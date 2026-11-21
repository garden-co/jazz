// The docs todo app's Kotlin snippets, over jazz-kotlin. `jazz-tools generate`
// sees this file beside jazz.app.json and writes src/main/kotlin/JazzApp.kt.

plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
}

repositories { mavenCentral() }

dependencies {
    implementation("tools.jazz:jazz-kotlin")
    testImplementation(kotlin("test"))
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
}

val repo = rootDir.resolve("../../..").canonicalFile
val ffiLibDir = repo.resolve("target/debug")

tasks.test {
    useJUnitPlatform()
    systemProperty("jna.library.path", ffiLibDir.path)
    // The tests' backend, a test harness: a Jazz server and the test stand-in
    // for a backend's Jazz endpoint (cargo build -p jazz-tools --bin jazz-tools;
    // cargo build -p jazz-app-support --features test-relay --bin jazz-app-test-relay).
    environment("JAZZ_TOOLS", System.getenv("JAZZ_TOOLS") ?: repo.resolve("target/debug/jazz-tools").path)
    environment(
        "JAZZ_APP_TEST_RELAY",
        System.getenv("JAZZ_APP_TEST_RELAY") ?: repo.resolve("target/debug/jazz-app-test-relay").path,
    )
    testLogging { events("passed", "failed"); showStandardStreams = true }
}
