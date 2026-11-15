// StagePlan, as a Kotlin/JVM CLI over jazz-kotlin. `jazz-tools generate`
// sees this file beside jazz.app.json and writes src/main/kotlin/JazzApp.kt.

plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
    application
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

application { mainClass.set("stageplan.MainKt") }

val repo = rootDir.resolve("../../../..").canonicalFile
val ffiLibDir = repo.resolve("target/debug")

tasks.test {
    useJUnitPlatform()
    systemProperty("jna.library.path", ffiLibDir.path)
    // The tests' StagePlan backend: a test harness, not an example backend
    // (cargo build -p jazz-app --example test_app_backend).
    environment(
        "JAZZ_TEST_APP_BACKEND",
        System.getenv("JAZZ_TEST_APP_BACKEND") ?: repo.resolve("target/debug/examples/test_app_backend").path,
    )
    testLogging { events("passed", "failed"); showStandardStreams = true }
}

tasks.named<JavaExec>("run") { systemProperty("jna.library.path", ffiLibDir.path) }
