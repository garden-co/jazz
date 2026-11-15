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

// The backend opens with the app's permissions: ship them as a resource.
sourceSets.main { resources.srcDir(layout.buildDirectory.dir("app-resources")) }
val appResources by tasks.registering(Copy::class) {
    from("jazz.permissions.json")
    into(layout.buildDirectory.dir("app-resources"))
}
tasks.processResources { dependsOn(appResources) }

application { mainClass.set("stageplan.MainKt") }

val repo = rootDir.resolve("../../../..").canonicalFile
val ffiLibDir = repo.resolve("target/debug")

tasks.test {
    useJUnitPlatform()
    systemProperty("jna.library.path", ffiLibDir.path)
    // The tests start `jazz-tools server` (cargo build -p jazz-cli --bin jazz-tools).
    environment("JAZZ_TOOLS", System.getenv("JAZZ_TOOLS") ?: repo.resolve("target/debug/jazz-tools").path)
    // An app endpoint for end users (cargo build -p jazz-ffi --example end_user_endpoint).
    environment(
        "JAZZ_END_USER_ENDPOINT",
        System.getenv("JAZZ_END_USER_ENDPOINT") ?: repo.resolve("target/debug/examples/end_user_endpoint").path,
    )
    testLogging { events("passed", "failed"); showStandardStreams = true }
}

tasks.named<JavaExec>("run") { systemProperty("jna.library.path", ffiLibDir.path) }
