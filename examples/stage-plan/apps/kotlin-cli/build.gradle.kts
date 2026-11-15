// StagePlan, as a Kotlin/JVM CLI over jazz-kotlin. src/main/kotlin/JazzApp.kt is what
// jazz-codegen's Kotlin back end writes for jazz.app.json. While Kotlin is switched off in the
// CLI, regenerate it with
//   JAZZ_UPDATE_FIXTURES=1 cargo test -p jazz-codegen --test golden the_stage_plan_kotlin_cli
// (once it's on, `jazz-tools generate` here, which this file's jazz-kotlin dependency triggers).

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
    testLogging { events("passed", "failed"); showStandardStreams = true }
}

tasks.named<JavaExec>("run") { systemProperty("jna.library.path", ffiLibDir.path) }
