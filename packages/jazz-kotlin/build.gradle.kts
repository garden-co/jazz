// jazz-kotlin: the Kotlin/JVM runtime for Jazz apps, over jazz-ffi.
//
// `gradle build` (Gradle 8.14; no wrapper is committed) first builds crates/jazz-ffi with
// cargo and generates its Kotlin bindings with uniffi-bindgen; pass
// -PjazzFfi=skip to build without them (tests then use a fake native).

plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
    `java-library`
}

group = "tools.jazz"
version = "2.0.0-alpha.65"

repositories { mavenCentral() }

dependencies {
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("net.java.dev.jna:jna:5.17.0")
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    // The sources uniffi-bindgen generates declare no visibility.
    explicitApiWarning()
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
}

val repo = rootDir.resolve("../..").canonicalFile
val withFfi = (findProperty("jazzFfi") as String?) != "skip"
val cargoProfile = (findProperty("cargoProfile") as String?) ?: "debug"
val ffiLib = repo.resolve("target/$cargoProfile/libjazz_ffi.so")
val uniffiOut = layout.buildDirectory.dir("generated/uniffi")

val cargoBuildFfi by tasks.registering(Exec::class) {
    description = "Builds crates/jazz-ffi."
    workingDir = repo
    commandLine(listOf("cargo", "build", "-p", "jazz-ffi") + if (cargoProfile == "release") listOf("--release") else emptyList())
}

val generateUniffi by tasks.registering(Exec::class) {
    description = "Generates jazz-ffi's Kotlin bindings with uniffi-bindgen."
    dependsOn(cargoBuildFfi)
    workingDir = repo
    inputs.file(ffiLib)
    outputs.dir(uniffiOut)
    commandLine(
        "cargo", "run", "-q", "-p", "jazz-ffi", "--features", "bindgen", "--bin", "uniffi-bindgen", "--",
        "generate", "--library", ffiLib.path, "--language", "kotlin", "--no-format",
        "--out-dir", uniffiOut.get().asFile.path,
    )
}

if (withFfi) {
    sourceSets.main {
        kotlin.srcDir("src/ffi/kotlin")
        kotlin.srcDir(uniffiOut)
    }
    tasks.named("compileKotlin") { dependsOn(generateUniffi) }
    // JNA finds the library on jna.library.path; also bundle it for this platform.
    val bundleFfi by tasks.registering(Copy::class) {
        dependsOn(cargoBuildFfi)
        from(ffiLib)
        into(layout.buildDirectory.dir("ffi-resources/linux-x86-64"))
    }
    sourceSets.main { resources.srcDir(layout.buildDirectory.dir("ffi-resources")) }
    tasks.named("processResources") { dependsOn(bundleFfi) }
}

// jazz-codegen's Kotlin goldens must compile against this runtime: chat-react
// (the widest types) with the tests, kotlin-edges (names that could hide
// packages or members) on its own, as both declare package jazzapp.
sourceSets.test { kotlin.srcDir(repo.resolve("crates/jazz-codegen/tests/apps/chat-react")) }
val edges = sourceSets.create("edges") {
    kotlin.srcDir(repo.resolve("crates/jazz-codegen/tests/apps/kotlin-edges"))
    compileClasspath += sourceSets.main.get().output + sourceSets.main.get().compileClasspath
}
tasks.named("check") { dependsOn("compileEdgesKotlin") }
tasks.named("test") { dependsOn("compileEdgesKotlin") }

tasks.test {
    useJUnitPlatform()
    systemProperty("jna.library.path", ffiLib.parentFile.path)
}
