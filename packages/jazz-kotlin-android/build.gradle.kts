// jazz-kotlin-android: jazz-kotlin as an Android library (AAR).
//
// The same Kotlin sources as packages/jazz-kotlin (the library and its UniFFI
// adapter), compiled for Android, with crates/jazz-ffi cross-compiled by
// cargo-ndk into jniLibs for arm64-v8a, armeabi-v7a and x86_64, and JNA's
// Android AAR. `gradle assembleRelease` (Gradle 8.14; no wrapper is committed)
// writes build/outputs/aar/jazz-kotlin-android-release.aar. It needs
// ANDROID_HOME (platforms;android-36, build-tools;36.0.0, ndk;27.1.12297006)
// and cargo-ndk on PATH: dev/scripts/test-kotlin-android.sh sets both up.
//
// -PcargoProfile=<profile> picks the Cargo profile (default release-mobile,
// the size-oriented one jazz-rn ships); -PandroidAbis=arm64-v8a,… narrows the
// ABIs.

plugins {
    id("com.android.library") version "8.13.0"
    kotlin("android") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
}

group = "tools.jazz"
version = "2.0.0-alpha.65"

val ndkVersionPinned = "27.1.12297006"
val minSdkVersion = 24

android {
    namespace = "tools.jazz"
    compileSdk = 36
    buildToolsVersion = "36.0.0"
    ndkVersion = ndkVersionPinned

    defaultConfig {
        minSdk = minSdkVersion
        consumerProguardFiles("consumer-rules.pro")
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

kotlin {
    // The sources uniffi-bindgen generates declare no visibility.
    explicitApiWarning()
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
}

dependencies {
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    // JNA's Android build: the same classes plus libjnidispatch.so per ABI.
    implementation("net.java.dev.jna:jna:5.17.0@aar")
}

val repo = rootDir.resolve("../..").canonicalFile
val jvmLibrary = repo.resolve("packages/jazz-kotlin")
val cargoProfile = (findProperty("cargoProfile") as String?) ?: "release-mobile"
val abis = ((findProperty("androidAbis") as String?) ?: "arm64-v8a,armeabi-v7a,x86_64").split(",").map { it.trim() }
val jniLibsOut = layout.buildDirectory.dir("jniLibs")
val uniffiOut = layout.buildDirectory.dir("generated/uniffi")

val cargoNdkFfi by tasks.registering(Exec::class) {
    description = "Cross-compiles crates/jazz-ffi for the Android ABIs with cargo-ndk."
    workingDir = repo
    outputs.dir(jniLibsOut)
    outputs.upToDateWhen { false } // cargo decides what to rebuild
    // cargo-ndk only adds files: drop ABIs (or profiles) a previous run left.
    doFirst { delete(jniLibsOut) }
    commandLine(
        listOf("cargo", "ndk", "--platform", minSdkVersion.toString()) +
            abis.flatMap { listOf("-t", it) } +
            listOf("-o", jniLibsOut.get().asFile.path, "build", "-p", "jazz-ffi", "--profile", cargoProfile),
    )
}

// The bindings come from the very library the AAR ships (the first ABI's),
// so they always match its UniFFI scaffolding.
val generateUniffi by tasks.registering(Exec::class) {
    description = "Generates jazz-ffi's Kotlin bindings with uniffi-bindgen."
    dependsOn(cargoNdkFfi)
    workingDir = repo
    val lib = jniLibsOut.get().file("${abis.first()}/libjazz_ffi.so").asFile
    inputs.file(lib)
    outputs.dir(uniffiOut)
    commandLine(
        "cargo", "run", "-q", "-p", "jazz-ffi", "--features", "bindgen", "--bin", "uniffi-bindgen", "--",
        "generate", "--library", lib.path, "--language", "kotlin", "--no-format",
        "--out-dir", uniffiOut.get().asFile.path,
    )
}

android.sourceSets.getByName("main") {
    kotlin.srcDirs(
        jvmLibrary.resolve("src/main/kotlin"),
        jvmLibrary.resolve("src/ffi/kotlin"),
        uniffiOut,
    )
    jniLibs.srcDir(jniLibsOut)
}
tasks.named("preBuild") { dependsOn(generateUniffi) }
