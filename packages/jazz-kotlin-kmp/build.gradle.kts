// jazz-kotlin-kmp: a SPIKE of jazz-kotlin as a Kotlin Multiplatform library.
//
// commonMain is jazz-kotlin's API (Jazz, Query, Edits, Auth, …) with a small
// expect/actual seam (Platform.kt). The JVM reaches jazz-ffi as jazz-kotlin
// does, through UniFFI's Kotlin bindings over JNA; Kotlin/Native (linuxX64,
// iOS) through cinterop on jazz-ffi's C ABI (crates/jazz-ffi/include/jazz_ffi.h),
// with libjazz_ffi.a linked into the klib.
//
// `gradle jvmTest linuxX64Test` (Gradle 8.14, as dev/scripts/kotlin-toolchain.sh pins).

import org.jetbrains.kotlin.gradle.plugin.mpp.KotlinNativeTarget
import org.jetbrains.kotlin.konan.target.KonanTarget

plugins {
    kotlin("multiplatform") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
    // Spike only: `gradle publishAllPublicationsToSpikeRepository` lays out what
    // Maven Central would get (per-target artifacts and Gradle module metadata) in build/repo.
    `maven-publish`
}

val repo = rootDir.resolve("../..").canonicalFile
group = "tools.jazz"
version = (groovy.json.JsonSlurper().parse(repo.resolve("packages/jazz-tools/package.json")) as Map<*, *>)["version"] as String

repositories { mavenCentral() }

val cargoProfile = (findProperty("cargoProfile") as String?) ?: "dev"
val profileDir = if (cargoProfile == "dev") "debug" else cargoProfile
val hostLib = repo.resolve("target/$profileDir/libjazz_ffi.so")
val uniffiOut = layout.buildDirectory.dir("generated/uniffi")

/** Where cargo puts libjazz_ffi.a for a Kotlin/Native target (built with `cargo build --target`). */
fun rustTarget(target: KonanTarget): String? = when (target) {
    KonanTarget.LINUX_X64 -> null // the host's own target/<profile>
    KonanTarget.LINUX_ARM64 -> "aarch64-unknown-linux-gnu"
    KonanTarget.IOS_ARM64 -> "aarch64-apple-ios"
    KonanTarget.IOS_SIMULATOR_ARM64 -> "aarch64-apple-ios-sim"
    KonanTarget.IOS_X64 -> "x86_64-apple-ios"
    KonanTarget.MACOS_ARM64 -> "aarch64-apple-darwin"
    KonanTarget.MACOS_X64 -> "x86_64-apple-darwin"
    KonanTarget.MINGW_X64 -> "x86_64-pc-windows-gnu"
    else -> error("no Rust target for $target")
}

val shimDir = layout.buildDirectory.dir("glibc-shim")
val buildGlibcShim by tasks.registering(Exec::class) {
    description = "Builds the fcntl64 shim linuxX64 links with (src/nativeInterop/glibc-shim.c)."
    val src = file("src/nativeInterop/glibc-shim.c")
    inputs.file(src)
    outputs.dir(shimDir)
    doFirst { shimDir.get().asFile.mkdirs() }
    val out = shimDir.get().asFile
    commandLine("sh", "-c", "cc -O2 -fPIC -c '${src.path}' -o '$out/glibc-shim.o' && ar rcs '$out/libjazz_glibc_shim.a' '$out/glibc-shim.o'")
}

kotlin {
    explicitApiWarning()
    jvm { compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) } }
    linuxX64()
    iosSimulatorArm64()
    iosArm64()

    applyDefaultHierarchyTemplate()

    targets.withType<KotlinNativeTarget>().configureEach {
        val rust = rustTarget(konanTarget)
        val libDir = if (rust == null) repo.resolve("target/$profileDir") else repo.resolve("target/$rust/$profileDir")
        compilations.getByName("main").cinterops.create("jazzFfi") {
            definitionFile.set(project.file("src/nativeInterop/cinterop/jazz_ffi.def"))
            includeDirs(repo.resolve("crates/jazz-ffi/include"))
            // The static library goes into the klib: an app linking this
            // klib links jazz-ffi too, with no separate native artifact.
            // (Only when it's there: a klib without it still compiles, for
            // the iOS compile check on Linux.)
            if (libDir.resolve("libjazz_ffi.a").exists()) {
                extraOpts("-staticLibrary", "libjazz_ffi.a", "-libraryPath", libDir.path)
            }
            if (konanTarget == KonanTarget.LINUX_X64) {
                // glibc-shim.c: fcntl64 for Kotlin/Native's glibc 2.19 sysroot.
                extraOpts("-staticLibrary", "libjazz_glibc_shim.a", "-libraryPath", shimDir.get().asFile.path)
                tasks.named(interopProcessingTaskName) { dependsOn(buildGlibcShim) }
            }
        }
        if (konanTarget.family.isAppleFamily) {
            binaries.all {
                linkerOpts("-framework", "CoreFoundation", "-framework", "Security", "-framework", "SystemConfiguration", "-liconv", "-lresolv")
            }
        }
    }

    sourceSets {
        commonMain.dependencies {
            api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
            api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
        }
        commonTest {
            // jazz-codegen's generated bindings, as they are, in common code:
            // `notes` with LocalAppTest against the real jazz-ffi, or another
            // golden (-PgoldenApp=chat-react, tasks, kotlin-edges, …) to check it compiles.
            val golden = (findProperty("goldenApp") as String?) ?: "notes"
            kotlin.srcDir(repo.resolve("crates/jazz-codegen/tests/apps/$golden"))
            if (golden == "notes") kotlin.srcDir("src/notesTest/kotlin")
            dependencies {
                implementation(kotlin("test"))
                implementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
            }
        }
        jvmMain {
            kotlin.srcDir(repo.resolve("packages/jazz-kotlin/src/ffi/kotlin"))
            kotlin.srcDir(uniffiOut)
            dependencies { implementation("net.java.dev.jna:jna:5.17.0") }
        }
    }
}

val cargoBuildFfi by tasks.registering(Exec::class) {
    description = "Builds crates/jazz-ffi for the host."
    workingDir = repo
    // The staticlib on its own: with lib and cdylib in one build, cargo skips LTO for it
    // (188 MB instead of 38 MB for release-mobile on linux-x86_64).
    commandLine("sh", "-c", "cargo build -p jazz-ffi --profile $cargoProfile && cargo rustc -p jazz-ffi --profile $cargoProfile --lib --crate-type staticlib")
}

val generateUniffi by tasks.registering(Exec::class) {
    description = "Generates jazz-ffi's Kotlin bindings with uniffi-bindgen (the JVM's)."
    dependsOn(cargoBuildFfi)
    workingDir = repo
    inputs.file(hostLib)
    outputs.dir(uniffiOut)
    commandLine(
        "cargo", "run", "-q", "-p", "jazz-ffi", "--features", "bindgen", "--bin", "uniffi-bindgen", "--",
        "generate", "--library", hostLib.path, "--language", "kotlin", "--no-format",
        "--out-dir", uniffiOut.get().asFile.path,
    )
}
tasks.named("compileKotlinJvm") { dependsOn(generateUniffi) }
tasks.named("cinteropJazzFfiLinuxX64") { dependsOn(cargoBuildFfi) }

tasks.named<Test>("jvmTest") {
    systemProperty("jna.library.path", hostLib.parentFile.path)
}

publishing {
    repositories { maven { name = "spike"; url = uri(layout.buildDirectory.dir("repo")) } }
}
