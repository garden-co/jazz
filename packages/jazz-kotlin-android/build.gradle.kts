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
// ABIs. -PprebuiltJniLibs=<dir> packages <dir>/<abi>/libjazz_ffi.so, built
// elsewhere by this same cargo-ndk command (attach-language-artifacts.yml
// builds one ABI per job), instead of running cargo-ndk; -PuniffiBindings=<dir>
// takes the Kotlin bindings generateUniffi made from such a build's
// arm64-v8a library. With both, the build needs no Rust toolchain.

plugins {
    id("com.android.library") version "8.13.0"
    kotlin("android") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
    // 0.35.0: the last release for Gradle 8 (0.36 needs Gradle 9).
    id("com.vanniktech.maven.publish") version "0.35.0"
}

val repo = rootDir.resolve("../..").canonicalFile

group = "tools.jazz"
// Lockstep with every Jazz package: jazz-tools' version.
version = (groovy.json.JsonSlurper().parse(repo.resolve("packages/jazz-tools/package.json")) as Map<*, *>)["version"] as String

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

val jvmLibrary = repo.resolve("packages/jazz-kotlin")
val cargoProfile = (findProperty("cargoProfile") as String?) ?: "release-mobile"
val abis = ((findProperty("androidAbis") as String?) ?: "arm64-v8a,armeabi-v7a,x86_64").split(",").map { it.trim() }
val jniLibsOut = layout.buildDirectory.dir("jniLibs")
val uniffiOut = layout.buildDirectory.dir("generated/uniffi")

val prebuiltJniLibs = (findProperty("prebuiltJniLibs") as String?)?.let { file(it) }
val uniffiBindings = (findProperty("uniffiBindings") as String?)?.let { file(it) }
if (uniffiBindings != null) {
    check(uniffiBindings.resolve("uniffi/jazz_ffi/jazz_ffi.kt").isFile) { "-PuniffiBindings=$uniffiBindings lacks uniffi/jazz_ffi/jazz_ffi.kt" }
}

val cargoNdkFfi by tasks.registering(Exec::class) {
    description = "Cross-compiles crates/jazz-ffi for the Android ABIs with cargo-ndk."
    // Skipped (and replaced by copyPrebuiltJniLibs) with -PprebuiltJniLibs.
    enabled = prebuiltJniLibs == null
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

// -PprebuiltJniLibs: exactly <abi>/libjazz_ffi.so for each ABI, into the
// same directory cargo-ndk writes, so the AAR packs the same layout.
val copyPrebuiltJniLibs by tasks.registering(Sync::class) {
    description = "Copies the prebuilt jazz-ffi libraries (-PprebuiltJniLibs) into jniLibs."
    enabled = prebuiltJniLibs != null
    if (prebuiltJniLibs != null) {
        val missing = abis.filterNot { prebuiltJniLibs.resolve("$it/libjazz_ffi.so").isFile }
        check(missing.isEmpty()) { "-PprebuiltJniLibs=$prebuiltJniLibs lacks libjazz_ffi.so for $missing" }
        from(prebuiltJniLibs) { abis.forEach { include("$it/libjazz_ffi.so") } }
    }
    into(jniLibsOut)
}

// The bindings come from the very library the AAR ships (the first ABI's),
// so they always match its UniFFI scaffolding.
val generateUniffi by tasks.registering(Exec::class) {
    description = "Generates jazz-ffi's Kotlin bindings with uniffi-bindgen."
    dependsOn(cargoNdkFfi, copyPrebuiltJniLibs)
    enabled = uniffiBindings == null
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
        uniffiBindings ?: uniffiOut,
    )
    jniLibs.srcDir(jniLibsOut)
}
tasks.named("preBuild") { dependsOn(generateUniffi) }
// The sources jar (Maven Central) packs the generated bindings too.
tasks.withType<Jar>().configureEach { dependsOn(generateUniffi) }

// Maven Central (dev/scripts/publish-kotlin.sh): the release AAR, sources, an
// empty javadoc jar, signed; see packages/jazz-kotlin for the properties.
// The javadoc jar is ours, empty: AGP's own (publishJavadocJar) runs its bundled
// Dokka 1.4.32, which downloads the JDK's, Kotlin's, Android's and AndroidX's
// package-lists while rendering and fails the build when a download breaks
// off mid-response.
mavenPublishing {
    publishToMavenCentral()
    signAllPublications()
    configure(
        com.vanniktech.maven.publish.AndroidSingleVariantLibrary(
            variant = "release",
            sourcesJar = true,
            publishJavadocJar = false,
        ),
    )
    coordinates("tools.jazz", "jazz-kotlin-android", version.toString())
    pom {
        name.set("jazz-kotlin-android")
        description.set("Jazz for Kotlin on Android: an end-user client of a Jazz app whose backend runs in TypeScript (experimental).")
        url.set("https://jazz.tools")
        inceptionYear.set("2026")
        licenses {
            license {
                name.set("MIT License")
                url.set("https://github.com/garden-co/jazz/blob/main/LICENSE")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("garden-co")
                name.set("Garden Computing, Inc.")
                url.set("https://github.com/garden-co")
            }
        }
        scm {
            url.set("https://github.com/garden-co/jazz")
            connection.set("scm:git:https://github.com/garden-co/jazz.git")
            developerConnection.set("scm:git:ssh://git@github.com/garden-co/jazz.git")
        }
    }
}

val emptyJavadocJar by tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
}
// After the plugin's afterEvaluate, which creates the publication.
afterEvaluate {
    extensions.getByType<PublishingExtension>().publications
        .named<MavenPublication>("maven") { artifact(emptyJavadocJar) }
}
