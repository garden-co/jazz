// jazz-kotlin: the Kotlin/JVM runtime for Jazz apps, over jazz-ffi.
//
// `gradle build` (Gradle 8.14; no wrapper is committed) first builds crates/jazz-ffi with
// cargo and generates its Kotlin bindings with uniffi-bindgen; pass
// -PjazzFfi=skip to build without them (tests then use a fake native).
// -PuniffiBindings=<dir> (with -PnativeLibs) takes bindings already generated
// by this same uniffi-bindgen command (attach-language-artifacts.yml's
// linux-x86_64 job) instead: no cargo at all.

plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
    `java-library`
    // 0.35.0: the last release for Gradle 8 (0.36 needs Gradle 9).
    id("com.vanniktech.maven.publish") version "0.35.0"
}

val repo = rootDir.resolve("../..").canonicalFile

group = "tools.jazz"
// Lockstep with every Jazz package: jazz-tools' version.
version = (groovy.json.JsonSlurper().parse(repo.resolve("packages/jazz-tools/package.json")) as Map<*, *>)["version"] as String

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

val withFfi = (findProperty("jazzFfi") as String?) != "skip"
val cargoProfile = (findProperty("cargoProfile") as String?) ?: "debug"
val ffiLib = repo.resolve(
    "target/$cargoProfile/" + when {
        System.getProperty("os.name").lowercase().startsWith("mac") -> "libjazz_ffi.dylib"
        System.getProperty("os.name").lowercase().startsWith("windows") -> "jazz_ffi.dll"
        else -> "libjazz_ffi.so"
    },
)
val uniffiOut = layout.buildDirectory.dir("generated/uniffi")
val uniffiBindings = (findProperty("uniffiBindings") as String?)?.let { file(it) }
if (uniffiBindings != null) {
    check(findProperty("nativeLibs") != null) { "-PuniffiBindings needs -PnativeLibs" }
    check(uniffiBindings.resolve("uniffi/jazz_ffi/jazz_ffi.kt").isFile) { "-PuniffiBindings=$uniffiBindings lacks uniffi/jazz_ffi/jazz_ffi.kt" }
}

val cargoBuildFfi by tasks.registering(Exec::class) {
    description = "Builds crates/jazz-ffi."
    enabled = uniffiBindings == null
    workingDir = repo
    commandLine(listOf("cargo", "build", "-p", "jazz-ffi") + if (cargoProfile == "release") listOf("--release") else emptyList())
}

val generateUniffi by tasks.registering(Exec::class) {
    description = "Generates jazz-ffi's Kotlin bindings with uniffi-bindgen."
    dependsOn(cargoBuildFfi)
    workingDir = repo
    enabled = uniffiBindings == null
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
        kotlin.srcDir(uniffiBindings ?: uniffiOut)
    }
    tasks.named("compileKotlin") { dependsOn(generateUniffi) }
    // The sources jar (Maven Central) packs the generated bindings too.
    tasks.withType<Jar>().configureEach { dependsOn(generateUniffi) }
    // JNA loads libjazz_ffi from the classpath, at <os>-<arch>/ (linux-x86-64,
    // linux-aarch64, darwin-aarch64, darwin-x86-64, win32-x86-64). A
    // distribution jar bundles every platform's from -PnativeLibs=<dir>, laid
    // out so (dev/scripts/build-kotlin-dist.sh); otherwise this platform's own.
    // Copied by processResources, never a resources srcDir: the sources jar
    // packs the source directories, and would pack the libraries with them.
    val nativeLibs = findProperty("nativeLibs") as String?
    tasks.named<ProcessResources>("processResources") {
        if (nativeLibs != null) {
            from(file(nativeLibs))
        } else {
            dependsOn(cargoBuildFfi)
            from(ffiLib) { into(jnaPrefix()) }
        }
    }
}

/** JNA's resource prefix for the platform this build runs on. */
fun jnaPrefix(): String {
    val os = System.getProperty("os.name").lowercase()
    val arch = when (val a = System.getProperty("os.arch").lowercase()) {
        "amd64", "x86_64" -> "x86-64"
        "arm64", "aarch64" -> "aarch64"
        else -> a
    }
    return when {
        os.startsWith("mac") -> "darwin-$arch"
        os.startsWith("windows") -> "win32-$arch"
        else -> "linux-$arch"
    }
}

// jazz-codegen's Kotlin goldens must compile against this runtime: chat-react
// (the widest types) with the tests, kotlin-edges (names that could hide
// packages or members) on its own, as both declare package jazzapp.
sourceSets.test { kotlin.srcDir(repo.resolve("crates/jazz-codegen/tests/apps/chat-react")) }
val edges = sourceSets.create("edges") {
    kotlin.srcDir(repo.resolve("crates/jazz-codegen/tests/apps/kotlin-edges"))
    compileClasspath += sourceSets.main.get().output + sourceSets.main.get().compileClasspath
}
// Goldens with tests of their own (each declares package jazzapp, so each
// gets a source set): kotlin-computed (transforms, computed fields).
fun goldenTest(name: String, app: String): TaskProvider<Test> {
    val set = sourceSets.create(name) {
        kotlin.srcDir(repo.resolve("crates/jazz-codegen/tests/apps/$app"))
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output +
            configurations.testCompileClasspath.get()
        runtimeClasspath += output + compileClasspath + configurations.testRuntimeClasspath.get()
    }
    return tasks.register<Test>(name) {
        testClassesDirs = set.output.classesDirs
        classpath = set.runtimeClasspath
        useJUnitPlatform()
    }
}
val goldenTests = listOf(goldenTest("computedTest", "kotlin-computed"))
tasks.named("check") { dependsOn("compileEdgesKotlin", goldenTests) }
tasks.named("test") { dependsOn("compileEdgesKotlin"); finalizedBy(goldenTests) }

tasks.test {
    useJUnitPlatform()
    systemProperty("jna.library.path", ffiLib.parentFile.path)
}

// Maven Central (dev/scripts/publish-kotlin.sh, .github/workflows/publish-jazz-kotlin.yml):
// the jar (with every platform's libjazz_ffi from -PnativeLibs), sources, an
// empty javadoc jar, signed. Credentials and the key come as Gradle
// properties: mavenCentralUsername, mavenCentralPassword, signingInMemoryKey,
// signingInMemoryKeyPassword.
mavenPublishing {
    publishToMavenCentral()
    signAllPublications()
    configure(
        com.vanniktech.maven.publish.KotlinJvm(
            javadocJar = com.vanniktech.maven.publish.JavadocJar.Empty(),
            sourcesJar = true,
        ),
    )
    coordinates("tools.jazz", "jazz-kotlin", version.toString())
    pom {
        name.set("jazz-kotlin")
        description.set("Jazz for Kotlin on the JVM: an end-user client of a Jazz app whose backend runs in TypeScript (experimental).")
        jazzPom()
    }
}

fun org.gradle.api.publish.maven.MavenPom.jazzPom() {
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
