#!/usr/bin/env bash
# Smoke test of jazz-kotlin's Kotlin Multiplatform package, as an adopter
# installs it: the zipped local Maven repository attached to a GitHub release
# (jazz-kotlin-maven-<version>.zip).
#
#   dev/kmp-smoke/run.sh <zip> <version>
#
# It builds two clean Gradle projects outside this checkout, each with <zip>
# unzipped into jazz-maven/ beside it, as the install docs
# (docs/content/docs/setup/kotlin.mdx) say, and depending on it only through
# their snippet:
#
#   repositories {
#       exclusiveContent {
#           forRepository { maven { url = uri(rootDir.resolve("jazz-maven")) } }
#           filter { includeGroup("tools.jazz") }
#       }
#       mavenCentral()
#   }
#   dependencies { implementation("tools.jazz:jazz-kotlin:<version>") }
#
# 1. JVM: checks Gradle resolved jazz-kotlin to its -jvm variant through the
#    Gradle module metadata, then runs an app that opens a local, persistent
#    Jazz app, writes, closes, reopens and reads the row back. The native
#    library comes from the jar (no jna.library.path).
# 2. Android: an app module resolving jazz-kotlin to jazz-kotlin-android,
#    assembled debug and release (R8, with no keep rules of its own); the APKs
#    must carry libjazz_ffi and JNA's libjnidispatch for arm64-v8a and x86_64.
#    It isn't run on a device.
#
# The generated bindings both apps use are the docs todo app's committed ones
# (examples/docs/todo-client-kotlin/src/main/kotlin/JazzApp.kt).
#
# Needs: a JDK and Gradle (dev/scripts/kotlin-toolchain.sh fetches the pinned
# ones), and ANDROID_HOME (or ANDROID_SDK_ROOT) with platform android-36 and
# build-tools 36.0.0 for the Android part. KMP_SMOKE_SKIP_ANDROID=1 skips it,
# and says so. Linux or macOS. Any failure exits non-zero with FAIL: <why>.

set -euo pipefail

# One line per check for a gate to count: PASS <check>, or FAIL <check>: <why> (then exit 1).
# Checks: zip-layout, jvm-resolve, jvm-roundtrip, android-resolve, android-assemble,
# android-natives-app-debug, android-natives-app-release, android-r8-keep; with
# KMP_SMOKE_SKIP_ANDROID=1, NOT_COVERED android in place of the Android ones.
check=setup
fail() { echo "FAIL $check: $*"; echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS $1"; }
step() { echo; echo "== $*"; }

[ $# -eq 2 ] || { echo "usage: $0 <jazz-kotlin-maven-VERSION.zip> <version>" >&2; exit 2; }
zip="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
version="$2"
[ -f "$zip" ] || fail "no zip at $1"

repo="$(cd "$(dirname "$0")/../.." && pwd)"
bindings="$repo/examples/docs/todo-client-kotlin/src/main/kotlin/JazzApp.kt"
[ -f "$bindings" ] || fail "no bindings at $bindings"

KOTLIN_TOOLCHAIN="${KOTLIN_TOOLCHAIN:-pinned}"
# shellcheck source=../scripts/kotlin-toolchain.sh
. "$repo/dev/scripts/kotlin-toolchain.sh"
command -v gradle >/dev/null || fail "no gradle"

work="$(mktemp -d "${TMPDIR:-/tmp}/kmp-smoke.XXXXXX")"
echo "work dir: $work"
gradle_flags=(--no-daemon --console=plain --stacktrace)
# Small enough for a 4 GB machine: one JVM (the Kotlin compiler runs in Gradle's).
gradle_properties="org.gradle.jvmargs=-Xmx1536m -XX:MaxMetaspaceSize=512m
kotlin.compiler.execution.strategy=in-process
org.gradle.workers.max=2"
# Gradle's cache of tools.jazz from an earlier run must not answer for this zip.
export GRADLE_USER_HOME="${KMP_SMOKE_GRADLE_HOME:-$work/gradle-home}"
rm -rf "$GRADLE_USER_HOME/caches/modules-2/files-2.1/tools.jazz" "$GRADLE_USER_HOME/caches/modules-2/metadata-"*/descriptors/tools.jazz

check=zip-layout
step "unzip $(basename "$zip")"
# As the docs do: unzipped into jazz-maven/ beside the build, tools/jazz/ at its top.
mkdir "$work/unzipped"
unzip -q "$zip" -d "$work/unzipped"
mavenrepo="$work/unzipped"
[ -f "$mavenrepo/tools/jazz/jazz-kotlin/$version/jazz-kotlin-$version.module" ] \
  || fail "the zip has no tools/jazz/jazz-kotlin/$version/jazz-kotlin-$version.module at its top (the root's Gradle module metadata); it has: $(cd "$mavenrepo" && find . -maxdepth 4 | head -20 | tr '\n' ' ')"
for a in jazz-kotlin jazz-kotlin-jvm jazz-kotlin-android; do
  d="$mavenrepo/tools/jazz/$a/$version"
  [ -f "$d/$a-$version.pom" ] || fail "no $a-$version.pom in the zip"
  [ -f "$d/$a-$version.module" ] || fail "no $a-$version.module in the zip"
done
[ -f "$mavenrepo/tools/jazz/jazz-kotlin-jvm/$version/jazz-kotlin-jvm-$version.jar" ] || fail "no jazz-kotlin-jvm-$version.jar"
[ -f "$mavenrepo/tools/jazz/jazz-kotlin-android/$version/jazz-kotlin-android-$version.aar" ] || fail "no jazz-kotlin-android-$version.aar"
(cd "$mavenrepo" && find tools -type f | sort | sed 's/^/  /')

# The docs' snippet, word for word.
jazz_repository="    exclusiveContent {
        forRepository { maven { url = uri(rootDir.resolve(\"jazz-maven\")) } }
        filter { includeGroup(\"tools.jazz\") }
    }
    mavenCentral()"

# ---------------------------------------------------------------- JVM
pass zip-layout
step "JVM project"
jvm="$work/jvm-app"
mkdir -p "$jvm/src/main/kotlin/smoke"
unzip -q "$zip" -d "$jvm/jazz-maven"
echo "$gradle_properties" > "$jvm/gradle.properties"
cat > "$jvm/settings.gradle.kts" <<'EOF'
rootProject.name = "kmp-smoke-jvm"
EOF
cat > "$jvm/build.gradle.kts" <<EOF
plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
    application
}

repositories {
$jazz_repository
}

dependencies {
    implementation("tools.jazz:jazz-kotlin:$version")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}
kotlin { compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) } }
application { mainClass.set("smoke.MainKt") }

// What Gradle resolved tools.jazz:jazz-kotlin to, through its module metadata.
tasks.register("jazzVariant") {
    val result = configurations.runtimeClasspath.get().incoming.resolutionResult
    val files = configurations.runtimeClasspath.get().incoming.artifacts
    doLast {
        result.allComponents.filter { it.moduleVersion?.group == "tools.jazz" }.forEach { c ->
            println("COMPONENT \${c.moduleVersion} variants=\${c.variants.map { it.displayName }}")
        }
        files.artifacts.filter { it.file.name.contains("jazz") || it.file.name.startsWith("jna") }
            .forEach { println("FILE \${it.file.name}") }
    }
}
EOF
cp "$bindings" "$jvm/src/main/kotlin/JazzApp.kt"
cat > "$jvm/src/main/kotlin/smoke/Main.kt" <<'EOF'
package smoke

import jazzapp.NewTodo
import jazzapp.Todos
import kotlinx.coroutines.runBlocking
import tools.jazz.JazzConfig
import tools.jazz.Storage
import java.io.File
import kotlin.system.exitProcess

fun main(args: Array<String>) = runBlocking {
    val dataDir = File(args[0])
    val title = "kmp smoke ${System.nanoTime()}"
    val config = JazzConfig(appId = "6f1c4a52-3d0e-4b7a-9c1e-5a2b8d7f0c31", storage = Storage.Persistent, dataDir = dataDir.path)

    val written = jazzapp.open(config).let { jazz ->
        try {
            jazz.insert(Todos, NewTodo(title = title, done = false))
        } finally {
            jazz.closeAndWait()
        }
    }
    println("WROTE ${written.id} ${written.title}")

    val read = jazzapp.open(config).let { jazz ->
        try {
            jazz.all(Todos.where(Todos.title eq title))
        } finally {
            jazz.closeAndWait()
        }
    }
    println("READ ${read.map { it.id to it.title }}")
    if (read.size != 1 || read[0].id != written.id || read[0].title != title) {
        System.err.println("read back ${read}, wanted the row ${written.id}")
        exitProcess(1)
    }
    println("JVM SMOKE OK")
}
EOF

check=jvm-resolve
(cd "$jvm" && gradle "${gradle_flags[@]}" -q jazzVariant) | tee "$work/jvm-variant.txt" || fail "JVM: Gradle failed resolving tools.jazz:jazz-kotlin:$version (above)"
grep -q "^COMPONENT tools.jazz:jazz-kotlin:$version " "$work/jvm-variant.txt" || fail "JVM: tools.jazz:jazz-kotlin:$version not in the runtime classpath"
grep -q "^COMPONENT tools.jazz:jazz-kotlin-jvm:$version " "$work/jvm-variant.txt" || fail "JVM: jazz-kotlin didn't resolve to jazz-kotlin-jvm through its module metadata"
grep -q "^FILE jazz-kotlin-jvm-$version.jar$" "$work/jvm-variant.txt" || fail "JVM: jazz-kotlin-jvm-$version.jar isn't on the runtime classpath"
grep -q "^COMPONENT tools.jazz:jazz-kotlin-android" "$work/jvm-variant.txt" && fail "JVM: the Android variant leaked into the JVM classpath"
grep -q "^FILE jna-.*\.jar$" "$work/jvm-variant.txt" || fail "JVM: JNA's jar isn't on the runtime classpath"
grep -q "^FILE jna-.*\.aar$" "$work/jvm-variant.txt" && fail "JVM: JNA's Android AAR is on the JVM classpath"
echo "JVM: jazz-kotlin -> jazz-kotlin-jvm ok"
pass jvm-resolve

check=jvm-roundtrip
(cd "$jvm" && gradle "${gradle_flags[@]}" -q run --args="$work/jvm-data") | tee "$work/jvm-run.txt" || fail "JVM: the app failed"
grep -q "^JVM SMOKE OK$" "$work/jvm-run.txt" || fail "JVM: the app didn't write and read back"
pass jvm-roundtrip

# ---------------------------------------------------------------- Android
if [ "${KMP_SMOKE_SKIP_ANDROID:-}" = 1 ]; then
  echo
  echo "NOT_COVERED android (KMP_SMOKE_SKIP_ANDROID=1)"
  echo "SKIPPED: Android (KMP_SMOKE_SKIP_ANDROID=1). The KMP package is NOT checked on Android."
  echo "KMP SMOKE: JVM PASS, ANDROID SKIPPED"
  exit 0
fi

step "Android project"
check=android-sdk
sdk="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
[ -n "$sdk" ] && [ -d "$sdk" ] || fail "Android: set ANDROID_HOME to an Android SDK (or KMP_SMOKE_SKIP_ANDROID=1)"
[ -d "$sdk/platforms/android-36" ] || fail "Android: the SDK lacks platforms;android-36"
[ -d "$sdk/build-tools/36.0.0" ] || fail "Android: the SDK lacks build-tools;36.0.0"

and="$work/android-app"
mkdir -p "$and/app/src/main/kotlin/smoke"
unzip -q "$zip" -d "$and/jazz-maven"
echo "sdk.dir=$sdk" > "$and/local.properties"
printf '%s\nandroid.useAndroidX=true\n' "$gradle_properties" > "$and/gradle.properties"
cat > "$and/settings.gradle.kts" <<'EOF'
pluginManagement {
    repositories { google(); mavenCentral(); gradlePluginPortal() }
}
rootProject.name = "kmp-smoke-android"
include(":app")
EOF
cat > "$and/build.gradle.kts" <<'EOF'
plugins {
    id("com.android.application") version "8.13.0" apply false
    kotlin("android") version "2.2.21" apply false
    kotlin("plugin.serialization") version "2.2.21" apply false
}
EOF
cat > "$and/app/build.gradle.kts" <<EOF
plugins {
    id("com.android.application")
    kotlin("android")
    kotlin("plugin.serialization")
}

repositories {
    google()
$jazz_repository
}

android {
    namespace = "smoke"
    compileSdk = 36
    buildToolsVersion = "36.0.0"
    defaultConfig {
        applicationId = "tools.jazz.kmpsmoke"
        minSdk = 24
        targetSdk = 36
        versionCode = 1
        versionName = "1"
    }
    buildTypes {
        release {
            isMinifyEnabled = true
            // No keep rules of the app's own: the library's consumer rules have to be enough.
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"))
            signingConfig = signingConfigs.getByName("debug")
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    lint { checkReleaseBuilds = false }
}

kotlin { compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) } }

dependencies {
    implementation("tools.jazz:jazz-kotlin:$version")
}

tasks.register("jazzVariant") {
    val result = configurations.getByName("releaseRuntimeClasspath").incoming.resolutionResult
    doLast {
        result.allComponents.filter { it.moduleVersion?.group == "tools.jazz" || it.moduleVersion?.group == "net.java.dev.jna" }.forEach { c ->
            println("COMPONENT \${c.moduleVersion} variants=\${c.variants.map { it.displayName }}")
        }
    }
}
EOF
cat > "$and/app/src/main/AndroidManifest.xml" <<'EOF'
<?xml version="1.0" encoding="utf-8"?>
<manifest xmlns:android="http://schemas.android.com/apk/res/android">
    <application android:label="kmp smoke">
        <activity android:name=".SmokeActivity" android:exported="true">
            <intent-filter>
                <action android:name="android.intent.action.MAIN" />
                <category android:name="android.intent.category.LAUNCHER" />
            </intent-filter>
        </activity>
    </application>
</manifest>
EOF
cp "$bindings" "$and/app/src/main/kotlin/JazzApp.kt"
cat > "$and/app/src/main/kotlin/smoke/SmokeActivity.kt" <<'EOF'
package smoke

import android.app.Activity
import android.os.Bundle
import android.util.Log
import jazzapp.NewTodo
import jazzapp.Todos
import kotlinx.coroutines.runBlocking
import tools.jazz.JazzConfig
import tools.jazz.Storage
import java.io.File

class SmokeActivity : Activity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val dataDir = File(filesDir, "jazz")
        Thread {
            runBlocking {
                val config = JazzConfig(appId = "6f1c4a52-3d0e-4b7a-9c1e-5a2b8d7f0c31", storage = Storage.Persistent, dataDir = dataDir.path)
                val jazz = jazzapp.open(config)
                try {
                    val row = jazz.insert(Todos, NewTodo(title = "kmp smoke", done = false))
                    val read = jazz.all(Todos.where(Todos.id eq row.id))
                    Log.i("KmpSmoke", if (read.singleOrNull()?.title == "kmp smoke") "KMP SMOKE OK" else "KMP SMOKE FAIL $read")
                } finally {
                    jazz.closeAndWait()
                }
            }
        }.start()
    }
}
EOF

check=android-resolve
(cd "$and" && gradle "${gradle_flags[@]}" -q :app:jazzVariant) | tee "$work/android-variant.txt" || fail "Android: Gradle failed resolving tools.jazz:jazz-kotlin:$version (above)"
grep -q "^COMPONENT tools.jazz:jazz-kotlin-android:$version " "$work/android-variant.txt" || fail "Android: jazz-kotlin didn't resolve to jazz-kotlin-android through its module metadata"
grep -q "^COMPONENT tools.jazz:jazz-kotlin-jvm" "$work/android-variant.txt" && fail "Android: the JVM variant leaked into the Android classpath"
echo "Android: jazz-kotlin -> jazz-kotlin-android ok"
pass android-resolve
check=android-assemble

(cd "$and" && gradle "${gradle_flags[@]}" :app:assembleDebug :app:assembleRelease) > "$work/android-build.txt" 2>&1 \
  || { tail -60 "$work/android-build.txt"; fail "Android: assembleDebug/assembleRelease failed (log: $work/android-build.txt)"; }
pass android-assemble
check=android-natives

for apk in "$and/app/build/outputs/apk/debug/app-debug.apk" "$and/app/build/outputs/apk/release/app-release.apk"; do
  check="android-natives-$(basename "$apk" .apk)"
  [ -f "$apk" ] || fail "Android: no $(basename "$apk")"
  listing="$(unzip -l "$apk")"
  for abi in arm64-v8a x86_64; do
    grep -q "lib/$abi/libjazz_ffi.so" <<<"$listing" || fail "Android: $(basename "$apk") lacks lib/$abi/libjazz_ffi.so"
    grep -q "lib/$abi/libjnidispatch.so" <<<"$listing" || fail "Android: $(basename "$apk") lacks JNA's lib/$abi/libjnidispatch.so"
  done
  echo "Android: $(basename "$apk") ok ($(du -h "$apk" | cut -f1))"
  pass "$check"
done

# R8 must have kept what JNA reaches by reflection: the UniFFI bindings and JNA's own classes.
check=android-r8-keep
mapping="$and/app/build/outputs/mapping/release/mapping.txt"
[ -f "$mapping" ] || fail "Android: R8 wrote no mapping.txt"
grep -q "^uniffi.jazz_ffi\.[A-Za-z_]* -> uniffi.jazz_ffi\." "$mapping" || fail "Android: R8 renamed or dropped uniffi.jazz_ffi (the consumer rules don't keep it)"
grep -q "^com.sun.jna.Native -> com.sun.jna.Native:" "$mapping" || fail "Android: R8 renamed com.sun.jna.Native"

echo
pass android-r8-keep
echo
echo "KMP SMOKE: JVM PASS, ANDROID PASS (assembled debug and R8 release; not run on a device)"
