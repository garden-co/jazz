// An Android app of the jazz-kotlin-android AAR, for
// dev/scripts/test-kotlin-android-emulator.sh: it takes the AAR by coordinate
// from -PjazzRepository, as a user's app would, and is minified with R8, so the
// AAR's dependencies and consumer rules are what make it work.

plugins {
    id("com.android.application") version "8.13.0"
    kotlin("android") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
}

val repo = rootDir.resolve("../..").canonicalFile

android {
    namespace = "smoke"
    compileSdk = 36
    buildToolsVersion = "36.0.0"

    defaultConfig {
        applicationId = "tools.jazz.smoke"
        minSdk = 24
        targetSdk = 36
        versionCode = 1
        versionName = "1"
    }

    buildTypes {
        release {
            isMinifyEnabled = true
            // No keep rules of the app's own: the AAR's have to be enough.
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

kotlin {
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
}

dependencies {
    implementation("tools.jazz:jazz-kotlin-android:${providers.gradleProperty("jazzVersion").get()}")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.10.2")
}

// StagePlan's committed bindings, unchanged.
val stagePlanBindings by tasks.registering(Sync::class) {
    from(repo.resolve("examples/stage-plan/apps/kotlin-cli/src/main/kotlin/JazzApp.kt"))
    into(layout.buildDirectory.dir("generated/stage-plan"))
}
android.sourceSets.getByName("main").kotlin.srcDir(layout.buildDirectory.dir("generated/stage-plan"))
tasks.named("preBuild") { dependsOn(stagePlanBindings) }
