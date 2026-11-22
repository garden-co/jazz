plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
    application
}

dependencies {
    implementation("tools.jazz:jazz-kotlin:__VERSION__")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
}

application { mainClass.set("lifecycle.DriverKt") }

// Resolves everything the build needs, so the build runs --offline and only the fetch retries.
tasks.register("fetchDependencies") {
    doLast {
        (buildscript.configurations + configurations).filter { it.isCanBeResolved }.forEach { it.resolve() }
    }
}
