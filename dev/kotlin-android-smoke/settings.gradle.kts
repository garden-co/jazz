pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

val jazzRepository = providers.gradleProperty("jazzRepository")

dependencyResolutionManagement {
    repositories {
        // tools.jazz comes only from the build under test: never an older
        // release that happens to be published.
        exclusiveContent {
            forRepository {
                maven { url = uri(file(jazzRepository.get())) }
            }
            filter { includeGroup("tools.jazz") }
        }
        google()
        mavenCentral()
    }
}

rootProject.name = "kotlin-android-smoke"
