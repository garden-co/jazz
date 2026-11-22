pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        // tools.jazz comes from the candidate's release-maven, and can come from nowhere else.
        exclusiveContent {
            forRepository { maven { url = uri(providers.gradleProperty("candidateRepo").get()) } }
            filter { includeGroup("tools.jazz") }
        }
        mavenCentral()
    }
}

rootProject.name = "lifecycle-driver"
