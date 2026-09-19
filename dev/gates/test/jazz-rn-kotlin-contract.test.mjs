import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import test from "node:test";
import { copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

function scanGroovy(source) {
  const tokens = [];
  let depth = 0;
  let index = 0;

  while (index < source.length) {
    const character = source[index];
    const next = source[index + 1];

    if (/\s/.test(character)) {
      index += 1;
      continue;
    }

    if (character === "/" && next === "/") {
      index += 2;
      while (index < source.length && source[index] !== "\n") index += 1;
      continue;
    }

    if (character === "/" && next === "*") {
      const start = index;
      index += 2;
      while (index < source.length && !(source[index] === "*" && source[index + 1] === "/")) {
        index += 1;
      }
      assert.equal(source[index], "*", `unterminated Groovy block comment at ${start}`);
      index += 2;
      continue;
    }

    if (character === "'" || character === '"') {
      const start = index;
      const delimiter = source.startsWith(character.repeat(3), index)
        ? character.repeat(3)
        : character;
      index += delimiter.length;
      let terminated = false;
      while (index < source.length) {
        if (source.startsWith(delimiter, index)) {
          index += delimiter.length;
          terminated = true;
          break;
        }
        if (source[index] === "\\") index += 2;
        else index += 1;
      }
      assert.ok(terminated, `unterminated Groovy string at ${start}`);
      tokens.push({ kind: "string", start, end: index, depth });
      continue;
    }

    if (/[A-Za-z_$]/.test(character)) {
      const start = index;
      index += 1;
      while (index < source.length && /[A-Za-z0-9_$]/.test(source[index])) index += 1;
      tokens.push({
        kind: "identifier",
        value: source.slice(start, index),
        start,
        end: index,
        depth,
      });
      continue;
    }

    if (character === "}") {
      assert.ok(depth > 0, `unexpected closing Groovy brace at ${index}`);
      depth -= 1;
    }
    tokens.push({ kind: "punctuation", value: character, start: index, end: index + 1, depth });
    if (character === "{") depth += 1;
    index += 1;
  }

  assert.equal(depth, 0, "unterminated Groovy brace");
  return tokens;
}

function matchingBraceEnd(tokens, openingIndex) {
  let depth = 0;
  for (let index = openingIndex; index < tokens.length; index += 1) {
    if (tokens[index].value === "{") depth += 1;
    if (tokens[index].value === "}" && --depth === 0) return tokens[index].end;
  }
  throw new Error("unterminated Groovy block");
}

function extractTopLevelBlock(source, tokens, identifier, occurrence = 0) {
  let seen = 0;
  for (let index = 0; index < tokens.length - 1; index += 1) {
    const token = tokens[index];
    if (
      token.kind === "identifier" &&
      token.value === identifier &&
      token.depth === 0 &&
      tokens[index + 1].value === "{"
    ) {
      if (seen === occurrence) {
        return source.slice(token.start, matchingBraceEnd(tokens, index + 1));
      }
      seen += 1;
    }
  }
  throw new Error(`missing top-level ${identifier} block`);
}

function extractTopLevelFunction(source, tokens, name) {
  for (let index = 1; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (
      token.kind !== "identifier" ||
      token.value !== name ||
      token.depth !== 0 ||
      tokens[index - 1].kind !== "identifier" ||
      tokens[index - 1].value !== "def"
    ) {
      continue;
    }

    let parentheses = 0;
    for (let bodyIndex = index + 1; bodyIndex < tokens.length; bodyIndex += 1) {
      const bodyToken = tokens[bodyIndex];
      if (bodyToken.value === "(") parentheses += 1;
      else if (bodyToken.value === ")") parentheses -= 1;
      else if (bodyToken.value === "{" && parentheses === 0) {
        return source.slice(tokens[index - 1].start, matchingBraceEnd(tokens, bodyIndex));
      }
    }
  }
  throw new Error(`missing top-level ${name} function`);
}

function extractTopLevelInitializer(source, tokens, name) {
  for (let index = 1; index < tokens.length - 2; index += 1) {
    if (
      tokens[index - 1].kind === "identifier" &&
      tokens[index - 1].value === "def" &&
      tokens[index].kind === "identifier" &&
      tokens[index].value === name &&
      tokens[index].depth === 0 &&
      tokens[index + 1].value === "="
    ) {
      const lineEnd = source.indexOf("\n", tokens[index - 1].start);
      return source.slice(tokens[index - 1].start, lineEnd === -1 ? source.length : lineEnd).trim();
    }
  }
  throw new Error(`missing top-level ${name} initializer`);
}

function extractAndroidGradleContract(source) {
  const tokens = scanGroovy(source);
  return {
    buildscript: extractTopLevelBlock(source, tokens, "buildscript"),
    helper: extractTopLevelFunction(source, tokens, "getExtOrDefault"),
    kotlinVersionInitializer: extractTopLevelInitializer(source, tokens, "kotlin_version"),
    dependencies: extractTopLevelBlock(source, tokens, "dependencies"),
  };
}

test("jazz-rn Android resolves the actual Kotlin plugin and stdlib declarations", async () => {
  const source = await readFile(
    new URL("../../../crates/jazz-rn/android/build.gradle", import.meta.url),
    "utf8",
  );
  const { buildscript, helper, kotlinVersionInitializer, dependencies } =
    extractAndroidGradleContract(source);

  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-kotlin-contract-"));
  try {
    await copyFile(
      new URL("../../../crates/jazz-rn/android/gradle.properties", import.meta.url),
      join(directory, "gradle.properties"),
    );
    await writeFile(
      join(directory, "settings.gradle"),
      `rootProject.name = "kotlin-contract"

def rootKotlinVersion = gradle.startParameter.projectProperties['jazzRnRootKotlinVersion']
gradle.beforeProject { project ->
  if (rootKotlinVersion != null && !rootKotlinVersion.isEmpty()) {
    project.rootProject.ext.kotlinVersion = rootKotlinVersion
  }
}
`,
    );
    await writeFile(
      join(directory, "build.gradle"),
      [
        buildscript,
        'apply plugin: "java-library"',
        helper,
        kotlinVersionInitializer,
        `repositories {
  mavenCentral()
  google()
}`,
        dependencies,
        `def shippedKotlinVersion = project.properties["JazzRn_kotlinVersion"]?.toString()
def requestedRootKotlinVersion = project.properties["jazzRnRootKotlinVersion"]?.toString()
def expectedKotlinVersion = requestedRootKotlinVersion != null && !requestedRootKotlinVersion.isEmpty()
  ? requestedRootKotlinVersion
  : shippedKotlinVersion

tasks.register("assertKotlinVersionContract") {
  doLast {
    assert shippedKotlinVersion != null && !shippedKotlinVersion.isEmpty():
      "the isolated root must read JazzRn_kotlinVersion from its copied gradle.properties"
    assert expectedKotlinVersion != null && !expectedKotlinVersion.isEmpty():
      "the Kotlin version fallback must be present"
    if (requestedRootKotlinVersion != null && !requestedRootKotlinVersion.isEmpty()) {
      assert rootProject.ext.has("kotlinVersion"):
        "the override must install rootProject.ext.kotlinVersion"
      assert rootProject.ext.get("kotlinVersion").toString() == requestedRootKotlinVersion:
        "the installed root Kotlin version must match the requested override"
    }

    def kotlinPluginArtifact = project.buildscript.configurations.classpath.resolvedConfiguration.resolvedArtifacts.find {
      it.moduleVersion.id.group == "org.jetbrains.kotlin" &&
        it.moduleVersion.id.name == "kotlin-gradle-plugin"
    }
    assert kotlinPluginArtifact != null: "the actual buildscript classpath must resolve the Kotlin Gradle plugin"
    assert kotlinPluginArtifact.moduleVersion.id.version == expectedKotlinVersion:
      "the resolved Kotlin Gradle plugin must use the selected version"

    def declaredDependencies = configurations.implementation.dependencies.collect {
      it.group + ":" + it.name + ":" + it.version
    }
    assert declaredDependencies.contains("com.facebook.react:react-native:+"):
      "the source dependency list must retain React"
    assert declaredDependencies.contains("org.jetbrains.kotlin:kotlin-stdlib:" + expectedKotlinVersion):
      "the source dependency list must retain Kotlin at the selected version"

    def sourceDeclaredKotlinStdlib = configurations.implementation.dependencies.find {
      it.group == "org.jetbrains.kotlin" && it.name == "kotlin-stdlib"
    }
    assert sourceDeclaredKotlinStdlib != null: "the source-declared Kotlin stdlib dependency is required"
    def detachedKotlinStdlib = configurations.detachedConfiguration(sourceDeclaredKotlinStdlib)
    def resolvedKotlinArtifacts = detachedKotlinStdlib.resolvedConfiguration.resolvedArtifacts
    assert resolvedKotlinArtifacts.any {
      it.moduleVersion.id.group == "org.jetbrains.kotlin" &&
        it.moduleVersion.id.name == "kotlin-stdlib" &&
        it.moduleVersion.id.version == expectedKotlinVersion
    }: "the source-declared Kotlin stdlib must resolve at the selected version"
    assert resolvedKotlinArtifacts.every {
      it.moduleVersion.id.group != "com.facebook.react"
    }: "the detached Kotlin resolution must not resolve React"
  }
}
`,
      ].join("\n\n"),
    );

    const gradle = new URL("../../../dev/rn-device-acceptance/android/gradlew", import.meta.url)
      .pathname;
    execFileSync(gradle, ["--no-daemon", "-p", directory, "assertKotlinVersionContract"], {
      stdio: "inherit",
    });
    execFileSync(
      gradle,
      [
        "--no-daemon",
        "-p",
        directory,
        "-PjazzRnRootKotlinVersion=1.9.24",
        "assertKotlinVersionContract",
      ],
      { stdio: "inherit" },
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
