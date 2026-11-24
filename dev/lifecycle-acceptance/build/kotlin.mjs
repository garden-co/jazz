// Kotlin/JVM: by Maven coordinate from release-maven as a local repository, exclusive for tools.jazz.
import { cpSync, mkdirSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { WORK, assert, fetchStep, run, sha256, timed } from "../lib/common.mjs";
import { candidate, cleanEnv, consumerProject, writeDriver } from "./consumer.mjs";

export async function buildKotlin() {
  const { version } = candidate();
  const repo = join(WORK, "maven");
  rmSync(repo, { recursive: true, force: true });
  mkdirSync(join(repo, "tools"), { recursive: true });
  const artifact = join(WORK, "artifacts", "release-maven");
  cpSync(artifact, join(repo, "tools", "jazz"), { recursive: true });

  const dir = consumerProject("kotlin");
  const env = cleanEnv({ GRADLE_USER_HOME: join(WORK, "gradle-home") });
  const gradle = ["--no-daemon", "--console=plain", `-PcandidateRepo=${repo}`];
  await timed("kotlin dependency fetch", () =>
    fetchStep("gradle", [...gradle, "fetchDependencies"], { cwd: dir, env }),
  );
  await timed("kotlin build", () => run("gradle", [...gradle, "--offline", "installDist"], { cwd: dir, env }));

  const lib = join(dir, "build", "install", "lifecycle-driver", "lib");
  // tools.jazz:jazz-kotlin may be one jar (beta.1) or a Multiplatform root whose Gradle module
  // sends JVM builds to jazz-kotlin-jvm (beta.2). Either way, every Jazz jar on the classpath is a
  // release-maven file of this candidate, byte for byte, at this version.
  const jazzJars = readdirSync(lib).filter((name) => name.startsWith("jazz"));
  assert(jazzJars.some((name) => /^jazz-kotlin(-jvm)?-/.test(name)), `the install holds no jazz-kotlin jar (${jazzJars.join(", ") || "none"})`);
  const manifest = JSON.parse(readFileSync(join(WORK, "artifacts", "release-manifest", "manifest.json"), "utf8"));
  const resolved = [];
  for (const name of jazzJars) {
    const artifact = name.replace(`-${version}.jar`, "");
    assert(name === `${artifact}-${version}.jar`, `the install holds ${name}, not version ${version}`);
    const listed = manifest.files.find(
      (file) => file.artifact === "release-maven" && file.path === `${artifact}/${version}/${name}`,
    );
    const got = await sha256(join(lib, name));
    assert(listed && got === listed.sha256, `gradle resolved a ${name} (${got}) that isn't the manifest's`);
    resolved.push({ what: `release-maven tools.jazz:${artifact}:${version}`, sha256: got });
  }
  const classpath = readdirSync(lib).map((name) => join(lib, name)).join(":");
  writeDriver("kotlin", {
    cmd: ["java", "-cp", classpath, "lifecycle.DriverKt"],
    resolved,
  });
}
