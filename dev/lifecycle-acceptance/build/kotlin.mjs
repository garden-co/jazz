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
  const jazzJars = readdirSync(lib).filter((name) => name.startsWith("jazz"));
  const jar = `jazz-kotlin-${version}.jar`;
  assert(jazzJars.join() === jar, `the install holds ${jazzJars.join(", ") || "no jazz jar"}, not just ${jar}`);
  const manifest = JSON.parse(readFileSync(join(WORK, "artifacts", "release-manifest", "manifest.json"), "utf8"));
  const listed = manifest.files.find(
    (file) => file.artifact === "release-maven" && file.path === `jazz-kotlin/${version}/${jar}`,
  );
  const got = await sha256(join(lib, jar));
  assert(listed && got === listed.sha256, `gradle resolved a ${jar} (${got}) that isn't the manifest's`);
  const classpath = readdirSync(lib).map((name) => join(lib, name)).join(":");
  writeDriver("kotlin", {
    cmd: ["java", "-cp", classpath, "lifecycle.DriverKt"],
    resolved: [{ what: `release-maven tools.jazz:jazz-kotlin:${version}`, sha256: got }],
  });
}
