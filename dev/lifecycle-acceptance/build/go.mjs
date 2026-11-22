// Go: the module the release tags (the candidate's jazz-go with release-languages' libs embedded),
// from a file GOPROXY, without cgo and without JAZZ_FFI_LIBRARY.
import { copyFileSync, cpSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { WORK, assert, fetchStep, run, sha256, timed } from "../lib/common.mjs";
import { candidate, candidateSource, cleanEnv, consumerProject, writeDriver } from "./consumer.mjs";
import { unpackFfi } from "./ffi.mjs";

const MODULE = "github.com/garden-co/jazz/packages/jazz-go/v2";
// As release-publish's "Embed the core in the Go module" zips it.
const ZIP = `
import os, sys, zipfile
src, out, prefix = sys.argv[1:4]
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as z:
    for root, _, files in os.walk(src):
        for name in files:
            full = os.path.join(root, name)
            z.write(full, prefix + "/" + os.path.relpath(full, src))
`;

export async function buildGo() {
  const { sha, version } = candidate();
  const tag = `v${version}`;
  const source = candidateSource();
  const ffi = await unpackFfi();
  const tree = join(WORK, "go-source");
  const proxy = join(WORK, "go-proxy");
  for (const dir of [tree, proxy]) rmSync(dir, { recursive: true, force: true });
  mkdirSync(tree, { recursive: true });

  const zip = await timed("go embed and zip", () => {
    const tar = join(WORK, "go-source.tar");
    const paths = ["packages/jazz-go", "dev/scripts/go-embed-ffi-libraries.sh"];
    run("git", ["-C", source, "archive", "-o", tar, sha, ...paths]);
    run("tar", ["-xf", tar, "-C", tree]);
    const archives = join(tree, "art");
    mkdirSync(archives);
    const languages = join(WORK, "artifacts", "release-languages");
    for (const name of readdirSync(languages))
      if (/^libjazz_ffi-.*\.tar\.gz$/.test(name)) copyFileSync(join(languages, name), join(archives, name));
    run("bash", [join(tree, "dev", "scripts", "go-embed-ffi-libraries.sh"), archives]);

    const versions = join(proxy, MODULE, "@v");
    mkdirSync(versions, { recursive: true });
    const module = join(tree, "packages", "jazz-go");
    writeFileSync(join(versions, "list"), `${tag}\n`);
    writeFileSync(join(versions, `${tag}.info`), JSON.stringify({ Version: tag }));
    cpSync(join(module, "go.mod"), join(versions, `${tag}.mod`));
    const file = join(versions, `${tag}.zip`);
    run("python3", ["-I", "-c", ZIP, module, file, `${MODULE}@${tag}`], { cwd: WORK });
    return file;
  });

  // Third-party modules come from the public proxy first, into a cache of their own. The build then
  // sees two file proxies and no network, so jazz-go can't come from proxy.golang.org.
  const base = cleanEnv({ CGO_ENABLED: "0", GOTOOLCHAIN: "local", GOENV: "off", GOWORK: "off", GOPRIVATE: "", GONOPROXY: "" });
  const thirdParty = join(WORK, "go-third-party");
  mkdirSync(thirdParty, { recursive: true });
  writeFileSync(join(thirdParty, "go.mod"), "module example.com/thirdparty\n\ngo 1.22\n");
  const requires = [...readFileSync(join(tree, "packages", "jazz-go", "go.mod"), "utf8").matchAll(/^(?:require\s+|\t)(\S+) (v\S+)/gm)];
  await timed("go third-party fetch", () => {
    for (const [, path, wanted] of requires)
      fetchStep("go", ["mod", "download", `${path}@${wanted}`], {
        cwd: thirdParty,
        env: { ...base, GOPROXY: "https://proxy.golang.org", GOMODCACHE: join(WORK, "go-third-party-cache"), GOFLAGS: "-modcacherw" },
      });
  });
  const env = {
    ...base,
    GOPROXY: `file://${proxy},file://${join(WORK, "go-third-party-cache", "cache", "download")}`,
    GOSUMDB: "off",
    GOMODCACHE: join(WORK, "go-modcache"),
    GOFLAGS: "-mod=mod -modcacherw",
  };
  rmSync(env.GOMODCACHE, { recursive: true, force: true });
  const dir = consumerProject("go");
  await timed("go resolve and build", () => {
    run("go", ["mod", "tidy"], { cwd: dir, env });
    run("go", ["build", "-o", "driver", "."], { cwd: dir, env });
  });

  const cached = join(env.GOMODCACHE, "cache", "download", MODULE, "@v", `${tag}.zip`);
  const zipSum = await sha256(zip);
  assert((await sha256(cached)) === zipSum, "go resolved another jazz-go zip");
  const moduleDir = run("go", ["list", "-m", "-f", "{{.Dir}}", MODULE], { cwd: dir, env, capture: true }).trim();
  assert(moduleDir.startsWith(env.GOMODCACHE), `go resolved jazz-go at ${moduleDir}`);
  assert(
    (await sha256(join(moduleDir, "lib", "linux_amd64", "libjazz_ffi.so"))) === ffi.library,
    "the resolved module doesn't embed release-languages' libjazz_ffi",
  );
  writeDriver("go", {
    cmd: [join(dir, "driver")],
    env: { XDG_CACHE_HOME: join(WORK, "go-user-cache") },
    loads: ffi.library,
    resolved: [
      { what: `${MODULE}@${tag}.zip`, sha256: zipSum },
      { what: "libjazz_ffi-linux-x86_64.tar.gz (embedded)", sha256: ffi.archive },
    ],
  });
}
