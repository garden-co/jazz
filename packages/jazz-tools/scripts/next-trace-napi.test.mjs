// A Next.js route that starts `createJazz` works from only its traced files,
// as Vercel deploys it (B65-1). createJazz loads jazz.permissions.json through
// jazz-napi's loader. When it reached jazz-napi through a variable specifier,
// Turbopack folded that to a bare `import("jazz-napi")` in the server chunk.
// Next ships jazz-napi only under its hashed external alias, so on Vercel with
// pnpm every /jazz route answered 503: "Cannot find package 'jazz-napi'".
//
// The test packs jazz-tools, jazz-napi and jazz-wasm, installs them in a pnpm
// app the way a user's install lays them out, and runs `next build` with
// `output: "standalone"`. That copies exactly the route's `.nft.json` trace
// into .next/standalone, which is the file set Vercel ships. It then moves the
// app's own node_modules away, starts the standalone server and requests the
// route.
import test from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = fileURLToPath(new URL("..", import.meta.url));
const repo = join(packageDir, "..", "..");
/** The version of `name` the repo installs (next at the root, react beside jazz-tools). */
function versionOf(name) {
  for (const from of [packageDir, repo]) {
    try {
      const manifest = createRequire(join(from, "package.json")).resolve(`${name}/package.json`);
      return JSON.parse(readFileSync(manifest, "utf8")).version;
    } catch {
      // Not installed there.
    }
  }
  throw new Error(`${name} isn't installed in the repo`);
}

/**
 * The environment of the consumer's install, build and server: without the
 * monorepo's sealed-artifact overrides (JAZZ_CORRECTNESS_*, NAPI_RS_*), so the
 * deployed copy loads the jazz-napi it ships, as on Vercel.
 */
const consumerEnv = Object.fromEntries(
  Object.entries(process.env).filter(
    ([key]) =>
      !/^JAZZ_CORRECTNESS_/.test(key) &&
      key !== "NAPI_RS_NATIVE_LIBRARY_PATH" &&
      key !== "NAPI_RS_FORCE_WASI",
  ),
);

function run(command, args, cwd, timeout = 600_000) {
  try {
    return execFileSync(command, args, {
      cwd,
      encoding: "utf8",
      stdio: "pipe",
      timeout,
      env: consumerEnv,
    });
  } catch (error) {
    error.message += `\n${error.stdout ?? ""}${error.stderr ?? ""}`;
    throw error;
  }
}

/** `pnpm pack` of a workspace package into `dest`, returning the tarball's path. */
function pack(dir, dest) {
  const before = new Set(readdirSync(dest));
  run("pnpm", ["pack", "--pack-destination", dest], dir);
  const tarball = readdirSync(dest).find((name) => !before.has(name) && name.endsWith(".tgz"));
  assert.ok(tarball, `pnpm pack wrote no tarball for ${dir}`);
  return join(dest, tarball);
}

function consumerApp(root, tarballs) {
  writeFileSync(
    join(root, "package.json"),
    JSON.stringify({
      name: "next-trace-consumer",
      private: true,
      dependencies: {
        "jazz-tools": `file:${tarballs.tools}`,
        "jazz-napi": `file:${tarballs.napi}`,
        next: versionOf("next"),
        react: versionOf("react"),
        "react-dom": versionOf("react-dom"),
        // Next needs these for a .ts route and only installs them itself outside CI.
        typescript: versionOf("typescript"),
        "@types/react": versionOf("@types/react"),
        "@types/node": versionOf("@types/node"),
      },
    }),
  );
  writeFileSync(
    join(root, "pnpm-workspace.yaml"),
    [
      "packages: []",
      "minimumReleaseAge: 0",
      "overrides:",
      `  jazz-wasm: file:${tarballs.wasm}`,
      `  jazz-napi: file:${tarballs.napi}`,
      // Install scripts the app doesn't need; pnpm 11 refuses to leave them undecided.
      "allowBuilds:",
      "  sharp: false",
      "  protobufjs: false",
      "",
    ].join("\n"),
  );
  // As the docs configure it.
  writeFileSync(
    join(root, "next.config.mjs"),
    'export default { output: "standalone", serverExternalPackages: ["jazz-napi"], typescript: { ignoreBuildErrors: true } };\n',
  );
  const app = { format: 1, tables: { notes: { columns: { body: "string" } } } };
  writeFileSync(join(root, "jazz.app.json"), JSON.stringify(app));
  writeFileSync(
    join(root, "jazz.permissions.json"),
    JSON.stringify({ format: 1, tables: { notes: { read: "true", insert: "true" } } }),
  );
  const napi = createRequire(join(packageDir, "package.json"))("jazz-napi");
  writeFileSync(join(root, "jazz.generated.ts"), napi.generate("ts", JSON.stringify(app)));
  mkdirSync(join(root, "app", "jazz"), { recursive: true });
  writeFileSync(
    join(root, "app", "layout.js"),
    "export default function Root({ children }) { return children; }\n",
  );
  writeFileSync(
    join(root, "app", "jazz", "route.ts"),
    `import { createJazz, memory } from "jazz-tools/backend";
import { app } from "../../jazz.generated";
import permissions from "../../jazz.permissions.json";

export const dynamic = "force-dynamic";
const jazz = createJazz({ app, permissions, store: memory() });

export async function GET() {
  try {
    await jazz.ready();
    const { value } = jazz.db.insert(app.notes, { body: "traced" });
    return Response.json({ ok: true, body: value.body });
  } catch (error) {
    return Response.json({ ok: false, error: String(error) }, { status: 503 });
  }
}
`,
  );
}

async function get(url, deadline) {
  for (;;) {
    try {
      const response = await fetch(url);
      return { status: response.status, body: await response.json() };
    } catch (error) {
      if (Date.now() > deadline) throw error;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
}

test(
  "a Next.js createJazz route serves from only its traced files (pnpm, Turbopack, standalone)",
  { timeout: 900_000 },
  async () => {
    const root = mkdtempSync(join(tmpdir(), "jazz-next-trace-"));
    const packs = join(root, "packs");
    const project = join(root, "app");
    mkdirSync(packs);
    mkdirSync(project);
    try {
      const tarballs = {
        tools: pack(packageDir, packs),
        napi: pack(join(repo, "crates", "jazz-napi"), packs),
        wasm: pack(join(repo, "crates", "jazz-wasm"), packs),
      };
      consumerApp(project, tarballs);
      try {
        run("pnpm", ["install", "--prefer-offline"], project);
      } catch (error) {
        assert.fail(
          `pnpm install of the consumer app failed:\n${error.stdout ?? ""}${error.stderr ?? error}`,
        );
      }
      run(
        process.execPath,
        [join(project, "node_modules", "next", "dist", "bin", "next"), "build"],
        project,
      );

      // Vercel's file set: the standalone copy, with nothing else to fall back on.
      const deployed = join(root, "deployed");
      cpSync(join(project, ".next", "standalone"), deployed, {
        recursive: true,
        verbatimSymlinks: true,
      });
      renameSync(join(project, "node_modules"), join(project, "node_modules.hidden"));
      const port = 7800 + Math.floor(Math.random() * 100);
      const server = spawn(process.execPath, [join(deployed, "server.js")], {
        cwd: deployed,
        env: { ...consumerEnv, PORT: String(port), HOSTNAME: "127.0.0.1" },
        stdio: "pipe",
      });
      let output = "";
      server.stdout.on("data", (chunk) => (output += chunk));
      server.stderr.on("data", (chunk) => (output += chunk));
      try {
        const { status, body } = await get(`http://127.0.0.1:${port}/jazz`, Date.now() + 60_000);
        assert.equal(
          status,
          200,
          `the route answered ${status} from its traced files: ${JSON.stringify(body)}\n${output}`,
        );
        assert.deepEqual(body, { ok: true, body: "traced" });
      } finally {
        server.kill();
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  },
);
