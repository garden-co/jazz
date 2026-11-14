import { execFile, spawn } from "node:child_process";
import { createServer, type AddressInfo } from "node:net";
import { createRequire } from "node:module";
import { mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import ts from "typescript";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import { promisify } from "node:util";
import { afterAll, expect, it, vi } from "vitest";

// A real Turbopack build (`next build`) of jazz-tools' `ws` loading
// (ws-peer.ts, verbatim) in an app that doesn't install `ws`. Turbopack
// fails the whole build, and in `next dev` every route that imports
// jazz-tools, on an optional import it can't resolve unless the import is
// a literal inside `try` (#5055's E5). Inside this repo `ws` resolves from
// jazz-tools' own devDependencies, so the app lives under the repo root's
// `target/`, where it doesn't; bundler-safe-imports.test.ts checks that
// ws-peer.ts is the only module that imports `ws`.

const here = dirname(fileURLToPath(import.meta.url));
const repo = join(here, "../../../..");
const app = join(repo, "target", `turbopack-no-ws-${randomUUID()}`);
const require = createRequire(import.meta.url);
const packageDir = (name: string) => dirname(require.resolve(`${name}/package.json`));

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as AddressInfo;
      server.close(() => resolve(port));
    });
  });
}

afterAll(() => rmSync(app, { recursive: true, force: true }));

it("builds with Turbopack where ws isn't installed, and fails only at runtime", async () => {
  // pnpm sets NODE_PATH to its hoisted store, where `ws` is: not in the app's.
  const { NODE_PATH: _, ...clean } = process.env;
  const env = { ...clean, NEXT_TELEMETRY_DISABLED: "1", NODE_ENV: "production" };
  mkdirSync(join(app, "node_modules"), { recursive: true });
  const resolves = await promisify(execFile)(
    process.execPath,
    ["-e", "try { require.resolve('ws'); console.log('yes') } catch { console.log('no') }"],
    { cwd: app, env },
  );
  expect(resolves.stdout.trim(), "ws must not resolve in the app").toBe("no");
  for (const name of ["next", "react", "react-dom"]) {
    symlinkSync(packageDir(name), join(app, "node_modules", name), "dir");
  }
  const route = join(app, "app", "ws", "route.js");
  mkdirSync(dirname(route), { recursive: true });
  // As jazz-tools ships them: compiled to JS, import specifiers unchanged.
  mkdirSync(join(app, "lib"));
  for (const file of ["ws-peer", "optional-peer"]) {
    const source = readFileSync(join(here, `${file}.ts`), "utf8");
    const { outputText } = ts.transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
    });
    writeFileSync(join(app, "lib", `${file}.js`), outputText);
  }
  writeFileSync(
    join(app, "package.json"),
    JSON.stringify({ name: "turbopack-no-ws", private: true, type: "module" }),
  );
  writeFileSync(
    join(app, "app", "layout.js"),
    "export default function Layout({ children }) { return children; }\n",
  );
  writeFileSync(
    route,
    [
      'import { importWs } from "../../lib/ws-peer.js";',
      "export const dynamic = 'force-dynamic';",
      "export async function GET() {",
      "  try {",
      '    await importWs("this route");',
      '    return new Response("loaded");',
      "  } catch (error) {",
      "    return new Response(error.message, { status: 501 });",
      "  }",
      "}",
      "",
    ].join("\n"),
  );

  const next = join(packageDir("next"), "dist", "bin", "next");
  const build = await promisify(execFile)(process.execPath, [next, "build", "--turbopack"], {
    cwd: app,
    env,
    maxBuffer: 16 * 1024 * 1024,
  }).catch((error: { stdout?: string; stderr?: string }) => {
    throw new Error(`next build failed:\n${error.stdout}\n${error.stderr}`);
  });
  expect(build.stdout).toMatch(/Turbopack/);

  // The built route loads the module, and says at runtime what's missing.
  const port = await freePort();
  const server = spawn(process.execPath, [next, "start", "-p", String(port)], { cwd: app, env });
  try {
    const response = await vi.waitFor(() => fetch(`http://127.0.0.1:${port}/ws`), {
      timeout: 60_000,
      interval: 250,
    });
    expect(response.status).toBe(501);
    expect(await response.text()).toMatch(/this route needs the ws package: npm install ws/);
  } finally {
    server.kill();
  }
}, 240_000);
