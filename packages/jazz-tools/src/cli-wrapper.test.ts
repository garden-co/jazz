import { execFileSync, spawn } from "node:child_process";
import { createServer, type Server } from "node:http";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const wrapper = join(packageRoot, "bin", "jazz-tools.js");

async function runWrapper(
  args: readonly string[],
  options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
): Promise<{ status: number | null; stdout: string; stderr: string }> {
  let resolve!: (value: { status: number | null; stdout: string; stderr: string }) => void;
  const promise = new Promise<{ status: number | null; stdout: string; stderr: string }>(
    (resolvePromise) => {
      resolve = resolvePromise;
    },
  );
  const child = spawn(process.execPath, ["--no-warnings", wrapper, ...args], {
    cwd: options.cwd,
    env: options.env ?? process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk: Buffer) => {
    stdout += chunk.toString();
  });
  child.stderr.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  child.on("close", (status) => resolve({ status, stdout, stderr }));
  return promise;
}

async function listenForDeployRequest(): Promise<{ server: Server; url: string }> {
  const server = createServer((request, response) => {
    response.statusCode = 400;
    response.end(
      `request=${request.url} secret=${request.headers["x-jazz-admin-secret"] ?? "<missing>"}`,
    );
  });
  let resolveListening!: () => void;
  let rejectListening!: (reason?: unknown) => void;
  const listening = new Promise<void>((resolvePromise, rejectPromise) => {
    resolveListening = resolvePromise;
    rejectListening = rejectPromise;
  });
  server.once("error", rejectListening);
  server.listen(0, "127.0.0.1", resolveListening);
  await listening;
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Expected deploy test server to have a TCP address.");
  }
  return { server, url: `http://127.0.0.1:${address.port}` };
}

const schemaImportPath = join(packageRoot, "dist", "index.js");

describe("jazz-tools wrapper", () => {
  it.each(["--help", "-h"])("shows deploy help before required arguments (%s)", (flag) => {
    const help = execFileSync(process.execPath, [wrapper, "deploy", flag], { encoding: "utf8" });
    expect(help).toContain("deploy <appId>");
    expect(help).toContain("--server-url");
    expect(help).toContain("--admin-secret");
  });

  it("does not advertise the removed documentation MCP server", () => {
    const help = execFileSync(process.execPath, [wrapper, "--help"], { encoding: "utf8" });

    expect(help).toContain("validate");
    expect(help).not.toMatch(/\bmcp\b/i);
  });
  it.each([
    ["before command", ["--env-file", ".env.staging", "deploy", "explicit-wrapper-app"]],
    ["after command", ["deploy", "--env-file", ".env.staging", "explicit-wrapper-app"]],
    ["equals before command", ["--env-file=.env.staging", "deploy", "explicit-wrapper-app"]],
  ] as const)("loads an explicit env file and routes deploy (%s)", async (_label, args) => {
    const root = await mkdtemp(join(tmpdir(), "jazz-tools-wrapper-env-file-"));
    const { server, url } = await listenForDeployRequest();
    let resolveClose!: () => void;
    let rejectClose!: (reason?: unknown) => void;
    const close = new Promise<void>((resolvePromise, rejectPromise) => {
      resolveClose = resolvePromise;
      rejectClose = rejectPromise;
    });

    try {
      await writeFile(
        join(root, "schema.ts"),
        `
import { schema as s } from ${JSON.stringify(schemaImportPath)};

const schema = {
  todos: s.table({
    title: s.string(),
  }, {  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`,
      );
      await writeFile(
        join(root, "permissions.ts"),
        `
import { schema as s } from ${JSON.stringify(schemaImportPath)};
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.always();
  policy.todos.allowInsert.always();
});
`,
      );
      await writeFile(
        join(root, ".env.staging"),
        [`JAZZ_SERVER_URL=${url}`, "JAZZ_ADMIN_SECRET=staging-secret", ""].join("\n"),
      );
      const env: NodeJS.ProcessEnv = { ...process.env, JAZZ_ADMIN_SECRET: "real-secret" };
      delete env.JAZZ_APP_ID;
      delete env.JAZZ_SERVER_URL;

      const result = await runWrapper(args, { cwd: root, env });

      expect(result.status).toBe(1);
      expect(result.stdout).toContain(`Loaded current schema from ${join(root, "schema.ts")}.`);
      expect(result.stderr).toContain("request=/apps/explicit-wrapper-app/schemas");
      expect(result.stderr).toContain("secret=real-secret");
      expect(result.stderr).not.toContain("Missing app ID");
      expect(result.stdout).not.toContain("Jazz distributed database CLI");
    } finally {
      server.close((error) => (error ? rejectClose(error) : resolveClose()));
      await close;
      await rm(root, { recursive: true, force: true });
    }
  });

  it.each([
    ["--rust-bin followed by --help", ["--rust-bin", "--help"]],
    ["--rust-bin with no following value", ["--rust-bin"]],
    ["--rust-bin with an empty value", ["--rust-bin", ""]],
  ])("rejects %s before routing to native Rust", async (_description, args) => {
    const result = await runWrapper(args);

    expect(result.status).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain("Missing value for --rust-bin.");
  });
});

it("routes inspect to the TypeScript session handoff without exposing root credentials", async () => {
  const result = await runWrapper(["--rust-bin", "/does-not-exist", "inspect", "sample-app"], {
    env: {
      ...process.env,
      JAZZ_ADMIN_SECRET: "synthetic-root-not-for-output",
      JAZZ_INSPECTOR_URL: "",
    },
  });
  expect(result.status).toBe(1);
  expect(result.stderr).toContain("Inspect requires");
  expect(result.stderr).not.toContain("does-not-exist");
  expect(result.stderr + result.stdout).not.toContain("synthetic-root-not-for-output");
});
