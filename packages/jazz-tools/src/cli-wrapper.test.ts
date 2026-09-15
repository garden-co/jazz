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
  args: string[],
  options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
): Promise<{ status: number | null; stdout: string; stderr: string }> {
  const { promise, resolve } = Promise.withResolvers<{
    status: number | null;
    stdout: string;
    stderr: string;
  }>();
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
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  server.once("error", reject);
  server.listen(0, "127.0.0.1", resolve);
  await promise;
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
    const close = Promise.withResolvers<void>();

    try {
      await writeFile(
        join(root, "schema.ts"),
        `
import { schema as s } from ${JSON.stringify(schemaImportPath)};

const schema = {
  todos: s.table({
    title: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
`,
      );
      await writeFile(
        join(root, ".env.staging"),
        [`JAZZ_SERVER_URL=${url}`, "JAZZ_ADMIN_SECRET=staging-secret", ""].join("\n"),
      );
      const env = { ...process.env, JAZZ_ADMIN_SECRET: "real-secret" };
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
      server.close((error) => (error ? close.reject(error) : close.resolve()));
      await close.promise;
      await rm(root, { recursive: true, force: true });
    }
  });
});
