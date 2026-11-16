import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, symlink, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { APP_ID_ENV_VARS, SERVER_URL_ENV_VARS } from "./cli.js";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const {
  distCliPath,
  tmpBase,
  tempRoots,
  APP_ID,
  createWorkspace,
  fileExists,
  writeAppFiles,
  writeTsApp,
  todosApp,
  emptyPermissions,
  runBin,
  runCli,
  listenForDeployRequest,
} = createCliFixtures(import.meta.url);

describe("bin integration", () => {
  it.each([
    ["migrations", "push", "app", "old", "new"],
    ["deploy", "app"],
    ["deploy", "app", "--no-verify"],
  ])("rejects removed publication command or bypass: %j", (...args) => {
    const result = runBin(args);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain("createJazz");
  });

  it.each([
    ["before command", ["--env-file", ".env.staging", "migrations", "graph", "explicit-cli-app"]],
    ["after command", ["migrations", "graph", "--env-file", ".env.staging", "explicit-cli-app"]],
    [
      "equals before command",
      ["--env-file=.env.staging", "migrations", "graph", "explicit-cli-app"],
    ],
  ] as const)(
    "loads an explicit env file and dispatches migrations graph (%s)",
    async (_label, args) => {
      const { root } = await createWorkspace();
      await writeAppFiles(root, todosApp(), emptyPermissions());
      const { server, url } = await listenForDeployRequest();
      let resolveClose!: () => void;
      let rejectClose!: (reason?: unknown) => void;
      const close = new Promise<void>((resolvePromise, rejectPromise) => {
        resolveClose = resolvePromise;
        rejectClose = rejectPromise;
      });

      await writeFile(
        join(root, ".env.staging"),
        [`JAZZ_SERVER_URL=${url}`, "JAZZ_ADMIN_SECRET=staging-secret", ""].join("\n"),
      );
      const env: NodeJS.ProcessEnv = { ...process.env, JAZZ_ADMIN_SECRET: "real-secret" };
      for (const name of [...APP_ID_ENV_VARS, ...SERVER_URL_ENV_VARS]) {
        delete env[name];
      }

      try {
        const result = await runCli(args, { cwd: root, env });

        expect(result.status).toBe(1);
        expect(result.stderr).toContain("request=/apps/explicit-cli-app/admin/migrations/graph");
        expect(result.stderr).toContain("secret=real-secret");
        expect(result.stderr).not.toContain("Missing app ID");
        expect(result.stdout).not.toContain("Usage:");
      } finally {
        server.close((error) => (error ? rejectClose(error) : resolveClose()));
        await close;
      }
    },
  );
  it("routes generate through the TypeScript CLI for a root schema.ts project", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root);

    const result = runBin(["generate", "--schema-dir", root]);

    expect(result.stderr).toBe("");
    expect(result.status).toBe(0);
    expect(result.stdout).toMatch(
      /Wrote jazz\.app\.json, jazz\.permissions\.json, .*jazz\.generated\.ts/,
    );
    for (const file of ["jazz.app.json", "jazz.permissions.json", "jazz.generated.ts"]) {
      expect(await fileExists(join(root, file))).toBe(true);
    }
    const check = runBin(["generate", "--check", "--schema-dir", root]);
    expect(check.status).toBe(0);
  });
  it.each([
    ["generate --schema-dir", ["generate", "--schema-dir"]],
    [
      "generate --schema-dir followed by another flag",
      ["generate", "--schema-dir", "--strict-provenance"],
    ],
    ["generate --schema-dir with an empty value", ["generate", "--schema-dir", ""]],
  ])("rejects %s with a deterministic missing-value error", async (_description, args) => {
    const { root } = await createWorkspace();
    await writeTsApp(root);

    // A valid cwd schema proves the parser does not silently fall back to cwd
    // when a recognized value flag is missing.
    const result = runBin(args, { cwd: root });

    expect(result.status).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain("Missing value for --schema-dir.");
  });
  it("rejects a malformed later value after a valid occurrence", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root);

    const result = runBin(["generate", "--schema-dir", root, "--schema-dir"], { cwd: root });

    expect(result.status).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain("Missing value for --schema-dir.");
  });

  it("finds src/schema.ts through the generate command", async () => {
    const { root } = await createWorkspace();
    const srcDir = join(root, "src");
    await writeTsApp(srcDir);

    const result = runBin(["generate", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(await fileExists(join(srcDir, "jazz.generated.ts"))).toBe(true);
    expect(await fileExists(join(srcDir, "jazz.app.json"))).toBe(true);
  });

  it("points an alpha.65 app (jazz.app.json, no schema.ts) at upgrade", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root);

    const result = runBin(["generate", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("npx jazz-tools upgrade");
  });

  it("fails when no schema.ts can be found", async () => {
    const { root } = await createWorkspace();

    const result = runBin(["generate", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain(`No schema.ts in ${root} (looked in ., src, src/lib).`);
  });

  it("fails when multiple schema.ts candidate locations are present", async () => {
    const { root } = await createWorkspace();
    await writeTsApp(root);
    await writeTsApp(join(root, "src", "lib"));

    const result = runBin(["generate", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("found schema.ts in both");
  });

  it("points the renamed validate command at generate", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root);

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("renamed to `jazz-tools generate`");
    expect(await fileExists(join(root, "jazz.generated.ts"))).toBe(false);
  });

  it("rejects the removed --strict-provenance flag and points at Jazz's provenance columns", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root);

    const result = runBin(["generate", "--schema-dir", root, "--strict-provenance"]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain(
      "--strict-provenance was removed with schema.ts: jazz.app.json's columns are taken as written. Use Jazz's $createdAt, $createdBy, $updatedAt and $updatedBy instead of columns that duplicate them.",
    );
    expect(await fileExists(join(root, "jazz.generated.ts"))).toBe(false);
  });

  it.each(["--fromHash", "--toHash", "--migrations-dir"])(
    "rejects the removed migrations create flag %s",
    async (flag) => {
      const { root } = await createWorkspace();
      await writeAppFiles(root);

      const result = runBin(["migrations", "create", "--schema-dir", root, flag, "value"]);

      expect(result.status).toBe(1);
      expect(result.stderr).toContain(
        `${flag} was removed: migrations go from the latest snapshot in migrations/snapshots to schema.ts's tables.`,
      );
    },
  );

  it("rejects the removed build alias with a generate hint", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root);

    const result = runBin(["build", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("renamed to `jazz-tools generate`");
  });

  it.each(["hash", "export"])("rejects the removed schema %s command", (command) => {
    const result = runBin(["schema", command]);
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("schema compile");
  });

  it("routes schema compile through the TypeScript CLI", async () => {
    const { root } = await createWorkspace();
    await writeAppFiles(root);

    const result = runBin(["schema", "compile", "--schema-dir", root]);

    expect(result.status).toBe(0);
    const exported = JSON.parse(String(result.stdout)) as {
      schema: {
        current: string;
        schemas: Array<{
          hash: string;
          schema: { tables: Record<string, { columns: Array<{ name: string }> }> };
        }>;
      };
    };
    const current = exported.schema.schemas.find(({ hash }) => hash === exported.schema.current)!;
    expect(current.schema.tables.todos!.columns.some((column) => column.name === "ownerId")).toBe(
      true,
    );
  });

  it.each(["--schema-hash", "--server-url", "--admin-secret", APP_ID])(
    "rejects removed schema compile argument %s",
    (argument) => {
      const result = runBin(["schema", "compile", argument, "unused"]);
      expect(result.status).toBe(1);
      expect(result.stderr).toContain("Only local schema compilation is supported.");
    },
  );

  it("shows the wrapper command surface in --help output", () => {
    const result = runBin(["--help"]);

    expect(result.status).toBe(0);
    expect(result.stdout).toContain("generate");
    expect(result.stdout).not.toMatch(/^\s+validate\b/m);
    expect(result.stdout).toContain("schema compile");
    expect(result.stdout).not.toMatch(/^\s+deploy\b/m);
    expect(result.stdout).not.toContain("migrations push");
    expect(result.stdout).not.toContain("permissions status");
    expect(result.stdout).not.toMatch(/^\s+server\b/m);
    expect(result.stdout).not.toMatch(/^\s+create\b/m);
  });
});

describe("dist/cli.js entrypoint under a pnpm symlink", () => {
  it("dispatches when invoked through a symlinked package directory", async () => {
    await mkdir(tmpBase, { recursive: true });
    const root = await mkdtemp(join(tmpBase, "jazz-tools-pnpm-symlink-"));
    tempRoots.push(root);

    // pnpm symlinks node_modules/jazz-tools to node_modules/.pnpm/.../jazz-tools.
    // Running dist/cli.js through that symlink must still run the CLI body
    // instead of silently exiting 0.
    const realPackageDir = dirname(dirname(distCliPath));
    const linkedPackageDir = join(root, "jazz-tools");
    await symlink(realPackageDir, linkedPackageDir, "dir");

    const result = spawnSync(process.execPath, [join(linkedPackageDir, "dist", "cli.js")], {
      encoding: "utf8",
    });

    expect(result.status).toBe(0);
    expect(result.stdout).toContain(
      "Usage: node <path-to-jazz-tools>/dist/cli.js <command> [options]",
    );
  });
});
