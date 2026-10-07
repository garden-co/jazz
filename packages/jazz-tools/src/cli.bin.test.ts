import { spawnSync } from "node:child_process";
import { constants } from "node:fs";
import {
  access,
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  symlink,
  writeFile,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import { APP_ID_ENV_VARS, SERVER_URL_ENV_VARS } from "./cli.js";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const {
  distIndexPath,
  distCliPath,
  binPath,
  bootstrapVerifierPath,
  tmpBase,
  tempRoots,
  APP_ID,
  createWorkspace,
  fileExists,
  rootSchemaWithoutInlinePermissions,
  rootPermissionsSchema,
  runBin,
  runCli,
  listenForDeployRequest,
  hostNativeBinaryName,
} = createCliFixtures(import.meta.url);

describe("bin integration", () => {
  it.each([
    ["migrations", "push", "app", "old", "new"],
    ["deploy", "app", "--no-verify"],
  ])("rejects removed publication command or bypass: %j", (...args) => {
    const result = runBin(args);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain("deploy");
  });

  it.each([
    ["before command", ["--env-file", ".env.staging", "deploy", "explicit-cli-app"]],
    ["after command", ["deploy", "--env-file", ".env.staging", "explicit-cli-app"]],
    ["equals before command", ["--env-file=.env.staging", "deploy", "explicit-cli-app"]],
  ] as const)("loads an explicit env file and dispatches deploy (%s)", async (_label, args) => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));
    await writeFile(join(root, "permissions.ts"), "export default {};\n");
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
      expect(result.stdout).toContain(`Loaded current schema from ${join(root, "schema.ts")}.`);
      expect(result.stderr).toContain("request=/apps/explicit-cli-app/admin/migrations/graph");
      expect(result.stderr).toContain("secret=real-secret");
      expect(result.stderr).not.toContain("Missing app ID");
      expect(result.stdout).not.toContain("Usage:");
    } finally {
      server.close((error) => (error ? rejectClose(error) : resolveClose()));
      await close;
    }
  });
  it("routes validate through the TypeScript CLI for a root schema.ts project", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    await writeFile(join(root, "permissions.ts"), "export default {};\n");

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(await fileExists(join(root, "schema", "current.sql"))).toBe(false);
    expect(await fileExists(join(root, "schema", "app.ts"))).toBe(false);
  });
  it.each([
    ["validate --schema-dir", ["validate", "--schema-dir"]],
    [
      "validate --schema-dir followed by another flag",
      ["validate", "--schema-dir", "--strict-provenance"],
    ],
    ["validate --schema-dir with an empty value", ["validate", "--schema-dir", ""]],
  ])("rejects %s with a deterministic missing-value error", async (_description, args) => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    // A valid cwd schema proves the parser does not silently fall back to cwd
    // when a recognized value flag is missing.
    const result = runBin(args, { cwd: root });

    expect(result.status).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain("Missing value for --schema-dir.");
  });
  it("rejects a malformed later value after a valid occurrence", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    const result = runBin(["validate", "--schema-dir", root, "--schema-dir"], { cwd: root });

    expect(result.status).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain("Missing value for --schema-dir.");
  });

  it.each(["", "\nexport const permissions = {};\n"])(
    "rejects missing permissions.ts even if schema.ts exports permissions (%j)",
    async (permissionsExport) => {
      const { root } = await createWorkspace();
      await writeFile(
        join(root, "schema.ts"),
        rootSchemaWithoutInlinePermissions(distIndexPath) + permissionsExport,
      );
      const result = runBin(["validate", "--schema-dir", root]);
      expect(result.status).toBe(1);
      expect(result.stderr).toContain("Create a permissions.ts file");
      expect(result.stdout).not.toContain("Validated");
    },
  );

  it("loads root permissions.ts through the validate command", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));
    await writeFile(
      join(root, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distIndexPath),
    );

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(await fileExists(join(root, "permissions.test.ts"))).toBe(false);
  });

  it("loads src/schema.ts and src/permissions.ts through the validate command", async () => {
    const { root } = await createWorkspace();
    const srcDir = join(root, "src");
    await mkdir(srcDir, { recursive: true });
    await writeFile(join(srcDir, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));
    await writeFile(
      join(srcDir, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distIndexPath),
    );

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(0);
    expect(result.stdout).toContain(`Loaded schema from ${join(srcDir, "schema.ts")}.`);
    expect(result.stdout).toContain(
      `Loaded current permissions from ${join(srcDir, "permissions.ts")}.`,
    );
  });

  it("fails when validate is pointed at the legacy ./schema shim directory", async () => {
    const { root, schemaDir } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    const result = runBin(["validate", "--schema-dir", schemaDir]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Schema file not found");
  });

  it("fails when no root schema.ts can be found", async () => {
    const { root } = await createWorkspace();

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Schema file not found");
  });

  it("fails when multiple schema.ts candidate locations are present", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));
    await writeFile(
      join(root, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distIndexPath),
    );
    await mkdir(join(root, "src", "lib"), { recursive: true });
    await writeFile(
      join(root, "src", "lib", "schema.ts"),
      rootSchemaWithoutInlinePermissions(distIndexPath),
    );

    const result = runBin(["validate", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Ambiguous schema location");
  });

  it("rejects the removed build alias with a validate hint", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));

    const result = runBin(["build", "--schema-dir", root]);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("renamed to `jazz-tools validate`");
  });

  it.each(["hash", "export"])("rejects the removed schema %s command", (command) => {
    const result = runBin(["schema", command]);
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("schema compile");
  });

  it("routes schema compile through the TypeScript CLI", async () => {
    const { root } = await createWorkspace();
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions(distIndexPath));
    await writeFile(
      join(root, "permissions.ts"),
      rootPermissionsSchema("./schema.ts", distIndexPath),
    );

    const result = runBin(["schema", "compile", "--schema-dir", root]);

    expect(result.status).toBe(0);
    const exported = JSON.parse(String(result.stdout));
    expect(
      exported.todos.columns.some((column: { name: string }) => column.name === "ownerId"),
    ).toBe(true);
  });

  it.each(["--schema-hash", "--server-url", "--admin-secret", APP_ID])(
    "rejects removed schema compile argument %s",
    (argument) => {
      const result = runBin(["schema", "compile", argument, "unused"]);
      expect(result.status).toBe(1);
      expect(result.stderr).toContain("Only local schema compilation is supported.");
    },
  );

  it("verifies packed runtime bootstrap with a native-only help probe", async () => {
    const hostBinaryName = hostNativeBinaryName();

    if (!hostBinaryName) {
      return;
    }

    const { root } = await createWorkspace();
    const packageRoot = join(root, "package");
    const nativeDir = join(packageRoot, "bin", "native");
    const argsPath = join(root, "captured-args.txt");
    const binaryPath = join(nativeDir, hostBinaryName);

    await mkdir(nativeDir, { recursive: true });
    await copyFile(binPath, join(packageRoot, "bin", "jazz-tools.js"));
    await writeFile(
      binaryPath,
      `#!/bin/sh
printf '%s\n' "$@" > ${JSON.stringify(argsPath)}
exit 0
`,
      "utf8",
    );
    await chmod(binaryPath, 0o644);

    const result = spawnSync(process.execPath, [bootstrapVerifierPath, packageRoot], {
      encoding: "utf8",
      env: process.env,
    });

    expect(result.status).toBe(0);
    expect(await readFile(argsPath, "utf8")).toBe("create\n--help\n");
    await expect(access(binaryPath, constants.X_OK)).resolves.toBeUndefined();
  });

  it("shows the wrapper command surface in --help output", () => {
    const result = runBin(["--help"]);

    expect(result.status).toBe(0);
    expect(result.stdout).toContain("validate");
    expect(result.stdout).toContain("schema compile");
    expect(result.stdout).toContain("deploy");
    expect(result.stdout).not.toContain("migrations push");
    expect(result.stdout).not.toContain("permissions status");
    expect(result.stdout).toContain("server");
    expect(result.stdout).toContain("create");
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
