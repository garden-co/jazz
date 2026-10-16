import { spawn, spawnSync } from "node:child_process";
import {
  access,
  copyFile,
  mkdir,
  readFile,
  readdir,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import { hostname } from "node:os";
import { vi } from "vitest";
import {
  createMigration as createCatalogueMigration,
  compileSchema as saveBaseline,
} from "./dev/catalogue-project.js";
import { describe, expect, it } from "vitest";
import { createCliFixtures } from "../tests/cli/fixtures.js";

const {
  distCliPath,
  createWorkspace,
  fileExists,
  spawnMigrationCreate,
  migrationLockLeftovers,
  waitForCrashMarker,
  waitForMarkers,
  killChild,
  rootSchemaWithoutInlinePermissions,
  rootSchemaWithTodoNotes,
} = createCliFixtures(import.meta.url);

describe("cli migrations", () => {
  it("serializes simultaneous migration generation against one directory", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await saveBaseline({ schemaDir: root, migrationsDir });
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());
    await mkdir(migrationsDir, { recursive: true });
    const externalLock = join(migrationsDir, ".jazz-create-migration.lock");
    await mkdir(externalLock);
    await writeFile(
      join(externalLock, "owner-00000000-0000-4000-8000-000000000001.json"),
      `${JSON.stringify({ version: 1, pid: process.pid, hostname: hostname(), token: "00000000-0000-4000-8000-000000000001" })}\n`,
    );

    const contentionMarkers = Array.from({ length: 4 }, (_, index) =>
      join(root, `migration-contention-${index}.marker`),
    );
    const resultsPromise = Promise.all(
      Array.from(
        { length: contentionMarkers.length },
        (_, index) =>
          new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve) => {
            const child = spawn(
              process.execPath,
              [
                distCliPath,
                "migrations",
                "create",
                "--schema-dir",
                root,
                "--migrations-dir",
                migrationsDir,
              ],
              {
                env: {
                  ...process.env,
                  NODE_ENV: "test",
                  JAZZ_TEST_MIGRATION_LOCK_CONTENTION_MARKER: contentionMarkers[index],
                },
              },
            );
            let stdout = "";
            let stderr = "";
            child.stdout.setEncoding("utf8").on("data", (chunk) => (stdout += chunk));
            child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
            child.on("close", (code) => resolve({ code, stdout, stderr }));
          }),
      ),
    );

    // The lock is a filesystem boundary, not an in-process mutex: independently
    // launched CLI processes must all observe the external owner before it is released.
    await waitForMarkers(contentionMarkers);
    expect((await readdir(migrationsDir)).filter((name) => name.endsWith(".ts"))).toEqual([]);
    await rm(externalLock, { recursive: true });
    const results = await resultsPromise;

    expect(
      results.map((result) => result.code),
      JSON.stringify(results, null, 2),
    ).toEqual([0, 0, 0, 0]);
    expect(results.filter((result) => result.stdout.includes("Generated:"))).toHaveLength(1);
    expect(results.filter((result) => result.stdout.includes("No schema changes"))).toHaveLength(3);
    expect(
      (await readdir(join(migrationsDir, "snapshots"))).filter((name) => name.endsWith(".json")),
    ).toHaveLength(2);
    await expect(access(join(migrationsDir, ".jazz-create-migration.lock"))).rejects.toThrow();
  });

  it("recovers a lock whose same-host owner was killed", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const marker = join(root, "lock-held.marker");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await saveBaseline({ schemaDir: root, migrationsDir });
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());

    const child = spawnMigrationCreate(root, migrationsDir, "lock-held", marker);
    await waitForCrashMarker(marker, child);
    await killChild(child);

    const results = await Promise.all([
      createCatalogueMigration({ schemaDir: root, migrationsDir }),
      createCatalogueMigration({ schemaDir: root, migrationsDir }),
    ]);
    expect(results.map((result) => result.status).sort()).toEqual(["generated", "unchanged"]);
    expect(
      (await readdir(join(migrationsDir, "snapshots"))).filter((name) => name.endsWith(".json")),
    ).toHaveLength(2);
    await expect(access(join(migrationsDir, ".jazz-create-migration.lock"))).rejects.toThrow();
  });

  it("recovers when a stale lock quarantiner is killed", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const lockDir = join(migrationsDir, ".jazz-create-migration.lock");
    const marker = join(root, "lock-recovered.marker");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await saveBaseline({ schemaDir: root, migrationsDir });
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());
    await mkdir(lockDir, { recursive: true });
    await writeFile(
      join(lockDir, "owner-00000000-0000-4000-8000-000000000002.json"),
      `${JSON.stringify({ version: 1, pid: 2_147_483_647, hostname: hostname(), token: "00000000-0000-4000-8000-000000000002" })}\n`,
    );

    const child = spawnMigrationCreate(root, migrationsDir, "lock-recovered", marker);
    await waitForCrashMarker(marker, child);
    await killChild(child);

    const result = await createCatalogueMigration({ schemaDir: root, migrationsDir });
    expect(result.status).toBe("generated");
    expect(await fileExists(lockDir)).toBe(false);
  });

  it("never quarantines a live lock that replaced the stale owner it observed", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const lockDir = join(migrationsDir, ".jazz-create-migration.lock");
    const marker = join(root, "lock-observed.marker");
    const releaseMarker = join(root, "lock-observed.release");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await saveBaseline({ schemaDir: root, migrationsDir });
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());
    await mkdir(lockDir, { recursive: true });
    await writeFile(
      join(lockDir, "owner-00000000-0000-4000-8000-000000000004.json"),
      `${JSON.stringify({ version: 1, pid: 2_147_483_647, hostname: hostname(), token: "00000000-0000-4000-8000-000000000004" })}\n`,
    );

    const child = spawnMigrationCreate(root, migrationsDir, "lock-observed", marker, releaseMarker);
    const exited = new Promise<number | null>((resolve) => child.once("close", resolve));
    try {
      // The recoverer has read the dead owner but not yet acted on it. Now the
      // stale lock is released and a live generator acquires a fresh one.
      await waitForCrashMarker(marker, child);
      await rm(lockDir, { recursive: true });
      const liveOwnerPath = join(lockDir, "owner-00000000-0000-4000-8000-000000000005.json");
      const liveOwner = `${JSON.stringify({ version: 1, pid: process.pid, hostname: hostname(), token: "00000000-0000-4000-8000-000000000005" })}\n`;
      await mkdir(lockDir);
      await writeFile(liveOwnerPath, liveOwner);
      await writeFile(releaseMarker, "release");

      // The recoverer must keep waiting on the live lock instead of stealing it.
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(child.exitCode).toBeNull();
      expect(await readFile(liveOwnerPath, "utf8")).toBe(liveOwner);
      expect((await readdir(migrationsDir)).filter((name) => name.includes(".recovery-"))).toEqual(
        [],
      );

      await rm(lockDir, { recursive: true });
      expect(await exited).toBe(0);
    } finally {
      await killChild(child);
    }
    expect(await migrationLockLeftovers(migrationsDir)).toEqual([]);
    expect(
      (await readdir(join(migrationsDir, "snapshots"))).filter((name) => name.endsWith(".json")),
    ).toHaveLength(2);
  });

  it("releases its lock without leaving a lock directory or claimed records behind", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await saveBaseline({ schemaDir: root, migrationsDir });
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());

    expect((await createCatalogueMigration({ schemaDir: root, migrationsDir })).status).toBe(
      "generated",
    );
    expect(await migrationLockLeftovers(migrationsDir)).toEqual([]);
    expect((await createCatalogueMigration({ schemaDir: root, migrationsDir })).status).toBe(
      "unchanged",
    );
    expect(await migrationLockLeftovers(migrationsDir)).toEqual([]);
  });

  it("leaves a recoverable lock when removing the released lock directory fails", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const lockDir = join(migrationsDir, ".jazz-create-migration.lock");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await saveBaseline({ schemaDir: root, migrationsDir });
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());

    const child = spawn(
      process.execPath,
      [
        distCliPath,
        "migrations",
        "create",
        "--schema-dir",
        root,
        "--migrations-dir",
        migrationsDir,
      ],
      {
        env: { ...process.env, NODE_ENV: "test", JAZZ_TEST_MIGRATION_FAIL_AT: "release-rmdir" },
        stdio: ["ignore", "ignore", "pipe"],
      },
    );
    let stderr = "";
    child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
    const code = await new Promise<number | null>((resolve) => child.once("close", resolve));

    // The generation itself succeeded; only the lock directory stayed behind,
    // still attributed to the now-dead child rather than ownerless.
    expect(code, stderr).toBe(0);
    expect(stderr).toContain(`Could not remove migration lock ${lockDir}`);
    expect(await migrationLockLeftovers(migrationsDir)).toEqual([".jazz-create-migration.lock"]);
    const [record, ...extra] = await readdir(lockDir);
    expect(extra).toEqual([]);
    expect(record).toMatch(/^owner-[0-9a-f-]{36}\.json$/);
    expect(JSON.parse(await readFile(join(lockDir, record!), "utf8")).pid).toBe(child.pid);

    expect((await createCatalogueMigration({ schemaDir: root, migrationsDir })).status).toBe(
      "unchanged",
    );
    expect(await migrationLockLeftovers(migrationsDir)).toEqual([]);
  });

  it("names the live owner and how to clear a stuck lock when waiting times out", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const lockDir = join(migrationsDir, ".jazz-create-migration.lock");
    const token = "00000000-0000-4000-8000-000000000006";
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await mkdir(lockDir, { recursive: true });
    await writeFile(
      join(lockDir, `owner-${token}.json`),
      `${JSON.stringify({ version: 1, pid: process.pid, hostname: hostname(), token })}\n`,
    );

    vi.stubEnv("JAZZ_TEST_MIGRATION_LOCK_TIMEOUT_MS", "200");
    try {
      await expect(createCatalogueMigration({ schemaDir: root, migrationsDir })).rejects.toThrow(
        `Timed out waiting for another migration generator to release ${lockDir}; it is held by pid ${process.pid} on host ${hostname()}. If no \`jazz-tools migrations create\` is running for this directory, delete ${lockDir} and retry.`,
      );
    } finally {
      vi.unstubAllEnvs();
    }
    expect(await readdir(lockDir)).toEqual([`owner-${token}.json`]);
    expect(await fileExists(join(migrationsDir, "snapshots"))).toBe(false);
  });

  it.each([false, true])(
    "fails closed for an unknown lock owner (nonempty=%s)",
    async (nonempty) => {
      const { root } = await createWorkspace();
      const migrationsDir = join(root, "migrations");
      const lockDir = join(migrationsDir, ".jazz-create-migration.lock");
      await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
      await mkdir(lockDir, { recursive: true });
      if (nonempty) await writeFile(join(lockDir, "unknown"), "unknown\n");

      await expect(createCatalogueMigration({ schemaDir: root, migrationsDir })).rejects.toThrow(
        `owner metadata is missing, invalid, or unsafe. If no \`jazz-tools migrations create\` is running for this directory, delete ${lockDir} and retry.`,
      );
      expect(await fileExists(lockDir)).toBe(true);
      expect(await fileExists(join(migrationsDir, "snapshots"))).toBe(false);
    },
  );

  it("fails closed for malformed dead-owner metadata", async () => {
    const deadPid = 2_147_483_647;
    const validToken = "00000000-0000-4000-8000-000000000003";
    const malformedOwners = [
      { version: 1, pid: deadPid, hostname: hostname(), token: null },
      { version: 1, pid: deadPid, hostname: hostname(), token: "not-a-uuid" },
      { version: 1, pid: deadPid, hostname: hostname() },
      { version: 1, pid: deadPid, hostname: hostname(), token: validToken, extra: true },
      { version: 1, pid: "2147483647", hostname: hostname(), token: validToken },
      { version: 1, pid: deadPid, hostname: "", token: validToken },
      { version: 2, pid: deadPid, hostname: hostname(), token: validToken },
      // A record whose content names a different lock instance than its file.
      {
        version: 1,
        pid: deadPid,
        hostname: hostname(),
        token: "00000000-0000-4000-8000-000000000009",
      },
    ];
    const validOwner = { version: 1, pid: deadPid, hostname: hostname(), token: validToken };
    const malformedRecords = [
      ...malformedOwners.map((owner) => ({ name: `owner-${validToken}.json`, owner })),
      // Records that are not the single token-named owner record of a lock.
      { name: "owner.json", owner: validOwner },
      { name: `owner-${validToken.toUpperCase()}-extra.json`, owner: validOwner },
    ];

    await Promise.all(
      malformedRecords.map(async ({ name, owner: malformedOwner }) => {
        const { root } = await createWorkspace();
        const migrationsDir = join(root, "migrations");
        const lockDir = join(migrationsDir, ".jazz-create-migration.lock");
        const ownerText = `${JSON.stringify(malformedOwner)}\n`;
        await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
        await mkdir(lockDir, { recursive: true });
        await writeFile(join(lockDir, name), ownerText);

        const failure = createCatalogueMigration({ schemaDir: root, migrationsDir });
        await expect(failure).rejects.toThrow("owner metadata is missing, invalid, or unsafe");
        await expect(failure).rejects.toThrow(`delete ${lockDir} and retry`);
        if (name === "owner.json") {
          await expect(failure).rejects.toThrow("owner.json left by an older jazz-tools");
        } else {
          await expect(failure).rejects.not.toThrow("older jazz-tools");
        }
        expect(await readFile(join(lockDir, name), "utf8")).toBe(ownerText);
        expect(await fileExists(join(migrationsDir, "snapshots"))).toBe(false);
      }),
    );
  });

  it("recovers a killed migration between publishing its paired outputs", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const marker = join(root, "between-publications.marker");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
    await saveBaseline({ schemaDir: root, migrationsDir });
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());

    const child = spawnMigrationCreate(root, migrationsDir, "between-publications", marker);
    await waitForCrashMarker(marker, child);
    await killChild(child);

    expect((await readdir(migrationsDir)).filter((name) => name.endsWith(".ts"))).toHaveLength(1);
    expect(await fileExists(join(migrationsDir, ".jazz-create-migration.journal.json"))).toBe(true);

    const retry = await createCatalogueMigration({ schemaDir: root, migrationsDir });
    expect(retry.status).toBe("unchanged");
    expect((await readdir(migrationsDir)).filter((name) => name.endsWith(".ts"))).toHaveLength(1);
    expect(
      (await readdir(join(migrationsDir, "snapshots"))).filter((name) => name.endsWith(".json")),
    ).toHaveLength(2);
    expect(await fileExists(join(migrationsDir, ".jazz-create-migration.journal.json"))).toBe(
      false,
    );
    expect(await fileExists(join(migrationsDir, ".jazz-create-migration-stage"))).toBe(false);
    expect(await fileExists(join(migrationsDir, ".jazz-create-migration.lock"))).toBe(false);
  });

  it.each(["contents", "symlink"])(
    "rejects a %s-tampered published prefix even when its staged copy is gone",
    async (tamperKind) => {
      const { root } = await createWorkspace();
      const migrationsDir = join(root, "migrations");
      const marker = join(root, "between-publications.marker");
      await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
      await saveBaseline({ schemaDir: root, migrationsDir });
      await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());
      const child = spawnMigrationCreate(root, migrationsDir, "between-publications", marker);
      await waitForCrashMarker(marker, child);
      await killChild(child);
      const migration = (await readdir(migrationsDir)).find((name) => name.endsWith(".ts"));
      expect(migration).toBeDefined();
      const migrationPath = join(migrationsDir, migration!);
      if (tamperKind === "contents") {
        await writeFile(migrationPath, "tampered\n");
      } else {
        const outside = join(root, "outside-migration.ts");
        await writeFile(outside, "tampered\n");
        await rm(migrationPath);
        await symlink(outside, migrationPath, "file");
      }

      await expect(createCatalogueMigration({ schemaDir: root, migrationsDir })).rejects.toThrow(
        tamperKind === "contents"
          ? "does not match its journal"
          : "must not contain a symlink or junction",
      );
      expect(await fileExists(join(migrationsDir, ".jazz-create-migration.journal.json"))).toBe(
        true,
      );
    },
  );

  it.each(["snapshots", ".jazz-create-migration-stage"])(
    "rejects a symlinked migration publication path: %s",
    async (linkedName) => {
      const { root } = await createWorkspace();
      const migrationsDir = join(root, "migrations");
      const outside = join(root, "outside");
      await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
      await mkdir(migrationsDir);
      await mkdir(outside);
      await symlink(outside, join(migrationsDir, linkedName), "dir");

      await expect(createCatalogueMigration({ schemaDir: root, migrationsDir })).rejects.toThrow(
        "must not contain a symlink or junction",
      );
      expect(await readdir(outside)).toEqual([]);
    },
  );

  it("does not overwrite a destination created after publication is journaled", async () => {
    const { root } = await createWorkspace();
    const migrationsDir = join(root, "migrations");
    const marker = join(root, "journaled.marker");
    const releaseMarker = join(root, "journaled.release");
    await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());

    await saveBaseline({ schemaDir: root, migrationsDir });
    await writeFile(join(root, "schema.ts"), rootSchemaWithTodoNotes());

    const child = spawnMigrationCreate(root, migrationsDir, "journaled", marker, releaseMarker);
    await waitForCrashMarker(marker, child);
    const journal = JSON.parse(
      await readFile(join(migrationsDir, ".jazz-create-migration.journal.json"), "utf8"),
    ) as { files: Array<{ finalRelativePath: string }> };
    const destination = join(migrationsDir, journal.files[0]!.finalRelativePath);
    await mkdir(dirname(destination), { recursive: true });
    await writeFile(destination, "concurrent writer\n");
    await writeFile(releaseMarker, "release\n");
    const code = await new Promise<number | null>((resolve) => child.once("close", resolve));

    expect(code).not.toBe(0);
    expect(await readFile(destination, "utf8")).toBe("concurrent writer\n");
  });

  it.each(["outside", "in-tree"])(
    "rejects a %s symlinked committed snapshot baseline in the CLI process",
    async (targetKind) => {
      const { root } = await createWorkspace();
      const migrationsDir = join(root, "migrations");
      const snapshotsDir = join(migrationsDir, "snapshots");
      await writeFile(join(root, "schema.ts"), rootSchemaWithoutInlinePermissions());
      await saveBaseline({ schemaDir: root, migrationsDir });
      const snapshot = (await readdir(snapshotsDir)).find((name) => name.endsWith(".json"));
      expect(snapshot).toBeDefined();
      const snapshotPath = join(snapshotsDir, snapshot!);
      const target =
        targetKind === "outside"
          ? join(root, "outside-snapshot.json")
          : join(migrationsDir, "in-tree-snapshot.json");
      await copyFile(snapshotPath, target);
      await rm(snapshotPath);
      await symlink(target, snapshotPath, "file");

      const result = spawnSync(
        process.execPath,
        [
          distCliPath,
          "migrations",
          "create",
          "--schema-dir",
          root,
          "--migrations-dir",
          migrationsDir,
        ],
        { encoding: "utf8" },
      );

      expect(result.status).not.toBe(0);
      expect(`${result.stdout}\n${result.stderr}`).toContain(
        "Migration path must not contain a symlink or junction",
      );
    },
  );
});
