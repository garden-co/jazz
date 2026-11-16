// The Rust CLI as a backend client of StagePlan's TypeScript main backend:
// builds the `stage-plan` binary, starts a stock `createJazz` that trusts
// the CLI's machine token (../main-backend.ts), and runs
// `stage-plan gate` against it, checking each JSON line it prints.
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { SCOPED_SUB, SCOPED_USER, startMainBackend, type MainBackend } from "../main-backend.ts";
import { cargoBin, examples } from "./cargo.ts";

interface Step {
  step: string;
  mode: string;
  ok: boolean;
  [field: string]: unknown;
}

function gate(env: Record<string, string>) {
  const run = spawnSync(cli, ["gate"], {
    encoding: "utf8",
    env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? "warn", ...env },
    timeout: 120_000,
  });
  const steps = run.stdout
    .split("\n")
    .filter((line) => line.startsWith("{"))
    .map((line) => JSON.parse(line) as Step);
  return { status: run.status, steps, stderr: run.stderr };
}

let cli: string;
let backend: MainBackend;
let tokenDir: string;

beforeAll(async () => {
  cli = cargoBin("stage-plan-rust-cli", "stage-plan");
  backend = await startMainBackend(join(examples, "stage-plan/apps/rust-cli"));
  tokenDir = mkdtempSync(join(tmpdir(), "stage-plan-gate-"));
}, 900_000);

afterAll(async () => {
  await backend?.close();
});

describe("stage-plan gate, scoped", () => {
  it("writes the user's show and task, and a fresh client reads them back", async () => {
    // The credential as a file, read again for every token the client needs.
    const tokenFile = join(tokenDir, "scoped");
    writeFileSync(tokenFile, backend.mint({ sub: SCOPED_SUB }));
    const { status, steps, stderr } = gate({
      JAZZ_SERVER_URL: backend.serverUrl,
      JAZZ_APP_ID: backend.appId,
      JAZZ_TOKEN_FILE: tokenFile,
      STAGE_PLAN_MODE: "scoped",
    });
    expect(steps.map((s) => [s.step, s.ok]), stderr).toEqual([
      ["open", true],
      ["write", true],
      ["read", true],
    ]);
    expect(status).toBe(0);
    const [open, write, read] = steps;
    expect(open).toMatchObject({ mode: "scoped", user: SCOPED_USER });
    expect(write).toMatchObject({ user: SCOPED_USER });
    expect(read).toMatchObject({ show: write!.show, task: write!.task });
    expect(read!.tasks).toContain(write!.task);

    // The main backend has the rows, with the CLI's user as the show's chief.
    const show = await backend.jazz.db.one(backend.app.shows.where({ id: write!.show as string }));
    expect(show?.chiefAccount).toBe(open!.account);
    const tasks = await backend.jazz.db.all(backend.app.tasks.where({ showId: write!.show as string }));
    expect(tasks.map((t) => t.id)).toEqual([write!.task]);
  });

  it("is refused with a token the main backend doesn't trust, and exits non-zero", () => {
    const { status, steps } = gate({
      JAZZ_SERVER_URL: backend.serverUrl,
      JAZZ_APP_ID: backend.appId,
      JAZZ_TOKEN: backend.mint({ sub: "intruder" }),
    });
    expect(steps).toEqual([expect.objectContaining({ step: "open", ok: false })]);
    expect(status).not.toBe(0);
  });
});
