// The StagePlan Rust CLI as a backend client of a TypeScript main backend:
// builds the `stage-plan` binary, starts a stock `createJazz` serving
// StagePlan that trusts the CLI's machine tokens (../main-backend.ts), and
// runs `stage-plan gate` against it, scoped and trusted, checking each JSON
// line it prints.
import { execFile } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  SCOPED_SUB,
  SCOPED_USER,
  TRUSTED_SUB,
  USER_A,
  USER_B,
  startMainBackend,
  type MainBackend,
} from "../main-backend.ts";
import { cargoBin, examples } from "./cargo.ts";

interface Step {
  step: string;
  mode: string;
  ok: boolean;
  [field: string]: unknown;
}

/**
 * Runs `stage-plan gate`. Asynchronously: the main backend serves from this
 * process, so a blocking spawn would stall it.
 */
function gate(env: Record<string, string>) {
  return new Promise<{ status: number | null; steps: Step[]; stderr: string }>((resolve) => {
    execFile(
      cli,
      ["gate"],
      {
        encoding: "utf8",
        env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? "warn", ...env },
        timeout: 120_000,
      },
      (error, stdout, stderr) => {
        const steps = stdout
          .split("\n")
          .filter((line) => line.startsWith("{"))
          .map((line) => JSON.parse(line) as Step);
        const status = error ? (typeof error.code === "number" ? error.code : -1) : 0;
        resolve({ status, steps, stderr });
      },
    );
  });
}

let cli: string;
let backend: MainBackend;
let tokenDir: string;
let target: Record<string, string>;

beforeAll(async () => {
  cli = cargoBin("stage-plan-rust-cli", "stage-plan");
  backend = await startMainBackend(join(examples, "stage-plan/apps/rust-cli"));
  tokenDir = mkdtempSync(join(tmpdir(), "stage-plan-gate-"));
  target = { JAZZ_SERVER_URL: backend.serverUrl, JAZZ_APP_ID: backend.appId };
}, 900_000);

afterAll(async () => {
  await backend?.close();
});

/** A show of USER_A's, written by the main backend itself. */
async function showOfA(): Promise<string> {
  const written = backend.jazz.db.insert(backend.app.shows, {
    name: "A's private show",
    venue: "Somewhere",
    date: "2026-06-01",
    doors: "20:00",
    chiefAccount: backend.accounts.a,
  });
  return written.value.id;
}

describe("stage-plan gate, scoped", () => {
  it("writes the user's show and task, reads them back, and is kept out of another user's show", async () => {
    // The credential as a file, read again for every token the client needs.
    const tokenFile = join(tokenDir, "scoped");
    writeFileSync(tokenFile, backend.mint({ sub: SCOPED_SUB }));
    const foreign = await showOfA();
    const { status, steps, stderr } = await gate({
      ...target,
      JAZZ_TOKEN_FILE: tokenFile,
      STAGE_PLAN_MODE: "scoped",
      STAGE_PLAN_FOREIGN_SHOW: foreign,
    });
    expect(
      steps.map((s) => [s.step, s.ok]),
      `${JSON.stringify(steps)}\n${stderr}`,
    ).toEqual([
      ["open", true],
      ["write", true],
      ["read", true],
      ["foreign_read", true],
      ["foreign_write", true],
    ]);
    expect(status).toBe(0);
    const [open, write, read, foreignRead, foreignWrite] = steps;
    expect(open).toMatchObject({ mode: "scoped", kind: "scoped", user: SCOPED_USER });
    expect(write).toMatchObject({ user: SCOPED_USER });
    expect(read).toMatchObject({ show: write!.show, task: write!.task, visible: true });
    expect(read!.tasks).toContain(write!.task);
    expect(foreignRead).toMatchObject({ show: foreign, visible: false, tasks: [] });
    expect(foreignWrite).toMatchObject({ show: foreign, refused: true });

    // The main backend has the rows, with the CLI's user as the show's chief.
    const [show] = await backend.jazz.db.all(
      backend.app.shows.where({ id: write!.show as string }),
    );
    expect(show?.chiefAccount).toBe(open!.account);
    const tasks = await backend.jazz.db.all(
      backend.app.tasks.where({ showId: write!.show as string }),
    );
    expect(tasks.map((t) => t.id)).toEqual([write!.task]);
    // And nothing landed in A's show.
    expect(await backend.jazz.db.all(backend.app.tasks.where({ showId: foreign }))).toEqual([]);
  });

  it("is refused with a token the main backend doesn't trust, and exits non-zero", async () => {
    const { status, steps } = await gate({
      ...target,
      JAZZ_TOKEN: backend.mint({ sub: "intruder" }),
    });
    expect(steps).toEqual([expect.objectContaining({ step: "open", ok: false })]);
    expect(status).not.toBe(0);
  });
});

// TODO(.65 fallback): trusted mode needs jazz-app's trusted backend client
// (Config::credential, Jazz::for_user, jazz#5798), which this branch doesn't
// have. Unskip when it lands.
describe.skip("stage-plan gate, trusted", () => {
  it("writes and reads as the system, then for A, and B can't see or add to A's show", async () => {
    const { status, steps, stderr } = await gate({
      ...target,
      // The credential as a command, run for every token the client needs.
      JAZZ_TOKEN_COMMAND: `printf %s '${backend.mint({ sub: TRUSTED_SUB })}'`,
      STAGE_PLAN_MODE: "trusted",
      STAGE_PLAN_USER_A: USER_A,
      STAGE_PLAN_USER_B: USER_B,
    });
    const aShowId = steps.find((s) => s.step === "for_user_write")?.show as string | undefined;
    const onMain = aShowId
      ? {
          shows: await backend.jazz.db.all(backend.app.shows.where({ id: aShowId })),
          tasks: await backend.jazz.db.all(backend.app.tasks.where({ showId: aShowId })),
          accounts: backend.accounts,
        }
      : undefined;
    expect(
      steps.map((s) => [s.step, s.ok, s.step.startsWith("for_user") ? s.user : null]),
      `${JSON.stringify(steps)}\non the main backend: ${JSON.stringify(onMain)}\n${stderr}`,
    ).toEqual([
      ["open", true, null],
      ["write", true, null],
      ["read", true, null],
      ["for_user_write", true, USER_A],
      ["for_user_read", true, USER_A],
      ["for_user_read", true, USER_B],
      ["for_user_write", true, USER_B],
    ]);
    expect(status).toBe(0);
    const [open, write, read, aWrite, aRead, bRead, bWrite] = steps;
    expect(open).toMatchObject({ mode: "trusted", kind: "trusted" });
    expect(read).toMatchObject({ show: write!.show, task: write!.task, visible: true });
    expect(aWrite).toMatchObject({ account: backend.accounts.a });
    expect(aRead).toMatchObject({ show: aWrite!.show, visible: true });
    expect(aRead!.tasks).toEqual([aWrite!.task]);
    expect(bRead).toMatchObject({ show: aWrite!.show, visible: false, tasks: [] });
    expect(bWrite).toMatchObject({ show: aWrite!.show, refused: true });

    // The main backend sees A's show with A as chief, holding only A's task.
    const [aShow] = await backend.jazz.db.all(
      backend.app.shows.where({ id: aWrite!.show as string }),
    );
    expect(aShow?.chiefAccount).toBe(backend.accounts.a);
    const aTasks = await backend.jazz.db.all(
      backend.app.tasks.where({ showId: aWrite!.show as string }),
    );
    expect(aTasks.map((t) => t.id)).toEqual([aWrite!.task]);
  });

  it("fails for_user for a user with no account, and exits non-zero", async () => {
    const { status, steps } = await gate({
      ...target,
      JAZZ_TOKEN: backend.mint({ sub: TRUSTED_SUB }),
      STAGE_PLAN_MODE: "trusted",
      STAGE_PLAN_USER_A: "nobody-yet",
      STAGE_PLAN_USER_B: USER_B,
    });
    expect(steps.at(-1)).toMatchObject({ step: "for_user_write", ok: false, user: "nobody-yet" });
    expect(status).not.toBe(0);
  });
});
