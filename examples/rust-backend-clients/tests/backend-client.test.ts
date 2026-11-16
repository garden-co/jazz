// jazz-app-support's backend-client contract against a stock TypeScript `createJazz`
// (../main-backend.ts, on a local core server so identities can be
// revoked), through the `rust-backend-client-probe` binary: what the
// stage-plan gate does not cover. A trusted client acting for a user with no
// account gets `NoAccount`, for one whose identity the core refuses a 403
// (`Refused`), and closes cleanly; a scoped one can't act for users; a
// credential the main backend doesn't trust is refused.
import { execFile } from "node:child_process";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  SCOPED_SUB,
  TRUSTED_SUB,
  USER_A,
  startMainBackend,
  type MainBackend,
} from "../main-backend.ts";
import { cargoBin, examples } from "./cargo.ts";

interface Step {
  step: string;
  ok: boolean;
  detail: string;
}

const APP_DIR = join(examples, "todo-server-rs");
const REVOKED = "user-revoked";
const NO_ACCOUNT = "user-without-account";

let probe: string;
let backend: MainBackend;

/** Runs the probe in `mode`, asynchronously: the main backend serves from this process. */
function run(mode: string, token: string) {
  return new Promise<{ status: number | null; steps: Step[]; stderr: string }>((resolve) => {
    execFile(
      probe,
      [mode],
      {
        encoding: "utf8",
        env: {
          ...process.env,
          RUST_LOG: process.env.RUST_LOG ?? "warn",
          JAZZ_SERVER_URL: backend.serverUrl,
          JAZZ_APP_ID: backend.appId,
          JAZZ_TOKEN: token,
          JAZZ_APP_DIR: APP_DIR,
          PROBE_USER: USER_A,
          PROBE_NO_ACCOUNT_USER: NO_ACCOUNT,
          PROBE_REVOKED_USER: REVOKED,
        },
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

function step(steps: Step[], name: string): Step {
  const found = steps.find((candidate) => candidate.step === name);
  if (!found) throw new Error(`the probe printed no ${name} step: ${JSON.stringify(steps)}`);
  return found;
}

beforeAll(async () => {
  probe = cargoBin("rust-backend-client-probe", "rust-backend-client-probe");
  backend = await startMainBackend(APP_DIR, { core: "server" });
  await backend.revokeIdentity(REVOKED);
});

afterAll(async () => {
  await backend?.close();
});

describe("a Rust backend client of a TS main backend", () => {
  it("trusted: acts for users with accounts, NoAccount without, a 403 when revoked, and closes", async () => {
    const { status, steps, stderr } = await run("trusted", backend.mint({ sub: TRUSTED_SUB }));
    expect(steps.map((s) => s.step)).toEqual([
      "kind",
      "for_user",
      "for_user_no_account",
      "for_user_revoked",
      "close",
      "closed",
    ]);
    for (const s of steps)
      expect(s, `${s.step}: ${s.detail}\n${stderr}`).toMatchObject({ ok: true });
    expect(step(steps, "for_user_revoked").detail).toContain("identity_not_authorized");
    expect(status).toBe(0);
  });

  it("scoped: can't act for users", async () => {
    const { status, steps, stderr } = await run("scoped", backend.mint({ sub: SCOPED_SUB }));
    expect(steps.map((s) => s.step)).toEqual(["kind", "for_user_refused", "close"]);
    for (const s of steps)
      expect(s, `${s.step}: ${s.detail}\n${stderr}`).toMatchObject({ ok: true });
    expect(status).toBe(0);
  });

  it("untrusted: a credential the main backend doesn't trust is refused", async () => {
    const { status, steps } = await run("untrusted", backend.mint({ sub: "nobody-we-know" }));
    expect(step(steps, "open_refused")).toMatchObject({ ok: true });
    expect(status).toBe(0);
  });
});
