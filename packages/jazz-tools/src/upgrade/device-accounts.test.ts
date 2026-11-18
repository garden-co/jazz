import { describe, expect, it } from "vitest";
import { loadTypeScript } from "./project.js";
import { FIXTURES } from "./upgrade-test-helpers.js";
import { join } from "node:path";
import {
  rewritePermissions,
  rewritePolicyText,
  rewriteScript,
  rewriteSource,
} from "./device-accounts.js";

const ts = loadTypeScript(join(FIXTURES, "..", "..", ".."));

describe("authMode in jazz.permissions.json", () => {
  it("rewrites every comparison with session.authMode, and nothing else", () => {
    for (const [before, after] of [
      ["session.authMode = 'local-first'", "session.authMode = 'device-account'"],
      ["session.authMode != 'local-first'", "session.authMode != 'device-account'"],
      ["'local-first' = session.authMode", "'device-account' = session.authMode"],
      [
        "session.authMode in ['external', 'local-first']",
        "session.authMode in ['external', 'device-account']",
      ],
      ["not (session.authMode = 'local-first')", "not (session.authMode = 'device-account')"],
      [
        "owner = session.user and session.authMode != 'local-first'",
        "owner = session.user and session.authMode != 'device-account'",
      ],
    ]) {
      expect(rewritePolicyText(before!), before).toBe(after);
    }
    // Not the auth mode: a claim, a column, a string elsewhere.
    for (const same of [
      "session.claims.mode = 'local-first'",
      "tier = 'local-first'",
      "session.authMode = 'external' and kind = 'local-first'",
    ]) {
      expect(rewritePolicyText(same)).toBe(same);
    }
  });

  it("walks anyOf, allOf, update old/new and defs, counting what changed", () => {
    const { permissions, count } = rewritePermissions({
      format: 1,
      defs: { "dev()": "session.authMode = 'local-first'" },
      tables: {
        todos: {
          read: { anyOf: ["owner = session.user", "session.authMode = 'local-first'"] },
          update: { old: "true", new: { allOf: ["session.authMode != 'local-first'"] } },
        },
      },
    });
    expect(count).toBe(3);
    expect(JSON.stringify(permissions)).not.toContain("local-first");
  });
});

describe("authMode and the renamed names in TS", () => {
  const rewrite = (text: string, file = "/app/a.ts") => rewriteSource(ts, file, text);

  it("rewrites the policy DSL's comparisons with session.authMode", () => {
    const out = rewrite(`
const a = eq(session.authMode, "local-first");
const b = ne(session.authMode, 'local-first');
const c = isIn(session.authMode, ["external", "local-first"]);
const d = eq(session.claims.mode, "local-first");
`);
    expect(out.text).toContain('eq(session.authMode, "device-account")');
    expect(out.text).toContain("ne(session.authMode, 'device-account')");
    expect(out.text).toContain('isIn(session.authMode, ["external", "device-account"])');
    expect(out.text).toContain('eq(session.claims.mode, "local-first")');
    expect(out.changes.map((c) => c.line)).toEqual([2, 3, 4]);
  });

  it("rewrites app code comparing an authMode, and test sessions", () => {
    const out = rewrite(`
if (session.authMode === "local-first") {}
if ("local-first" !== auth?.session?.authMode) {}
switch (s.authMode) { case "local-first": break; }
app.as({ authMode: "local-first" });
const tier = { tier: "local-first" };
import "jazz-tools/local-first";
const mode = "local-first-unless-empty";
const issuer = "urn:jazz:local-first";
`);
    expect(out.text).toContain('session.authMode === "device-account"');
    expect(out.text).toContain('"device-account" !== auth?.session?.authMode');
    expect(out.text).toContain('case "device-account":');
    expect(out.text).toContain('as({ authMode: "device-account" })');
    expect(out.text).toContain('{ tier: "local-first" }');
    expect(out.text).toContain('"jazz-tools/local-first"');
    expect(out.text).toContain('"local-first-unless-empty"');
    expect(out.text).toContain('"urn:jazz:local-first"');
  });

  it("renames the AccountAuthError codes, not the HTTP wire codes", () => {
    const out = rewrite(`
if (error.code === "local_first_account_required") {}
const codes = ["local_first_recovery_unavailable", "local_first_restore_unavailable"];
const wire = ["local_first_auth_disabled", "invalid_local_first_proof", "local_first_proof_required"];
`);
    expect(out.text).toContain('"device_account_required"');
    expect(out.text).toContain(
      '["device_account_recovery_unavailable", "device_account_restore_unavailable"]',
    );
    expect(out.text).toContain(
      '["local_first_auth_disabled", "invalid_local_first_proof", "local_first_proof_required"]',
    );
  });

  it("renames the deprecated getLocalFirstIdentityProof and LocalFirstGuest (from jazz-tools/backend only)", () => {
    const out = rewrite(`
import { type LocalFirstGuest, createJazz } from "jazz-tools/backend";
import type { LocalFirstGuest as Guest } from "jazz-tools/backend";
const proof = await db.getLocalFirstIdentityProof({ audience });
const answer: LocalFirstGuest = guest;
const g: Guest = guest;
`);
    expect(out.text).toContain(
      'import { type DeviceAccountsAnswer, createJazz } from "jazz-tools/backend";',
    );
    expect(out.text).toContain(
      'import type { DeviceAccountsAnswer as Guest } from "jazz-tools/backend";',
    );
    expect(out.text).toContain("db.getDeviceAccountProof({ audience })");
    expect(out.text).toContain("const answer: DeviceAccountsAnswer = guest;");
    expect(out.text).toContain("const g: Guest = guest;");
    const other = rewrite(
      `import type { LocalFirstGuest } from "./mine";\nconst a: LocalFirstGuest = x;\n`,
    );
    expect(other.text).toContain("const a: LocalFirstGuest = x;");
  });

  it("renames the CLI flag in a script", () => {
    expect(rewriteScript("jazz-tools apps auth set --allow-local-first-auth && x")).toBe(
      "jazz-tools apps auth set --allow-device-accounts && x",
    );
    expect(rewriteScript("echo --allow-local-first-authx")).toBe("echo --allow-local-first-authx");
  });
});
