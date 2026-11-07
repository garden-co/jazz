import { afterEach, expect, it, vi } from "vitest";
import { createAccountManagerWithMinter } from "./account-manager-core.js";
import { localFirstAccountId } from "./local-first.js";
import { generateAuthSecret } from "../runtime/auth-secret-store.js";
import { formatAuthSecret } from "../runtime/auth-secret-codec.js";

// An alpha.59 app whose browsers reached the Jazz server directly; its
// alpha.60 client syncs through the app's own backend at /jazz.
const appId = "0a590000-0000-4000-8000-000000000059";
const subject = "00000000-0000-4000-8000-0000000000a5";
const earlierKey = `jazz-account-selection-v1:${encodeURIComponent(
  `https://sync.example/apps/${appId}/accounts`,
)}:dev`;
// Controlled crypto boundary, as in persistence.test.ts: the subject a root
// derives is covered by the Rust identity corpus.
const mintToken = () =>
  `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: subject }))}.sig`;

function browser(seed: Record<string, string>) {
  const items = new Map(Object.entries(seed));
  vi.stubGlobal("localStorage", {
    get length() {
      return items.size;
    },
    key: (index: number) => [...items.keys()][index] ?? null,
    getItem: (key: string) => items.get(key) ?? null,
    setItem: (key: string, value: string) => void items.set(key, value),
  });
  vi.stubGlobal("navigator", {
    locks: { request: async (_: string, run: () => unknown) => run() },
  });
  return items;
}

afterEach(() => vi.unstubAllGlobals());

function earlierSelection(root: string) {
  return JSON.stringify({
    format: "jazz-account-selection-v1",
    roots: [root],
    selected: 0,
  });
}

it("a client scoped to an alpha.59 app opens that app's guest on its imported account", async () => {
  const root = generateAuthSecret();
  const items = browser({ [earlierKey]: earlierSelection(root) });
  const accounts = await createAccountManagerWithMinter(
    { serverUrl: "https://app.example/jazz", scope: appId },
    mintToken,
  );
  const guest = accounts.getLoggedIn();
  // The account alpha.59 founded for this guest, which the import keeps.
  expect(guest?.id).toBe(localFirstAccountId(appId, subject));
  // Copied, not moved: alpha.59's key stays for a rollback.
  expect(items.get(earlierKey)).toBe(earlierSelection(root));
  accounts.logout();
  await vi.waitFor(() =>
    expect(
      [...items.keys()].some((key) => key !== earlierKey && items.get(key)!.includes(root)),
    ).toBe(true),
  );
  expect(items.get(earlierKey)).toBe(earlierSelection(root));
});

it("a client without scope does not take over an earlier app's guest", async () => {
  browser({ [earlierKey]: earlierSelection(generateAuthSecret()) });
  const accounts = await createAccountManagerWithMinter(
    { serverUrl: "https://app.example/jazz" },
    mintToken,
  );
  expect(accounts.getLoggedIn()).toBeUndefined();
});

it("one app reached through two earlier servers is ambiguous, so nothing is taken over", async () => {
  const other = `jazz-account-selection-v1:${encodeURIComponent(
    `https://other.example/apps/${appId}/accounts`,
  )}:dev`;
  browser({
    [earlierKey]: earlierSelection(generateAuthSecret()),
    [other]: earlierSelection(generateAuthSecret()),
  });
  const accounts = await createAccountManagerWithMinter(
    { serverUrl: "https://app.example/jazz", scope: appId },
    mintToken,
  );
  expect(accounts.getLoggedIn()).toBeUndefined();
});

it("an earlier selection this client can't read is left alone, and the client starts afresh", async () => {
  const items = browser({ [earlierKey]: "{not a selection" });
  const accounts = await createAccountManagerWithMinter(
    { serverUrl: "https://app.example/jazz", scope: appId },
    mintToken,
  );
  expect(accounts.getLoggedIn()).toBeUndefined();
  expect(items.get(earlierKey)).toBe("{not a selection");
});

it("a guest picked up from alpha.59 and forgotten once linked leaves no root in either key", async () => {
  // A shared device (clearOnSignOut): the guest signs up, and the device must
  // not keep a way back to them, alpha.59's entry included.
  const root = formatAuthSecret(new Uint8Array(32).fill(7)) as string;
  const guestSubject = root.slice("jazz-auth-v1:".length);
  const account = localFirstAccountId(appId, guestSubject);
  const items = browser({ [earlierKey]: earlierSelection(root) });
  vi.stubGlobal("document", {});
  const linkFetch = (async (input: unknown) =>
    String(input).endsWith("/backend-link/challenge")
      ? Response.json({ challenge: "issued-for-kim.mac", expiresAt: 0 })
      : Response.json({
          account,
          identity: { issuer: "jazz:backend", subject: "kim" },
          claims: {},
        })) as unknown as typeof fetch;
  const accounts = await createAccountManagerWithMinter(
    { serverUrl: "https://app.example/jazz", scope: appId, fetch: linkFetch },
    (secret) =>
      `e30.${btoa(JSON.stringify({ iss: "urn:jazz:local-first", sub: secret.slice("jazz-auth-v1:".length) }))}.sig`,
    { rememberBackendUser: false },
  );
  expect(accounts.getLoggedIn()?.id).toBe(account);
  await accounts.linkWithBackend();
  await accounts.settleSelection();
  accounts.logout();
  await accounts.settleSelection();
  for (const value of items.values()) expect(value).not.toContain(root);
  expect(JSON.parse(items.get(earlierKey)!)).toMatchObject({ roots: [], selected: null });
});

it("a .60 client's own registry for the same scope is not an earlier selection", async () => {
  const sixty = `jazz-account-selection-v1:${encodeURIComponent(
    `https://other.example/jazz/apps/${appId}/accounts`,
  )}:dev`;
  browser({ [sixty]: earlierSelection(generateAuthSecret()) });
  const accounts = await createAccountManagerWithMinter(
    { serverUrl: "https://app.example/jazz", scope: appId },
    mintToken,
  );
  expect(accounts.getLoggedIn()).toBeUndefined();
});

it("a logout on a device whose own key already exists keeps alpha.59's roots", async () => {
  // An rc.6 device with scope set: its own guest G, alpha.59's guest K.
  const ownKey = `jazz-account-selection-v1:${encodeURIComponent(
    `https://app.example/jazz/apps/${appId}/accounts`,
  )}:dev`;
  const g = generateAuthSecret();
  const k = generateAuthSecret();
  const items = browser({ [ownKey]: earlierSelection(g), [earlierKey]: earlierSelection(k) });
  const accounts = await createAccountManagerWithMinter(
    { serverUrl: "https://app.example/jazz", scope: appId },
    mintToken,
  );
  accounts.logout();
  await accounts.settleSelection();
  expect(items.get(earlierKey)).toBe(earlierSelection(k));
});

it("a guest created in alpha.59 during a rollback survives the next alpha.60 write", async () => {
  const ownKey = `jazz-account-selection-v1:${encodeURIComponent(
    `https://app.example/jazz/apps/${appId}/accounts`,
  )}:dev`;
  const g = generateAuthSecret();
  const n = generateAuthSecret();
  const rolledBack = JSON.stringify({
    format: "jazz-account-selection-v1",
    roots: [g, n],
    selected: 1,
  });
  const items = browser({ [ownKey]: earlierSelection(g), [earlierKey]: rolledBack });
  const accounts = await createAccountManagerWithMinter(
    { serverUrl: "https://app.example/jazz", scope: appId },
    mintToken,
  );
  accounts.logout();
  await accounts.settleSelection();
  expect(items.get(earlierKey)).toBe(rolledBack);
});
