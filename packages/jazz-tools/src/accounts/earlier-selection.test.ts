import { afterEach, expect, it, vi } from "vitest";
import { createAccountManagerWithMinter } from "./account-manager-core.js";
import { localFirstAccountId } from "./local-first.js";
import { generateAuthSecret } from "../runtime/auth-secret-store.js";

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
