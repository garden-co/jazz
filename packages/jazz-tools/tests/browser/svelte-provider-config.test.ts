import { afterEach, describe, expect, it, vi } from "vitest";
import { flushSync, mount, unmount } from "svelte";
import { createAccountManager } from "../../src/accounts/create-account-manager.js";
import type { AccountStore } from "../../src/accounts/persistence.js";
import { createJazzClient } from "../../src/svelte/create-jazz-client.js";
import SvelteClientProviderHarness from "./fixtures/SvelteClientProviderHarness.svelte";
import SvelteProviderConfigHarness from "./fixtures/SvelteProviderConfigHarness.svelte";
import { waitForCondition } from "./support.js";

function inMemoryAccountStore(): AccountStore {
  let selected: string | null = null;
  return {
    async read() {
      return selected;
    },
    async update(transform) {
      selected = transform(selected);
    },
  };
}

async function createTestAccountManager(appId: string) {
  return createAccountManager({
    appId,
    serverUrl: "https://svelte-provider.example",
    store: inMemoryAccountStore(),
  });
}

describe("JazzSvelteProvider config handover", () => {
  let component: Record<string, unknown> | undefined;
  let target: HTMLDivElement | undefined;

  afterEach(async () => {
    if (component) await unmount(component);
    target?.remove();
  });

  it("switches auth configuration after shutting down the previous client", async () => {
    const appId = `svelte-provider-${crypto.randomUUID()}`;
    const dbName = crypto.randomUUID();
    target = document.createElement("div");
    document.body.appendChild(target);

    const accounts = await createTestAccountManager(appId);
    const initialAccount = accounts.createLocalFirst();
    const replacementAccount = accounts.createLocalFirst();
    component = mount(SvelteProviderConfigHarness, {
      target,
      props: {
        initialConfig: {
          appId,
          driver: { type: "persistent", dbName },
          account: initialAccount,
        },
        replacementConfig: {
          appId,
          driver: { type: "persistent", dbName },
          account: replacementAccount,
        },
      },
    });

    await waitForCondition(
      async () =>
        target?.querySelector("[data-provider-account]")?.textContent === initialAccount.id,
      10_000,
      "the initial opaque-account client to become ready",
    );

    // The fixture exports this method from its public component instance.
    const harness = component as { useReplacementConfig(): void };
    harness.useReplacementConfig();
    flushSync();
    expect(target.querySelector('[data-provider-state="loading"]')).not.toBeNull();

    await waitForCondition(
      async () =>
        target?.querySelector("[data-provider-account]")?.textContent === replacementAccount.id,
      10_000,
      "the replacement opaque-account client to become ready",
    );
  });

  it("hands over when an external-JWT config changes in place", async () => {
    const appId = `svelte-provider-${crypto.randomUUID()}`;
    const dbName = crypto.randomUUID();
    const alice = makeJwt({ sub: "alice", iss: "https://auth.example.com" });
    const bob = makeJwt({ sub: "bob", iss: "https://auth.example.com" });
    target = document.createElement("div");
    document.body.appendChild(target);

    component = mount(SvelteProviderConfigHarness, {
      target,
      props: {
        initialConfig: {
          appId,
          driver: { type: "persistent", dbName },
          jwtToken: alice,
        },
        replacementConfig: {
          appId,
          driver: { type: "persistent", dbName },
          jwtToken: bob,
        },
      },
    });

    await waitForCondition(
      async () =>
        target?.querySelector("[data-provider-user]")?.getAttribute("data-provider-user") ===
        '["https://auth.example.com","alice"]',
      10_000,
      "the Alice client to become ready",
    );

    // The fixture exports this method from its public component instance.
    const harness = component as { mutateJwtToken(jwtToken: string): void };
    harness.mutateJwtToken(bob);
    flushSync();
    expect(target.querySelector('[data-provider-state="loading"]')).not.toBeNull();

    await waitForCondition(
      async () =>
        target?.querySelector("[data-provider-user]")?.getAttribute("data-provider-user") ===
        '["https://auth.example.com","bob"]',
      10_000,
      "the replacement Bob client to become ready",
    );
  });
  it("keeps the client for a semantically equivalent replacement config", async () => {
    const appId = `svelte-provider-${crypto.randomUUID()}`;
    const dbName = crypto.randomUUID();
    const alice = makeJwt({ sub: "alice", iss: "https://auth.example.com" });
    const initialDate = new Date("2026-01-01T00:00:00.000Z");
    const equivalentDate = new Date("2026-01-01T00:00:00.000Z");
    target = document.createElement("div");
    document.body.appendChild(target);

    component = mount(SvelteProviderConfigHarness, {
      target,
      props: {
        initialConfig: {
          appId,
          driver: { type: "persistent", dbName },
          jwtToken: alice,
          date: initialDate,
        } as DbConfig,
        replacementConfig: {
          appId,
          driver: { type: "persistent", dbName },
          jwtToken: alice,
          date: equivalentDate,
        } as DbConfig,
      },
    });

    await waitForCondition(
      async () =>
        target?.querySelector("[data-provider-user]")?.getAttribute("data-provider-user") ===
        '["https://auth.example.com","alice"]',
      10_000,
      "the Alice client to become ready",
    );

    // The fixture exports this method from its public component instance.
    const harness = component as { useReplacementConfig(): void };
    harness.useReplacementConfig();
    flushSync();
    expect(target.querySelector('[data-provider-state="loading"]')).toBeNull();
    expect(target.querySelector("[data-provider-user]")?.getAttribute("data-provider-user")).toBe(
      '["https://auth.example.com","alice"]',
    );
  });
});

describe("JazzSvelteClientProvider client ownership", () => {
  it("provides a promised client without shutting it down on unmount", async () => {
    const appId = `svelte-client-provider-${crypto.randomUUID()}`;
    const accounts = await createTestAccountManager(appId);
    const client = await createJazzClient({
      appId,
      driver: { type: "memory" },
      account: accounts.createLocalFirst(),
    });
    const shutdown = vi.spyOn(client, "shutdown");
    const target = document.createElement("div");
    document.body.appendChild(target);

    const component = mount(SvelteClientProviderHarness, {
      target,
      props: { client: Promise.resolve(client) },
    });

    await waitForCondition(
      async () =>
        target.querySelector('[data-client-provider-state="ready"]')?.textContent === "local-first",
      10_000,
      "the caller-owned client to become available",
    );

    await unmount(component);
    expect(shutdown).not.toHaveBeenCalled();

    await client.shutdown();
    target.remove();
  });
});
