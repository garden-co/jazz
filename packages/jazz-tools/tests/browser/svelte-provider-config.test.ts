import { describe, expect, it, vi } from "vitest";
import { mount, unmount } from "svelte";
import { createTestAccountManager } from "../../src/runtime/testing/account-fixtures.js";
import type { AccountStore } from "../../src/accounts/persistence.js";
import { openJazzClient } from "../../src/svelte/create-jazz-client.js";
import SvelteClientProviderHarness from "./fixtures/SvelteClientProviderHarness.svelte";
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

async function accountsFor(appId: string) {
  return createTestAccountManager({
    appId,
    serverUrl: "https://svelte-provider.example",
    store: inMemoryAccountStore(),
  });
}

describe("JazzSvelteClientProvider client ownership", () => {
  it("provides a promised client without shutting it down on unmount", async () => {
    const appId = `svelte-client-provider-${crypto.randomUUID()}`;
    const accounts = await accountsFor(appId);
    const client = await openJazzClient({
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
        target.querySelector('[data-client-provider-state="ready"]')?.textContent ===
        "device-account",
      10_000,
      "the caller-owned client to become available",
    );

    await unmount(component);
    expect(shutdown).not.toHaveBeenCalled();

    await client.shutdown();
    target.remove();
  });
});
