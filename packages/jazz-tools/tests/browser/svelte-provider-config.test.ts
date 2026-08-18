import { afterEach, describe, expect, it } from "vitest";
import { flushSync, mount, unmount } from "svelte";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import SvelteProviderConfigHarness from "./fixtures/SvelteProviderConfigHarness.svelte";
import { waitForCondition } from "./support.js";

function makeJwt(payload: Record<string, unknown>): string {
  const encode = (value: unknown) =>
    btoa(JSON.stringify(value)).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
  return `${encode({ alg: "none", typ: "JWT" })}.${encode(payload)}.`;
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

    component = mount(SvelteProviderConfigHarness, {
      target,
      props: {
        initialConfig: {
          appId,
          driver: { type: "persistent", dbName },
          secret: generateAuthSecret(),
        },
        replacementConfig: {
          appId,
          driver: { type: "persistent", dbName },
          jwtToken: makeJwt({
            sub: crypto.randomUUID(),
            iss: "https://auth.example.com",
          }),
        },
      },
    });

    await waitForCondition(
      async () =>
        target?.querySelector('[data-provider-state="ready"]')?.textContent === "local-first",
      10_000,
      "the local-first client to become ready",
    );

    (component as { useReplacementConfig(): void }).useReplacementConfig();
    flushSync();
    expect(target.querySelector('[data-provider-state="loading"]')).not.toBeNull();

    await waitForCondition(
      async () =>
        target?.querySelector('[data-provider-state="ready"]')?.textContent === "external",
      10_000,
      "the replacement JWT client to become ready",
    );
  });
});
