import { afterEach, describe, expect, it } from "vitest";
import { mount, unmount, type Component } from "svelte";

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`Timeout: ${message}`);
}

const mounts: Array<{ instance: Record<string, never>; container: HTMLDivElement }> = [];

afterEach(() => {
  for (const { instance, container } of mounts) {
    unmount(instance);
    container.remove();
  }
  mounts.length = 0;
});

describe("JazzSvelteProvider session readiness", () => {
  it("does not render children with a stale null session", async () => {
    const el = document.createElement("div");
    document.body.appendChild(el);

    const fakeClient = {
      db: { shutdown: async () => {} },
      session: { user_id: "user-1", claims: {} },
      shutdown: async () => {},
    };

    const { default: ProviderSessionProbe } =
      await import("./fixtures/ProviderSessionProbe.svelte");
    const instance = mount(ProviderSessionProbe as Component, {
      target: el,
      props: {
        client: Promise.resolve(fakeClient as never),
      },
    });
    mounts.push({ instance, container: el });

    await waitFor(
      () => el.querySelector('[data-testid="session-user"]') !== null,
      2000,
      "provider child should render",
    );

    expect(el.querySelector('[data-testid="db-ready"]')?.textContent).toBe("yes");
    expect(el.querySelector('[data-testid="session-user"]')?.textContent).toBe("user-1");
  });
});
