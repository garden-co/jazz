import { afterEach, describe, expect, it, vi } from "vitest";
import { createApp, defineComponent, h, nextTick, ref, type App } from "vue";
import type { JazzClient } from "../../src/vue/create-jazz-client.js";

import { JazzClientProvider } from "../../src/vue/provider.js";

function fakeClient(shutdown = vi.fn().mockResolvedValue(undefined)): JazzClient {
  return {
    db: {
      onAuthChanged: vi.fn(() => () => {}),
    },
    session: null,
    shutdown,
  } as unknown as JazzClient;
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await nextTick();
}

const apps: App[] = [];

afterEach(() => {
  for (const app of apps.splice(0)) {
    app.unmount();
  }
  document.body.innerHTML = "";
});

describe("Vue JazzClientProvider", () => {
  it("JazzClientProvider never shuts down a caller-owned client", async () => {
    const first = fakeClient();
    const second = fakeClient();
    const client = ref<JazzClient>(first);
    const root = defineComponent(
      () => () =>
        h(
          JazzClientProvider,
          { client: client.value },
          { default: () => h("p", { id: "ready" }, "ready") },
        ),
    );

    const element = document.createElement("div");
    document.body.appendChild(element);
    const app = createApp(root);
    apps.push(app);
    app.mount(element);
    await vi.waitFor(() => expect(element.querySelector("#ready")).not.toBeNull());

    client.value = second;
    await settle();
    expect(element.querySelector("#ready")).not.toBeNull();
    app.unmount();
    apps.splice(apps.indexOf(app), 1);

    expect(first.shutdown).not.toHaveBeenCalled();
    expect(second.shutdown).not.toHaveBeenCalled();
  });
});
