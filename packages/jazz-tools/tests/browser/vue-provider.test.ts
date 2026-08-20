import { afterEach, describe, expect, it, vi } from "vitest";
import { createApp, defineComponent, h, nextTick, ref, type App } from "vue";
import type { DbConfig } from "../../src/runtime/db.js";
import type { JazzClient } from "../../src/vue/create-jazz-client.js";

const mocks = vi.hoisted(() => ({
  createJazzClient: vi.fn(),
}));

vi.mock("../../src/vue/create-jazz-client.js", () => ({
  createJazzClient: mocks.createJazzClient,
}));

import { JazzClientProvider, JazzProvider } from "../../src/vue/provider.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

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
  mocks.createJazzClient.mockReset();
  document.body.innerHTML = "";
});

describe("Vue Jazz providers", () => {
  it("JazzProvider renders the SSR fallback first, then starts its client after mount", async () => {
    const client = fakeClient();
    const creation = deferred();
    mocks.createJazzClient.mockImplementation(async () => {
      await creation.promise;
      return client;
    });
    const config: DbConfig = { appId: "hydration", driver: { type: "memory" } };
    const root = defineComponent(
      () => () =>
        h(
          JazzProvider,
          { config },
          {
            default: () => h("p", { id: "ready" }, "ready"),
            fallback: () => h("p", { id: "loading" }, "loading"),
          },
        ),
    );

    const element = document.createElement("div");
    document.body.appendChild(element);
    const app = createApp(root);
    apps.push(app);
    app.mount(element);

    expect(element.innerHTML).toBe('<p id="loading">loading</p>');
    await settle();
    expect(mocks.createJazzClient).toHaveBeenCalledWith(config);

    creation.resolve();
    await vi.waitFor(() => expect(element.querySelector("#ready")).not.toBeNull());
  });

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

  it("JazzProvider closes its client before creating one for a replacement config", async () => {
    const shutdown = deferred();
    const first = fakeClient(vi.fn(() => shutdown.promise));
    const second = fakeClient();
    mocks.createJazzClient.mockResolvedValueOnce(first).mockResolvedValueOnce(second);
    const config = ref<DbConfig>({ appId: "first", driver: { type: "memory" } });
    const root = defineComponent(
      () => () =>
        h(
          JazzProvider,
          { config: config.value },
          {
            default: () => h("p", { id: "ready" }, "ready"),
            fallback: () => h("p", { id: "loading" }, "loading"),
          },
        ),
    );

    const element = document.createElement("div");
    document.body.appendChild(element);
    const app = createApp(root);
    apps.push(app);
    app.mount(element);
    await vi.waitFor(() => expect(element.querySelector("#ready")).not.toBeNull());

    config.value = { appId: "second", driver: { type: "memory" } };
    await settle();

    expect(first.shutdown).toHaveBeenCalledOnce();
    expect(mocks.createJazzClient).toHaveBeenCalledOnce();
    expect(element.querySelector("#loading")).not.toBeNull();

    shutdown.resolve();
    await vi.waitFor(() => {
      expect(mocks.createJazzClient).toHaveBeenNthCalledWith(2, config.value);
      expect(element.querySelector("#ready")).not.toBeNull();
    });

    app.unmount();
    apps.splice(apps.indexOf(app), 1);
    await settle();
    expect(second.shutdown).toHaveBeenCalledOnce();
  });

  it("JazzProvider shuts down a client whose slow creation finishes after unmount", async () => {
    let resolveClient!: (client: JazzClient) => void;
    const clientCreatedAfterUnmount = new Promise<JazzClient>((resolve) => {
      resolveClient = resolve;
    });
    const client = fakeClient();
    mocks.createJazzClient.mockReturnValue(clientCreatedAfterUnmount);
    const config: DbConfig = { appId: "slow", driver: { type: "memory" } };
    const root = defineComponent(
      () => () =>
        h(JazzProvider, { config }, { fallback: () => h("p", { id: "loading" }, "loading") }),
    );

    const element = document.createElement("div");
    document.body.appendChild(element);
    const app = createApp(root);
    apps.push(app);
    app.mount(element);
    await settle();
    expect(mocks.createJazzClient).toHaveBeenCalledOnce();

    app.unmount();
    apps.splice(apps.indexOf(app), 1);
    resolveClient(client);

    await vi.waitFor(() => expect(client.shutdown).toHaveBeenCalledOnce());
  });
});
