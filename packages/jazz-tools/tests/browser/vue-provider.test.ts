import { afterEach, describe, expect, it, vi } from "vitest";
import { createApp, defineComponent, h, nextTick, ref, type App } from "vue";
import type { DbConfig } from "../../src/runtime/db.js";
import type { JazzClient } from "../../src/vue/create-jazz-client.js";

const mocks = vi.hoisted(() => ({
  openJazzClient: vi.fn(),
}));

vi.mock("../../src/vue/create-jazz-client.js", () => ({
  openJazzClient: mocks.openJazzClient,
}));

import { JazzClientProvider, LegacyJazzProvider as JazzProvider } from "../../src/vue/provider.js";

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
  mocks.openJazzClient.mockReset();
  document.body.innerHTML = "";
});

describe("Vue Jazz providers", () => {
  it("JazzProvider renders the SSR fallback first, then starts its client after mount", async () => {
    const client = fakeClient();
    const creation = deferred();
    mocks.openJazzClient.mockImplementation(async () => {
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
    expect(mocks.openJazzClient).toHaveBeenCalledWith(config);

    creation.resolve();
    await vi.waitFor(() => expect(element.querySelector("#ready")).not.toBeNull());
  });

  it("does not recreate an equivalent config while the client is loading", async () => {
    let resolveClient!: (client: JazzClient) => void;
    const first = fakeClient();
    const creation = new Promise<JazzClient>((resolve) => {
      resolveClient = resolve;
    });
    mocks.openJazzClient.mockReturnValue(creation);
    const config = ref<DbConfig>({ appId: "loading", driver: { type: "memory" } });
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
    await settle();
    expect(mocks.openJazzClient).toHaveBeenCalledOnce();

    config.value = { driver: { type: "memory" }, appId: "loading" };
    await settle();
    expect(mocks.openJazzClient).toHaveBeenCalledOnce();

    resolveClient(first);
    await vi.waitFor(() => expect(element.querySelector("#ready")).not.toBeNull());
    expect(mocks.openJazzClient).toHaveBeenCalledOnce();
    expect(first.shutdown).not.toHaveBeenCalled();
  });

  it("does not flicker or recreate an equivalent config after the client is ready", async () => {
    const client = fakeClient();
    mocks.openJazzClient.mockResolvedValue(client);
    const config = ref<DbConfig>({ appId: "ready", driver: { type: "memory" } });
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
    expect(mocks.openJazzClient).toHaveBeenCalledOnce();

    config.value = { driver: { type: "memory" }, appId: "ready" };
    await nextTick();
    expect(element.querySelector("#ready")).not.toBeNull();
    expect(element.querySelector("#loading")).toBeNull();
    await settle();
    expect(mocks.openJazzClient).toHaveBeenCalledOnce();
    expect(client.shutdown).not.toHaveBeenCalled();
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
    mocks.openJazzClient.mockResolvedValueOnce(first).mockResolvedValueOnce(second);
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
    expect(mocks.openJazzClient).toHaveBeenCalledOnce();
    expect(element.querySelector("#loading")).not.toBeNull();

    shutdown.resolve();
    await vi.waitFor(() => {
      expect(mocks.openJazzClient).toHaveBeenNthCalledWith(2, config.value);
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
    mocks.openJazzClient.mockReturnValue(clientCreatedAfterUnmount);
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
    expect(mocks.openJazzClient).toHaveBeenCalledOnce();

    app.unmount();
    apps.splice(apps.indexOf(app), 1);
    resolveClient(client);

    await vi.waitFor(() => expect(client.shutdown).toHaveBeenCalledOnce());
  });
});
