// @vitest-environment jsdom
import { createApp, h, nextTick, shallowRef, type Component } from "vue";
import { describe, expect, it, vi } from "vitest";
import { AccountManager, type AccountHandle } from "../accounts/state.js";
import { createJazzSessionOwner } from "../session/state.js";

const mocks = vi.hoisted(() => ({ openJazzClient: vi.fn() }));
vi.mock("./create-jazz-client.js", () => ({ openJazzClient: mocks.openJazzClient }));
import { JazzClientProvider, LegacyJazzProvider } from "./provider.js";
import { JazzSessionProvider } from "./session.js";

const client = {
  db: { onAuthChanged: () => () => {} },
  session: null,
  shutdown: async () => {},
};
const account = { id: "local", identity: { issuer: "test", subject: "local" } } as AccountHandle;
const session = await createJazzSessionOwner({
  accounts: new AccountManager(
    { createLocalFirst: () => account, loginWithBackend: async () => account },
    account,
  ),
  openClient: async () => client as never,
});

function mountWith(component: Component, props: () => Record<string, unknown>) {
  const errors: unknown[] = [];
  const app = createApp({ render: () => h(component, props()) });
  app.config.errorHandler = (error) => {
    errors.push(error);
  };
  app.config.warnHandler = () => {};
  app.mount(document.createElement("div"));
  return { errors, app };
}

// The providers no longer open the Inspector overlay themselves; a page
// mounts it with `mountInspector(db)` from `jazz-tools/inspector`.
describe("Vue providers refuse the removed autoAttachDevTools", () => {
  const providers: [string, Component, Record<string, unknown>][] = [
    ["JazzProvider", LegacyJazzProvider, { config: { appId: "x", driver: { type: "memory" } } }],
    ["JazzClientProvider", JazzClientProvider, { client }],
    ["JazzSessionProvider", JazzSessionProvider, { session }],
  ];

  for (const [name, component, props] of providers) {
    it(`${name} refuses it, camelCase or kebab-case`, () => {
      for (const removed of [{ autoAttachDevTools: false }, { "auto-attach-dev-tools": "" }]) {
        const { errors, app } = mountWith(component, () => ({ ...props, ...removed }));
        expect(String(errors[0])).toMatch(
          new RegExp(`${name} takes no autoAttachDevTools.*mountInspector`),
        );
        app.unmount();
      }
    });

    it(`${name} accepts it left undefined`, () => {
      const { errors, app } = mountWith(component, () => ({
        ...props,
        autoAttachDevTools: undefined,
      }));
      expect(errors).toEqual([]);
      app.unmount();
    });
  }

  it("the config provider refuses it when it arrives on an update", async () => {
    const extra = shallowRef<Record<string, unknown>>({});
    const { errors, app } = mountWith(LegacyJazzProvider, () => ({
      config: { appId: "x", driver: { type: "memory" } },
      ...extra.value,
    }));
    expect(errors).toEqual([]);
    extra.value = { autoAttachDevTools: false };
    await nextTick();
    expect(String(errors[0])).toMatch(/JazzProvider takes no autoAttachDevTools/);
    app.unmount();
  });
});
