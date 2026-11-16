import { describe, expect, it, vi } from "vitest";
import { createRawSnippet, mount, unmount } from "svelte";
const factory = vi.hoisted(() => vi.fn());
vi.mock("../session/create-jazz-session.js", () => ({ openBrowserSessionCore: factory }));
vi.mock("./create-jazz-client.js", () => ({ openJazzClient: factory }));
import JazzProvider from "./JazzProvider.svelte";
import JazzSvelteProvider from "./JazzSvelteProvider.svelte";

const children = createRawSnippet(() => ({ render: () => "<p>ready</p>" }));

// The providers no longer open the Inspector overlay themselves; a page
// mounts it with `mountInspector(db)` from `jazz-tools/inspector`. These two
// collect their other props with a rest pattern, so the check reads Svelte's
// rest-props object.
describe("Svelte providers refuse the removed autoAttachDevTools", () => {
  const providers = [
    ["JazzProvider", JazzProvider, { config: { scope: "test" }, children }],
    [
      "JazzSvelteProvider",
      JazzSvelteProvider,
      { config: { appId: "x", driver: { type: "memory" } }, children },
    ],
  ] as const;

  for (const [name, component, props] of providers) {
    it(`${name} refuses it without starting a client`, () => {
      factory.mockReset();
      expect(() =>
        mount(component as never, {
          target: document.createElement("div"),
          props: { ...props, autoAttachDevTools: false } as never,
        }),
      ).toThrow(new RegExp(`${name} takes no autoAttachDevTools.*mountInspector`));
      expect(factory).not.toHaveBeenCalled();
    });

    it(`${name} accepts it left undefined`, async () => {
      const instance = mount(component as never, {
        target: document.createElement("div"),
        props: { ...props, autoAttachDevTools: undefined } as never,
      });
      await unmount(instance);
    });
  }
});
