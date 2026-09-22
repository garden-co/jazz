import { describe, expect, it, vi } from "vitest";
import { createSSRApp, h } from "vue";
import { renderToString } from "@vue/server-renderer";
import type { AccountDbConfig } from "../accounts/context.js";
import { makeFakeAccount } from "../react-core/test-utils.js";

const mocks = vi.hoisted(() => ({
  createJazzClient: vi.fn(),
}));

vi.mock("./create-jazz-client.js", () => ({
  createJazzClient: mocks.createJazzClient,
}));

import { JazzProvider } from "./provider.js";

describe("JazzProvider SSR", () => {
  it("renders its fallback without creating a browser client", async () => {
    const config: AccountDbConfig = {
      appId: "ssr",
      serverUrl: "https://jazz.example.com",
      driver: { type: "memory" },
      account: makeFakeAccount("ssr"),
    };
    const app = createSSRApp({
      render: () =>
        h(
          JazzProvider,
          { config },
          {
            default: () => h("p", { id: "ready" }, "ready"),
            fallback: () => h("p", { id: "loading" }, "loading"),
          },
        ),
    });

    await expect(renderToString(app)).resolves.toContain('<p id="loading">loading</p>');
    expect(mocks.createJazzClient).not.toHaveBeenCalled();
  });
});
