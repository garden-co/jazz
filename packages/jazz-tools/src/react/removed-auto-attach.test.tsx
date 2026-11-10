import { describe, expect, it, vi } from "vitest";
import { render } from "@testing-library/react";
import * as React from "react";

const { startSpy } = vi.hoisted(() => ({ startSpy: vi.fn() }));
vi.mock("../dev/inspector-overlay/loader.js", () => ({ startInspectorOverlay: startSpy }));

vi.mock("../react-core/provider.js", () => ({
  JazzProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  JazzClientProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  useDb: () => ({}),
  useJazzClient: () => ({ db: {} }),
  useSession: () => null,
}));
vi.mock("../react-core/session.js", () => ({
  JazzSessionProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  ConfiguredJazzSessionProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  useJazzSession: () => null,
}));
vi.mock("../session/create-jazz-session.js", () => ({ createJazzClient: vi.fn() }));

import { JazzProvider } from "./provider.js";
import { JazzSessionProvider } from "./session.js";

// The providers no longer open the Inspector overlay themselves: a page
// mounts it with `mountInspector(db)` from `jazz-tools/inspector`.
describe("React providers without dev auto-attach", () => {
  it("never load the overlay, even with the old bundler-plugin flag set", async () => {
    (process.env as Record<string, string>).NODE_ENV = "development";
    (process.env as Record<string, string>).NEXT_PUBLIC_JAZZ_INSPECTOR = "1";
    try {
      render(
        <>
          <JazzProvider config={{} as never}>
            <div />
          </JazzProvider>
          <JazzSessionProvider session={{} as never}>
            <div />
          </JazzSessionProvider>
        </>,
      );
      await new Promise((r) => setTimeout(r, 20));
      expect(startSpy).not.toHaveBeenCalled();
    } finally {
      delete (process.env as Record<string, string>).NEXT_PUBLIC_JAZZ_INSPECTOR;
    }
  });

  it("refuse the removed autoAttachDevTools prop and say what to use instead", () => {
    const props = { autoAttachDevTools: false } as Record<string, unknown>;
    const quiet = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      expect(() =>
        render(
          <JazzProvider config={{} as never} {...props}>
            <div />
          </JazzProvider>,
        ),
      ).toThrow(/JazzProvider takes no autoAttachDevTools.*mountInspector/);
      expect(() =>
        render(
          <JazzSessionProvider session={{} as never} {...props}>
            <div />
          </JazzSessionProvider>,
        ),
      ).toThrow(/JazzSessionProvider takes no autoAttachDevTools/);
    } finally {
      quiet.mockRestore();
    }
  });
});
