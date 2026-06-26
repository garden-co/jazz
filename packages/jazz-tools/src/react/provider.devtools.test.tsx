import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, waitFor } from "@testing-library/react";
import * as React from "react";

const { startSpy } = vi.hoisted(() => ({ startSpy: vi.fn() }));
vi.mock("../dev/inspector-overlay/loader.js", () => ({ startInspectorOverlay: startSpy }));

let currentDb: object = {};
vi.mock("../react-core/provider.js", () => ({
  JazzProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  useDb: () => currentDb,
  useJazzClient: () => ({ db: currentDb }),
  useSession: () => null,
}));
import { JazzProvider } from "./provider.js";

describe("JazzProvider dev auto-attach", () => {
  beforeEach(() => {
    startSpy.mockClear();
    // Fresh db each test so the once-per-db guard (markDevToolsAttached) doesn't carry over.
    currentDb = {};
    (process.env as Record<string, string>).NODE_ENV = "development";
  });

  it("mounts the inspector overlay in dev (no schema needed)", async () => {
    render(
      <JazzProvider config={{} as never}>
        <div />
      </JazzProvider>,
    );
    await waitFor(() => expect(startSpy).toHaveBeenCalledTimes(1));
    expect(startSpy).toHaveBeenCalledWith(currentDb);
  });

  it("does not mount with autoAttachDevTools={false}", async () => {
    render(
      <JazzProvider config={{} as never} autoAttachDevTools={false}>
        <div />
      </JazzProvider>,
    );
    await new Promise((r) => setTimeout(r, 20));
    expect(startSpy).not.toHaveBeenCalled();
  });

  it("does not mount when NODE_ENV=production", async () => {
    (process.env as Record<string, string>).NODE_ENV = "production";
    render(
      <JazzProvider config={{} as never}>
        <div />
      </JazzProvider>,
    );
    await new Promise((r) => setTimeout(r, 20));
    expect(startSpy).not.toHaveBeenCalled();
  });
});
