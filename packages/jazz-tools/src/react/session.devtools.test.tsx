import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, waitFor } from "@testing-library/react";
import * as React from "react";

const { startSpy } = vi.hoisted(() => ({ startSpy: vi.fn() }));
vi.mock("../dev/inspector-overlay/loader.js", () => ({ startInspectorOverlay: startSpy }));

let currentDb: object = {};
vi.mock("../react-core/provider.js", () => ({
  JazzClientProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  useJazzClient: () => ({ db: currentDb }),
}));

let configuredProps: { config?: Record<string, unknown> } | undefined;
vi.mock("../react-core/session.js", () => ({
  JazzSessionProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  ConfiguredJazzSessionProvider: (props: {
    config: Record<string, unknown>;
    children: React.ReactNode;
  }) => {
    configuredProps = props;
    return <>{props.children}</>;
  },
  useJazzSession: () => null,
}));
vi.mock("../session/create-jazz-session.js", () => ({
  createJazzSession: vi.fn(),
}));

import { JazzSessionProvider } from "./session.js";

describe("JazzSessionProvider dev auto-attach", () => {
  beforeEach(() => {
    startSpy.mockClear();
    currentDb = {};
    configuredProps = undefined;
    (process.env as Record<string, string>).NODE_ENV = "development";
    (process.env as Record<string, string>).NEXT_PUBLIC_JAZZ_INSPECTOR = "1";
  });

  afterEach(() => {
    delete (process.env as Record<string, string>).NEXT_PUBLIC_JAZZ_INSPECTOR;
  });

  it("mounts the inspector for an externally owned session", async () => {
    render(
      <JazzSessionProvider session={{} as never}>
        <div />
      </JazzSessionProvider>,
    );

    await waitFor(() => expect(startSpy).toHaveBeenCalledTimes(1));
    expect(startSpy).toHaveBeenCalledWith(currentDb);
  });

  it("enables devMode before creating a configured session", () => {
    render(
      <JazzSessionProvider config={{ appId: "app" } as never}>
        <div />
      </JazzSessionProvider>,
    );

    expect(configuredProps?.config?.devMode).toBe(true);
  });

  it("does not mount when autoAttachDevTools is disabled", async () => {
    render(
      <JazzSessionProvider session={{} as never} autoAttachDevTools={false}>
        <div />
      </JazzSessionProvider>,
    );

    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(startSpy).not.toHaveBeenCalled();
  });
});
