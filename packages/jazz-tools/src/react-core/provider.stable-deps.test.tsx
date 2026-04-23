import React from "react";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, act } from "@testing-library/react";
import { JazzProvider } from "./provider.js";
import { makeFakeClient } from "./test-utils.js";
import type { DbConfig } from "../runtime/db.js";

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  // Flush any pending release timers so the module-level cache is clean.
  vi.runAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("JazzProvider — stable config deps", () => {
  /**
   * Regression: passing a fresh config object with the same JSON shape must not
   * trigger a cleanup→reacquire cycle. The useEffect must only re-fire when the
   * configKey (JSON.stringify of config) or createJazzClient changes.
   *
   * Before the fix, `config` was in the dep array. A new reference caused:
   *   cleanup → releaseClient (schedules setTimeout(0) to shut down the client)
   *   re-run  → acquireClient (clears the timer — but only if it runs in time)
   * Under production timing the timer could fire before the re-run, tearing the
   * client down and surfacing as 'Worker load error: undefined'.
   *
   * We detect the cleanup firing by spying on setTimeout: releaseClient calls
   * setTimeout(0) to schedule the deferred shutdown. If the effect re-runs
   * unnecessarily, at least one setTimeout call will occur.
   */
  it("does not trigger a release timer when config object is replaced by a structurally identical one", async () => {
    const alice = makeFakeClient({ authMode: "local-first", userId: "alice", claims: {} });
    const createJazzClient = vi.fn().mockResolvedValue(alice);

    // Track setTimeout calls made after initial mount so we can detect spurious
    // release timers scheduled by an unnecessary cleanup.
    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");

    function Wrapper({ config }: { config: DbConfig }) {
      return (
        <JazzProvider config={config} createJazzClient={createJazzClient} fallback={null}>
          <div data-testid="child" />
        </JazzProvider>
      );
    }

    const initialConfig: DbConfig = { appId: "app-1", serverUrl: "https://jazz.example.com" };

    let rerender!: (ui: React.ReactElement) => void;

    await act(async () => {
      const result = render(<Wrapper config={initialConfig} />);
      rerender = result.rerender;
      await Promise.resolve();
    });

    expect(createJazzClient).toHaveBeenCalledTimes(1);

    // Reset the spy so we only observe calls that happen during the re-render.
    setTimeoutSpy.mockClear();

    // Re-render with a freshly constructed config object: same shape, new reference.
    const freshConfig: DbConfig = { appId: "app-1", serverUrl: "https://jazz.example.com" };
    expect(freshConfig).not.toBe(initialConfig); // guard: references differ

    await act(async () => {
      rerender(<Wrapper config={freshConfig} />);
      await Promise.resolve();
    });

    // If config was in the dep array, releaseClient would have called setTimeout(0).
    // After the fix (config removed from deps), no setTimeout call should occur.
    expect(setTimeoutSpy).not.toHaveBeenCalled();
  });
});
