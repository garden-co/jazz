import { describe, it, expect, vi } from "vitest";
import { render, act } from "@testing-library/react";
import { JazzClientProvider } from "./provider.js";
import { makeFakeClient } from "./test-utils.js";

describe("JazzClientProvider — onJWTExpired", () => {
  it("fires on error=expired; applies returned token", async () => {
    const client = makeFakeClient({ authMode: "external", userId: "u-1", claims: {} });
    const onJWTExpired = vi.fn().mockResolvedValue("fresh.jwt.token");

    render(
      <JazzClientProvider client={client} onJWTExpired={onJWTExpired}>
        <div />
      </JazzClientProvider>,
    );

    await act(async () => {
      client.__markUnauthenticated("expired");
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(onJWTExpired).toHaveBeenCalledTimes(1);
    expect(client.__updateAuthTokenSpy.lastToken).toBe("fresh.jwt.token");
  });

  it("ignores repeated expired events while in-flight", async () => {
    const client = makeFakeClient({ authMode: "external", userId: "u-1", claims: {} });
    const resolve: Array<() => void> = [];
    const onJWTExpired = vi.fn(() => new Promise<string | null>((r) => resolve.push(() => r("t"))));

    render(
      <JazzClientProvider client={client} onJWTExpired={onJWTExpired}>
        <div />
      </JazzClientProvider>,
    );

    await act(async () => {
      client.__markUnauthenticated("expired");
      client.__markUnauthenticated("expired");
      client.__markUnauthenticated("expired");
      await Promise.resolve();
    });

    expect(onJWTExpired).toHaveBeenCalledTimes(1);
    await act(async () => {
      resolve[0]!();
      await Promise.resolve();
    });
  });

  it("dedups the refresh across two providers sharing one client", async () => {
    const client = makeFakeClient({ authMode: "external", userId: "u-1", claims: {} });
    const onJWTExpired = vi.fn().mockResolvedValue("fresh.jwt.token");

    render(
      <>
        <JazzClientProvider client={client} onJWTExpired={onJWTExpired}>
          <div />
        </JazzClientProvider>
        <JazzClientProvider client={client} onJWTExpired={onJWTExpired}>
          <div />
        </JazzClientProvider>
      </>,
    );

    await act(async () => {
      client.__markUnauthenticated("expired");
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(onJWTExpired).toHaveBeenCalledTimes(1);
  });

  it("does not fire for non-expired errors", async () => {
    const client = makeFakeClient({ authMode: "external", userId: "u-1", claims: {} });
    const onJWTExpired = vi.fn();

    render(
      <JazzClientProvider client={client} onJWTExpired={onJWTExpired}>
        <div />
      </JazzClientProvider>,
    );

    await act(async () => {
      client.__markUnauthenticated("invalid");
      await Promise.resolve();
    });

    expect(onJWTExpired).not.toHaveBeenCalled();
  });

  it("releases the latch after a hung refresh times out so a later expiry can retry", async () => {
    vi.useFakeTimers();
    try {
      const client = makeFakeClient({ authMode: "external", userId: "u-1", claims: {} });
      const onJWTExpired = vi
        .fn()
        .mockImplementationOnce(() => new Promise<string | null>(() => {}))
        .mockResolvedValueOnce("fresh.jwt.token");

      render(
        <JazzClientProvider client={client} onJWTExpired={onJWTExpired}>
          <div />
        </JazzClientProvider>,
      );

      await act(async () => {
        client.__markUnauthenticated("expired");
        await Promise.resolve();
      });
      expect(onJWTExpired).toHaveBeenCalledTimes(1);

      // Still in-flight before the timeout: a repeated expiry is ignored.
      await act(async () => {
        client.__markUnauthenticated("expired");
        await Promise.resolve();
      });
      expect(onJWTExpired).toHaveBeenCalledTimes(1);

      // The first refresh never settles; the timeout releases the latch.
      await act(async () => {
        vi.advanceTimersByTime(30_000);
        await Promise.resolve();
      });

      await act(async () => {
        client.__markUnauthenticated("expired");
        await Promise.resolve();
        await Promise.resolve();
      });
      expect(onJWTExpired).toHaveBeenCalledTimes(2);
      expect(client.__updateAuthTokenSpy.lastToken).toBe("fresh.jwt.token");
    } finally {
      vi.useRealTimers();
    }
  });
});
