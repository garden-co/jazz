import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { admitBackendSecret, parseRetryAfter, type BackendAdmissionOptions } from "./admission.js";

type TestFetch = (input: unknown, init?: RequestInit) => Promise<Response>;
const url = "https://core.example/apps/test/backend/admit";
const never = () => new Promise<never>(() => {});

/** Virtual clock: sleeps advance time instantly and are recorded. */
function virtualClock(start = 1_000_000) {
  let time = start;
  const sleeps: number[] = [];
  return {
    sleeps,
    advance(ms: number) {
      time += ms;
    },
    options: {
      now: () => time,
      random: () => 1,
      sleep: async (ms: number) => {
        sleeps.push(ms);
        time += ms;
      },
    } satisfies BackendAdmissionOptions,
  };
}

const status = (code: number, headers?: Record<string, string>) =>
  new Response(code === 204 ? null : "", { status: code, headers });

afterEach(() => vi.useRealTimers());

describe("backend admission", () => {
  it("sends the secret once and resolves on 204", async () => {
    const fetcher = vi.fn<TestFetch>().mockResolvedValue(status(204));
    await admitBackendSecret(url, "secret", { fetch: fetcher as typeof fetch });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher.mock.calls[0]![1]).toMatchObject({
      method: "POST",
      headers: { "X-Jazz-Backend-Secret": "secret" },
      redirect: "manual",
    });
  });

  it.each([502, 503, 504])("waits out a waking app that answers %i", async (code) => {
    const clock = virtualClock();
    const fetcher = vi
      .fn<TestFetch>()
      .mockResolvedValueOnce(status(code))
      .mockResolvedValueOnce(status(code))
      .mockResolvedValueOnce(status(204));
    await admitBackendSecret(url, "secret", { fetch: fetcher as typeof fetch, ...clock.options });
    expect(fetcher).toHaveBeenCalledTimes(3);
    // Jittered exponential backoff, at the top of each step with random() = 1.
    expect(clock.sleeps).toEqual([250, 500]);
  });

  it("retries network failures while the app is unreachable", async () => {
    const clock = virtualClock();
    const fetcher = vi
      .fn<TestFetch>()
      .mockRejectedValueOnce(new TypeError("fetch failed"))
      .mockResolvedValueOnce(status(204));
    await admitBackendSecret(url, "secret", { fetch: fetcher as typeof fetch, ...clock.options });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("honours Retry-After in seconds and as an HTTP date", async () => {
    const clock = virtualClock(Date.parse("2026-10-14T00:00:00Z"));
    const fetcher = vi
      .fn<TestFetch>()
      .mockResolvedValueOnce(status(503, { "Retry-After": "3" }))
      .mockResolvedValueOnce(status(503, { "Retry-After": "Wed, 14 Oct 2026 00:00:10 GMT" }))
      .mockResolvedValueOnce(status(204));
    await admitBackendSecret(url, "secret", { fetch: fetcher as typeof fetch, ...clock.options });
    expect(clock.sleeps).toEqual([3_000, 7_000]);
  });

  it.each(["0", "Tue, 13 Oct 2026 00:00:00 GMT"])(
    "never retries faster than backoff for Retry-After %s",
    async (retryAfter) => {
      const clock = virtualClock(Date.parse("2026-10-14T00:00:00Z"));
      const fetcher = vi
        .fn<TestFetch>()
        .mockResolvedValueOnce(status(503, { "Retry-After": retryAfter }))
        .mockResolvedValueOnce(status(503, { "Retry-After": retryAfter }))
        .mockResolvedValueOnce(status(204));
      await admitBackendSecret(url, "secret", { fetch: fetcher as typeof fetch, ...clock.options });
      expect(clock.sleeps).toEqual([250, 500]);
    },
  );

  it("bounds the request rate when a gateway always answers Retry-After: 0", async () => {
    const clock = virtualClock();
    const fetcher = vi.fn<TestFetch>().mockResolvedValue(status(503, { "Retry-After": "0" }));
    await expect(
      admitBackendSecret(url, "secret", {
        fetch: fetcher as typeof fetch,
        // Bottom of each jittered step: the fastest possible schedule.
        ...clock.options,
        random: () => 0,
      }),
    ).rejects.toThrow("Backend admission failed (503)");
    // 180 s at a 2.5 s floor once backoff saturates, plus the ramp-up.
    expect(fetcher.mock.calls.length).toBeLessThan(80);
  });

  it.each([
    ["a secret with a line break", url, "secret\nX-Injected: 1", "not allowed in an HTTP header"],
    ["an empty secret", url, "", "not allowed in an HTTP header"],
    ["an invalid URL", "not a url", "secret", "valid serverUrl"],
    ["a non-http URL", "ftp://core.example/admit", "secret", "http(s) serverUrl"],
  ])("rejects %s at once without echoing the secret", async (_case, target, secret, message) => {
    const fetcher = vi.fn<TestFetch>();
    const failure = await admitBackendSecret(target, secret, {
      fetch: fetcher as typeof fetch,
    }).catch((error: Error) => error);
    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toContain(message);
    if (secret) expect((failure as Error).message).not.toContain(secret);
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("names the connection error code once the budget is spent", async () => {
    const clock = virtualClock();
    const refused = new TypeError("fetch failed", {
      cause: Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:1"), { code: "ECONNREFUSED" }),
    });
    const fetcher = vi.fn<TestFetch>().mockRejectedValue(refused);
    await expect(
      admitBackendSecret(url, "secret", {
        fetch: fetcher as typeof fetch,
        wakeBudgetMs: 5_000,
        ...clock.options,
      }),
    ).rejects.toThrow("Backend admission could not reach the app: fetch failed (ECONNREFUSED)");
  });

  it("caps an excessive Retry-After", async () => {
    const clock = virtualClock();
    const fetcher = vi
      .fn<TestFetch>()
      .mockResolvedValueOnce(status(503, { "Retry-After": "3600" }))
      .mockResolvedValueOnce(status(204));
    await admitBackendSecret(url, "secret", { fetch: fetcher as typeof fetch, ...clock.options });
    expect(clock.sleeps).toEqual([30_000]);
  });

  it.each([400, 401, 403, 404, 500, 302])("treats %i as the core's final answer", async (code) => {
    const clock = virtualClock();
    const fetcher = vi.fn<TestFetch>().mockResolvedValue(status(code));
    await expect(
      admitBackendSecret(url, "secret", { fetch: fetcher as typeof fetch, ...clock.options }),
    ).rejects.toThrow(`Backend admission failed (${code})`);
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(clock.sleeps).toEqual([]);
  });

  it("gives up with the last status once the wake budget is spent", async () => {
    const clock = virtualClock();
    const fetcher = vi.fn<TestFetch>().mockImplementation(async () => {
      clock.advance(1_000);
      return status(503);
    });
    await expect(
      admitBackendSecret(url, "secret", {
        fetch: fetcher as typeof fetch,
        wakeBudgetMs: 20_000,
        ...clock.options,
      }),
    ).rejects.toThrow("Backend admission failed (503)");
    const waited =
      clock.sleeps.reduce((sum, ms) => sum + ms, 0) + fetcher.mock.calls.length * 1_000;
    expect(waited).toBeLessThanOrEqual(20_000);
    expect(fetcher.mock.calls.length).toBeGreaterThan(3);
  });

  it("does not swallow a caller cancellation", async () => {
    const cancelled = new DOMException("Caller cancelled", "AbortError");
    const fetcher = vi.fn<TestFetch>().mockRejectedValue(cancelled);
    await expect(
      admitBackendSecret(url, "secret", { fetch: fetcher as typeof fetch }),
    ).rejects.toBe(cancelled);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("aborts a stalled attempt and retries until the core answers", async () => {
    vi.useFakeTimers();
    const fetcher = vi
      .fn<TestFetch>()
      .mockImplementationOnce(never)
      .mockResolvedValueOnce(status(204));
    const admitted = admitBackendSecret(url, "secret", {
      fetch: fetcher as typeof fetch,
      random: () => 1,
    });
    await vi.advanceTimersByTimeAsync(30_000);
    expect(fetcher.mock.calls[0]![1]!.signal!.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(250);
    await admitted;
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("reports a timeout when every attempt stalls", async () => {
    vi.useFakeTimers();
    const fetcher = vi.fn<TestFetch>().mockImplementation(never);
    const admitted = expect(
      admitBackendSecret(url, "secret", { fetch: fetcher as typeof fetch }),
    ).rejects.toThrow("Backend admission timed out: the app did not answer within 180s");
    await vi.advanceTimersByTimeAsync(180_000);
    await admitted;
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("backend admission over HTTP", () => {
  let server: Server | undefined;
  afterEach(async () => {
    await new Promise<void>((done) => (server ? server.close(() => done()) : done()));
    server = undefined;
  });

  it("waits through a gateway answering 503 with Retry-After while the core boots", async () => {
    let requests = 0;
    server = createServer((request, response) => {
      requests++;
      if (requests <= 2) {
        response.writeHead(503, { "Retry-After": "0" }).end("no healthy upstream");
        return;
      }
      expect(request.headers["x-jazz-backend-secret"]).toBe("secret");
      response.writeHead(204).end();
    });
    await new Promise<void>((done) => server!.listen(0, "127.0.0.1", done));
    const { port } = server.address() as AddressInfo;
    await admitBackendSecret(`http://127.0.0.1:${port}/apps/test/backend/admit`, "secret");
    expect(requests).toBe(3);
  });
});

describe("Retry-After parsing", () => {
  it("reads delta-seconds, HTTP dates and rejects garbage", () => {
    const now = Date.parse("2026-10-14T00:00:00Z");
    expect(parseRetryAfter(null, now)).toBeUndefined();
    expect(parseRetryAfter(" 5 ", now)).toBe(5_000);
    expect(parseRetryAfter("Wed, 14 Oct 2026 00:00:02 GMT", now)).toBe(2_000);
    expect(parseRetryAfter("Tue, 13 Oct 2026 00:00:00 GMT", now)).toBe(0);
    expect(parseRetryAfter("soon", now)).toBeUndefined();
  });
});
