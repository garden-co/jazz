import { describe, expect, it, vi } from "vitest";
import {
  provisionHostedApp,
  ProvisionHttpError,
  ProvisionNetworkError,
  ProvisionParseError,
} from "./cloud-provision.js";

function makeResponse(body: unknown, status = 200): typeof globalThis.fetch {
  return async () =>
    new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    });
}

describe("provisionHostedApp", () => {
  describe("happy path", () => {
    it("returns appId, adminSecret, backendSecret from the response", async () => {
      const payload = {
        appId: "app_abc123",
        adminSecret: "secret_admin",
        backendSecret: "secret_backend",
      };

      const result = await provisionHostedApp({
        apiUrl: "https://example.com/api/apps/generate",
        fetch: makeResponse(payload),
      });

      expect(result).toEqual(payload);
    });
  });

  describe("network error", () => {
    it("throws ProvisionNetworkError including the cause message and apiUrl", async () => {
      const apiUrl = "https://example.com/api/apps/generate";
      const cause = new TypeError("Failed to fetch");

      const throwingFetch: typeof globalThis.fetch = async () => {
        throw cause;
      };

      await expect(provisionHostedApp({ apiUrl, fetch: throwingFetch })).rejects.toSatisfy(
        (err: unknown) => {
          if (!(err instanceof ProvisionNetworkError)) return false;
          return err.message.includes(cause.message) && err.message.includes(apiUrl);
        },
      );
    });
  });

  describe("request timeout", () => {
    it("aborts a stalled fetch and reports a timed-out network error", async () => {
      vi.useFakeTimers();
      const apiUrl = "https://example.com/api/apps/generate";
      const timeoutReason = new DOMException(
        "The operation was aborted due to timeout",
        "TimeoutError",
      );
      const timeoutController = new AbortController();
      const timeoutSpy = vi.spyOn(AbortSignal, "timeout").mockImplementation((delay) => {
        setTimeout(() => timeoutController.abort(timeoutReason), delay);
        return timeoutController.signal;
      });
      let requestSignal: AbortSignal | undefined;

      const stalledFetch: typeof globalThis.fetch = async (_url, init) => {
        requestSignal = init?.signal ?? undefined;
        return new Promise<Response>((_resolve, reject) => {
          requestSignal?.addEventListener("abort", () => reject(requestSignal?.reason), {
            once: true,
          });
        });
      };

      try {
        const provisioning = provisionHostedApp({ apiUrl, fetch: stalledFetch });

        expect(requestSignal).toBe(timeoutController.signal);
        expect(timeoutSpy).toHaveBeenCalledWith(8_000);

        const timeoutAssertion = expect(provisioning).rejects.toSatisfy((err: unknown) => {
          if (!(err instanceof ProvisionNetworkError)) return false;
          return (
            /timeout/i.test(err.message) &&
            err.cause === timeoutReason &&
            err.message.includes(apiUrl)
          );
        });

        await vi.advanceTimersByTimeAsync(7_999);
        await vi.advanceTimersByTimeAsync(1);

        await timeoutAssertion;
      } finally {
        timeoutSpy.mockRestore();
        vi.useRealTimers();
      }
    });
  });

  describe("non-2xx HTTP", () => {
    it("throws ProvisionHttpError carrying status and apiUrl", async () => {
      const apiUrl = "https://example.com/api/apps/generate";

      await expect(provisionHostedApp({ apiUrl, fetch: makeResponse({}, 500) })).rejects.toSatisfy(
        (err: unknown) => {
          if (!(err instanceof ProvisionHttpError)) return false;
          return err.status === 500 && err.message.includes(apiUrl);
        },
      );
    });
  });

  describe("malformed JSON", () => {
    it("throws ProvisionParseError when the response body is not valid JSON", async () => {
      const apiUrl = "https://example.com/api/apps/generate";

      const badJsonFetch: typeof globalThis.fetch = async () =>
        new Response("not-json", { status: 200 });

      await expect(provisionHostedApp({ apiUrl, fetch: badJsonFetch })).rejects.toBeInstanceOf(
        ProvisionParseError,
      );
    });
  });

  describe("missing fields", () => {
    it("throws ProvisionParseError naming the missing field when appId is absent", async () => {
      const apiUrl = "https://example.com/api/apps/generate";

      await expect(
        provisionHostedApp({
          apiUrl,
          fetch: makeResponse({ adminSecret: "s", backendSecret: "b" }),
        }),
      ).rejects.toSatisfy((err: unknown) => {
        if (!(err instanceof ProvisionParseError)) return false;
        return err.message.includes("appId");
      });
    });

    it("throws ProvisionParseError naming the missing field when adminSecret is absent", async () => {
      const apiUrl = "https://example.com/api/apps/generate";

      await expect(
        provisionHostedApp({
          apiUrl,
          fetch: makeResponse({ appId: "a", backendSecret: "b" }),
        }),
      ).rejects.toSatisfy((err: unknown) => {
        if (!(err instanceof ProvisionParseError)) return false;
        return err.message.includes("adminSecret");
      });
    });

    it("throws ProvisionParseError naming the missing field when backendSecret is absent", async () => {
      const apiUrl = "https://example.com/api/apps/generate";

      await expect(
        provisionHostedApp({
          apiUrl,
          fetch: makeResponse({ appId: "a", adminSecret: "s" }),
        }),
      ).rejects.toSatisfy((err: unknown) => {
        if (!(err instanceof ProvisionParseError)) return false;
        return err.message.includes("backendSecret");
      });
    });

    it("throws ProvisionParseError naming the field when appId is an empty string", async () => {
      const apiUrl = "https://example.com/api/apps/generate";

      await expect(
        provisionHostedApp({
          apiUrl,
          fetch: makeResponse({ appId: "", adminSecret: "secret", backendSecret: "another" }),
        }),
      ).rejects.toSatisfy((err: unknown) => {
        if (!(err instanceof ProvisionParseError)) return false;
        return err.message.includes("appId") && err.message.includes("missing required fields");
      });
    });
  });

  describe("array body", () => {
    it("throws ProvisionParseError with 'not an object' message when response is an array", async () => {
      const apiUrl = "https://example.com/api/apps/generate";

      await expect(
        provisionHostedApp({
          apiUrl,
          fetch: makeResponse([{ appId: "x" }]),
        }),
      ).rejects.toSatisfy((err: unknown) => {
        if (!(err instanceof ProvisionParseError)) return false;
        return err.message.includes("not an object");
      });
    });
  });

  describe("malformed JSON cause preservation", () => {
    it("preserves the underlying SyntaxError as cause when JSON parsing fails", async () => {
      const apiUrl = "https://example.com/api/apps/generate";

      const plainTextFetch: typeof globalThis.fetch = async () =>
        new Response("this is not json", {
          status: 200,
          headers: { "Content-Type": "text/plain" },
        });

      await expect(provisionHostedApp({ apiUrl, fetch: plainTextFetch })).rejects.toSatisfy(
        (err: unknown) => {
          if (!(err instanceof ProvisionParseError)) return false;
          return err.cause instanceof Error;
        },
      );
    });
  });
});
