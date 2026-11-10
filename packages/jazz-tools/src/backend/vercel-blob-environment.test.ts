import { describe, expect, it } from "vitest";
import { refuseOtherEnvironmentsBlobStore } from "./vercel-store.js";

/** A fetch that answers every request with `answer`, and records what it was asked. */
function fakeFetch(answer: () => Response | Promise<Response>) {
  const asked: Array<{ url: string; authorization: string | null }> = [];
  const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    asked.push({
      url: String(input),
      authorization: new Headers(init?.headers).get("authorization"),
    });
    return answer();
  }) as typeof fetch;
  return { fetchImpl, asked };
}

describe("refuseOtherEnvironmentsBlobStore", () => {
  it("refuses a store another environment claimed", async () => {
    const { fetchImpl, asked } = fakeFetch(() => new Response("production\n"));
    await expect(
      refuseOtherEnvironmentsBlobStore("store_AbC123", "preview", "tok", fetchImpl),
    ).rejects.toThrow(
      /Blob store store_AbC123 serves the production environment, not preview; connect a separate Blob store to each environment/,
    );
    // The marker the core writes, read past the cache with the function's own token.
    expect(asked).toEqual([
      {
        url: "https://abc123.private.blob.vercel-storage.com/.jazz-environment?cache=0",
        authorization: "Bearer tok",
      },
    ]);
  });

  it("accepts its own environment's store", async () => {
    const { fetchImpl } = fakeFetch(() => new Response("preview\n"));
    await refuseOtherEnvironmentsBlobStore("abc123", "preview", "tok", fetchImpl);
  });

  it("leaves a store nobody claimed yet to the core", async () => {
    const { fetchImpl } = fakeFetch(() => new Response("not found", { status: 404 }));
    await refuseOtherEnvironmentsBlobStore("abc123", "preview", "tok", fetchImpl);
  });

  it("leaves a store it can't read to the core", async () => {
    const denied = fakeFetch(() => new Response("forbidden", { status: 403 }));
    await refuseOtherEnvironmentsBlobStore("abc123", "preview", "tok", denied.fetchImpl);
    const down = fakeFetch(() => Promise.reject(new TypeError("fetch failed")));
    await refuseOtherEnvironmentsBlobStore("abc123", "preview", "tok", down.fetchImpl);
  });

  it("doesn't read a store for a workload that names no environment", async () => {
    const { fetchImpl, asked } = fakeFetch(() => new Response("production\n"));
    await refuseOtherEnvironmentsBlobStore("abc123", undefined, "tok", fetchImpl);
    expect(asked).toEqual([]);
  });
});
