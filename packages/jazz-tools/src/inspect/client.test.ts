import { describe, expect, it } from "vitest";
import { callInspector } from "./client.js";
import { INSPECT_TOKEN_PREFIX } from "./protocol.js";

const TOKEN = `${INSPECT_TOKEN_PREFIX}payload.mac`;

describe("callInspector", () => {
  /** A fetch that records where it was sent with which headers, and answers whoami. */
  function recording() {
    const sent: { url: string; authorization: string | null }[] = [];
    const fetch = (async (url: string, init?: RequestInit) => {
      sent.push({ url, authorization: new Headers(init?.headers).get("authorization") });
      return Response.json({ by: "me" });
    }) as typeof globalThis.fetch;
    return { sent, fetch };
  }

  it("never sends a token over plain http to another machine", async () => {
    const { sent, fetch } = recording();
    await expect(
      callInspector("http://app.example.com/jazz", { op: "whoami" }, { token: TOKEN, fetch }),
    ).rejects.toMatchObject({ code: "bad_request", message: expect.stringMatching(/plain http/) });
    expect(sent).toEqual([]);
  });

  it("sends it over https, and to this machine over http", async () => {
    const { sent, fetch } = recording();
    for (const endpoint of [
      "https://app.example.com/jazz",
      "http://localhost:3000/jazz",
      "http://127.0.0.1:3000/jazz",
      "http://[::1]:3000/jazz",
    ]) {
      await callInspector(endpoint, { op: "whoami" }, { token: TOKEN, fetch });
    }
    expect(sent.map((call) => call.authorization)).toEqual(Array(4).fill(`Bearer ${TOKEN}`));
  });

  it("calls a plain http app without a token, where its hook decides", async () => {
    const { sent, fetch } = recording();
    await callInspector("http://app.example.com/jazz", { op: "whoami" }, { fetch });
    expect(sent).toEqual([{ url: "http://app.example.com/jazz/inspect/api", authorization: null }]);
  });
});
