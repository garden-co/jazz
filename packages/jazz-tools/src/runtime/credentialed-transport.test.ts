import { afterEach, describe, expect, it, vi } from "vitest";
import { bearerWebSocket } from "./credentialed-transport.js";

type Opened = { url: string; options?: { headers?: Record<string, string> } };

function fakeBase() {
  const opened: Opened[] = [];
  class FakeSocket {
    binaryType = "blob";
    readyState = 0;
    constructor(url: string, options?: { headers?: Record<string, string> }) {
      opened.push({ url, options });
    }
    addEventListener() {}
    send() {}
    close() {}
  }
  return { opened, Base: FakeSocket };
}

function events(socket: { addEventListener(type: string, listener: (e?: unknown) => void): void }) {
  const seen: string[] = [];
  socket.addEventListener("error", () => seen.push("error"));
  socket.addEventListener("close", () => seen.push("close"));
  return seen;
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("bearerWebSocket", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("sends the bearer to the app's own origin", async () => {
    const { opened, Base } = fakeBase();
    const Socket = bearerWebSocket("https://app.example/jazz", () => "token-alice", Base);
    new Socket("wss://app.example/jazz/sync");
    await settle();
    expect(opened).toEqual([
      {
        url: "wss://app.example/jazz/sync",
        options: { headers: { authorization: "Bearer token-alice" } },
      },
    ]);
  });

  it("sends nothing to another origin, and doesn't ask the bearer for it", async () => {
    const { opened, Base } = fakeBase();
    const bearer = vi.fn(() => "token-alice");
    const Socket = bearerWebSocket("https://app.example/jazz", bearer, Base);
    new Socket("wss://other.example/jazz/sync");
    await settle();
    expect(opened).toEqual([{ url: "wss://other.example/jazz/sync", options: undefined }]);
    expect(bearer).not.toHaveBeenCalled();
  });

  it("opens without a header while the bearer names no one", async () => {
    const { opened, Base } = fakeBase();
    const Socket = bearerWebSocket("https://app.example/jazz", () => null, Base);
    new Socket("wss://app.example/jazz/sync");
    await settle();
    expect(opened).toEqual([{ url: "wss://app.example/jazz/sync", options: undefined }]);
  });

  it("closes the socket cleanly when the bearer throws", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const { opened, Base } = fakeBase();
    const Socket = bearerWebSocket(
      "https://app.example/jazz",
      () => {
        throw new Error("no session");
      },
      Base,
    );
    let socket!: InstanceType<typeof Socket>;
    expect(() => {
      socket = new Socket("wss://app.example/jazz/sync");
    }).not.toThrow();
    const seen = events(socket);
    await settle();
    expect(seen).toEqual(["error", "close"]);
    expect(socket.readyState).toBe(3);
    expect(opened).toEqual([]);
  });

  it("gives up on a bearer that never answers", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "error").mockImplementation(() => {});
    const { opened, Base } = fakeBase();
    const Socket = bearerWebSocket(
      "https://app.example/jazz",
      () => new Promise<string>(() => {}),
      Base,
    );
    const socket = new Socket("wss://app.example/jazz/sync");
    const seen = events(socket);
    await vi.advanceTimersByTimeAsync(29_000);
    expect(seen).toEqual([]);
    expect(socket.readyState).toBe(0);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(seen).toEqual(["error", "close"]);
    expect(socket.readyState).toBe(3);
    expect(opened).toEqual([]);
  });
});
