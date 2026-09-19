import { afterEach, describe, expect, it, vi } from "vitest";
import {
  DashboardInspectorSession,
  completeInspectorCallback,
  dashboardOrigin,
  proofChallenge,
  validateSession,
} from "./browser-session.js";

const session = () => ({
  accessToken: "ephemeral",
  expiresAt: Math.floor(Date.now() / 1000) + 900,
  appId: "app",
  serverUrl: "https://sync.example",
  capabilities: ["inspector:read"],
});
afterEach(() => {
  vi.restoreAllMocks();
  window.history.replaceState(null, "", "/");
});
const auth = () =>
  new DashboardInspectorSession(
    "https://dashboard.example",
    "app",
    new URL("/inspector/callback", window.location.origin).href,
  );

describe("Inspector browser sessions", () => {
  it("renews through cookie-authenticated CSRF exchange without opening a popup", async () => {
    const open = vi.spyOn(window, "open");
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrfToken: "cookie-bound-csrf" })))
      .mockResolvedValueOnce(new Response(JSON.stringify(session())));
    await expect(auth().renew()).resolves.toMatchObject({ accessToken: "ephemeral" });
    expect(fetcher.mock.calls[0]?.[0].toString()).toBe(
      "https://dashboard.example/inspector/session",
    );
    expect(fetcher.mock.calls[0]?.[1]).toMatchObject({
      credentials: "include",
      cache: "no-store",
      redirect: "error",
    });
    expect(fetcher.mock.calls[1]?.[1]).toMatchObject({
      method: "POST",
      credentials: "include",
      headers: { "X-Inspector-CSRF": "cookie-bound-csrf", "Content-Type": "application/json" },
    });
    expect(JSON.parse(fetcher.mock.calls[1]?.[1]?.body as string)).toEqual({
      appId: "app",
      capabilities: ["inspector:read"],
    });
    expect(open).not.toHaveBeenCalled();
    expect(localStorage.getItem("jazz-inspector-session-connection") ?? "").not.toContain(
      "ephemeral",
    );
  });
  it("fails closed on access removal and cancels renewal after logout", async () => {
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("upstream secret", { status: 403 }));
    await expect(auth().renew()).rejects.toThrow("Sign in again");
    let resolve!: (response: Response) => void;
    fetcher.mockImplementation(
      () =>
        new Promise<Response>((r) => {
          resolve = r;
        }),
    );
    const client = auth();
    const pending = client.renew();
    client.logout();
    resolve(new Response(JSON.stringify({ csrfToken: "csrf" })));
    await expect(pending).rejects.toThrow("session ended");
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
  it("binds popup source, origin, state and PKCE before code redemption", async () => {
    let authorize!: URL;
    const popup = {
      closed: false,
      close: vi.fn(),
      location: {
        replace: (url: string) => {
          authorize = new URL(url);
        },
      },
    };
    vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response(JSON.stringify(session())));
    const client = auth();
    const pending = client.authorize();
    await vi.waitFor(() => expect(authorize).toBeDefined());
    const data = {
      type: "jazz-inspector-code",
      code: "single-use",
      state: authorize.searchParams.get("state"),
    };
    const emit = (source: unknown, origin: string, payload = data) =>
      window.dispatchEvent(
        new MessageEvent("message", { source: source as Window, origin, data: payload }),
      );
    emit(popup, "https://untrusted.example");
    emit(window, window.location.origin);
    emit(popup, window.location.origin, { ...data, state: "wrong" });
    await Promise.resolve();
    expect(fetcher).not.toHaveBeenCalled();
    emit(popup, window.location.origin);
    await expect(pending).resolves.toMatchObject({ accessToken: "ephemeral" });
    const body = JSON.parse(fetcher.mock.calls[0]?.[1]?.body as string);
    expect(await proofChallenge(body.code_verifier)).toBe(
      authorize.searchParams.get("code_challenge"),
    );
    expect(body).toMatchObject({
      code: "single-use",
      redirect_uri: new URL("/inspector/callback", window.location.origin).href,
    });
    expect(fetcher.mock.calls[0]?.[1]).toMatchObject({ credentials: "omit", redirect: "error" });
    expect(authorize.href).not.toContain("ephemeral");
    expect(popup.close).toHaveBeenCalled();
  });
  it("removes callback code immediately and rejects unsafe or expired session metadata", () => {
    window.history.replaceState(null, "", "/inspector/callback?code=one-time&state=bound");
    expect(completeInspectorCallback()).toBe(true);
    expect(window.location.search).toBe("");
    expect(() => dashboardOrigin("https://dashboard.example/attacker")).toThrow();
    expect(() => validateSession({ ...session(), appId: "other" }, "app")).toThrow();
    expect(() =>
      validateSession({ ...session(), expiresAt: Math.floor(Date.now() / 1000) }, "app"),
    ).toThrow();
    expect(() =>
      validateSession({ ...session(), serverUrl: "https://root:secret@sync.example" }, "app"),
    ).toThrow();
    expect(() => validateSession({ ...session(), capabilities: ["root"] }, "app")).toThrow();
  });
  it("closes the popup if PKCE generation fails", async () => {
    const popup = { close: vi.fn() };
    vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);
    vi.spyOn(crypto.subtle, "digest").mockRejectedValueOnce(new Error("unavailable"));
    await expect(auth().authorize()).rejects.toThrow("unavailable");
    expect(popup.close).toHaveBeenCalledTimes(1);
  });
  it("rejects an unrequested privilege returned by renewal", async () => {
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrfToken: "csrf" })))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ ...session(), capabilities: ["inspector:read", "inspector:admin"] }),
        ),
      );
    await expect(auth().renew()).rejects.toThrow("Unexpected Inspector capability");
  });
});
