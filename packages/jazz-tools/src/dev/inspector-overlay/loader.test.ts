// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { installInspectorHost } from "./host-bridge.js";

vi.mock("./host-bridge.js", () => ({
  installInspectorHost: vi.fn(() => vi.fn()),
}));

class TestStyleSheet {
  replaceSync() {}
}

interface InspectorOverlayControl {
  detach(route: string): boolean;
  setActiveRoute(route: string): void;
}

describe("inspector overlay detached window", () => {
  beforeEach(() => {
    const values = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      clear: () => values.clear(),
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
    });
    vi.useFakeTimers();
    localStorage.setItem("jazz-inspector-overlay:open", "1");
    vi.stubGlobal("CSSStyleSheet", TestStyleSheet);
  });

  afterEach(() => {
    document.querySelector("jazz-inspector-overlay")?.remove();
    localStorage.clear();
    vi.useRealTimers();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("loads only on open, unloads on close, and restores the route on reopen", async () => {
    localStorage.setItem("jazz-inspector-overlay:open", "0");
    vi.mocked(installInspectorHost).mockClear();
    const { startInspectorOverlay } = await import("./loader.js");
    const db = {} as import("../../runtime/db.js").Db;
    startInspectorOverlay(db);
    const overlay = document.querySelector("jazz-inspector-overlay")!;
    const iframe = overlay.shadowRoot!.querySelector<HTMLIFrameElement>("iframe")!;
    expect(iframe.hasAttribute("src")).toBe(false);
    expect(installInspectorHost).not.toHaveBeenCalled();
    // jsdom doesn't create a browsing context for iframes inside shadow roots.
    const frameWindow = { postMessage: vi.fn() } as unknown as Window;
    Object.defineProperty(iframe, "contentWindow", { configurable: true, value: frameWindow });
    const shortcut = () =>
      window.dispatchEvent(
        new KeyboardEvent("keydown", { altKey: true, shiftKey: true, code: "KeyJ" }),
      );
    shortcut();
    expect(new URL(iframe.src).pathname).toBe("/__jazz/embedded/embedded.html");
    expect(installInspectorHost).toHaveBeenCalledOnce();
    const dispose = vi.mocked(installInspectorHost).mock.results[0]!.value;
    const control = (window as unknown as { __jazzInspectorOverlay: InspectorOverlayControl })
      .__jazzInspectorOverlay;
    control.setActiveRoute("/settings");
    startInspectorOverlay(db);
    expect(installInspectorHost).toHaveBeenCalledOnce();
    window.dispatchEvent(
      new MessageEvent("message", {
        origin: window.location.origin,
        source: frameWindow,
        data: { type: "jazz-inspector-overlay:close" },
      }),
    );
    expect(iframe.hasAttribute("src")).toBe(false);
    expect(dispose).toHaveBeenCalledOnce();
    shortcut();
    expect(new URL(iframe.src).searchParams.get("route")).toBe("/settings");
    expect(installInspectorHost).toHaveBeenCalledTimes(2);
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    expect(iframe.hasAttribute("src")).toBe(false);
  });

  it("opens at the dock size and restores the dock when the popup closes", async () => {
    const popup = {
      closed: false,
      close: vi.fn(),
      focus: vi.fn(),
    } as unknown as Window;
    const open = vi.spyOn(window, "open").mockReturnValue(popup);
    const { startInspectorOverlay } = await import("./loader.js");

    startInspectorOverlay({} as import("../../runtime/db.js").Db);
    const overlay = document.querySelector("jazz-inspector-overlay");
    const dock = overlay?.shadowRoot?.querySelector<HTMLElement>(".jzov-dock");
    const iframe = overlay?.shadowRoot?.querySelector<HTMLIFrameElement>(".jzov-frame");
    const postRoute = vi.fn();
    Object.defineProperty(iframe!, "contentWindow", {
      configurable: true,
      value: { postMessage: postRoute },
    });
    expect(dock?.dataset.open).toBe("true");
    vi.spyOn(dock!, "getBoundingClientRect").mockReturnValue({
      width: 1024,
      height: 420,
    } as DOMRect);

    let control = (window as unknown as { __jazzInspectorOverlay: InspectorOverlayControl })
      .__jazzInspectorOverlay;
    control.setActiveRoute("/settings");
    overlay?.remove();
    document.body.append(overlay!);
    control = (window as unknown as { __jazzInspectorOverlay: InspectorOverlayControl })
      .__jazzInspectorOverlay;
    window.dispatchEvent(
      new KeyboardEvent("keydown", { altKey: true, shiftKey: true, code: "KeyD" }),
    );

    expect(open).toHaveBeenCalledOnce();
    const url = new URL(String(open.mock.calls[0]?.[0]));
    expect(url.searchParams.get("detached")).toBe("1");
    expect(url.searchParams.get("route")).toBe("/settings");
    expect(open.mock.calls[0]?.[2]).toContain("width=1024,height=420");
    expect(dock?.dataset.open).toBe("false");
    expect(iframe?.hasAttribute("src")).toBe(false);
    expect(localStorage.getItem("jazz-inspector-overlay:open")).toBe("1");

    window.dispatchEvent(
      new KeyboardEvent("keydown", { altKey: true, shiftKey: true, code: "KeyJ" }),
    );
    expect(popup.focus).toHaveBeenCalledTimes(2);
    expect(dock?.dataset.open).toBe("false");

    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    expect(localStorage.getItem("jazz-inspector-overlay:open")).toBe("1");

    Object.defineProperty(popup, "closed", { value: true });
    window.dispatchEvent(
      new KeyboardEvent("keydown", { altKey: true, shiftKey: true, code: "KeyJ" }),
    );

    expect(dock?.dataset.open).toBe("true");
    expect(new URL(iframe!.src).searchParams.get("route")).toBe("/settings");
    expect(localStorage.getItem("jazz-inspector-overlay:open")).toBe("1");

    open.mockReturnValueOnce(null);
    expect(control.detach("/settings")).toBe(false);
    expect(dock?.dataset.open).toBe("true");

    const replacementPopup = {
      closed: false,
      close: vi.fn(),
      focus: vi.fn(),
    } as unknown as Window;
    open.mockReturnValue(replacementPopup);
    control.detach("/settings");
    window.dispatchEvent(new PageTransitionEvent("pagehide"));
    expect(replacementPopup.close).toHaveBeenCalledOnce();
    expect(dock?.dataset.open).toBe("true");
  });
});
