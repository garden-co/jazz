// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

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
    expect(postRoute).toHaveBeenCalledWith(
      { type: "jazz-inspector-overlay:route", route: "/settings" },
      window.location.origin,
    );
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
