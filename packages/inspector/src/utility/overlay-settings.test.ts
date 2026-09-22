import { afterEach, describe, expect, it, vi } from "vitest";
import {
  requestCloseOverlay,
  requestDetachOverlay,
  setOverlayActiveRoute,
} from "./overlay-settings.js";

const initialUrl = window.location.href;

describe("requestDetachOverlay", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    window.history.replaceState(null, "", initialUrl);
    delete (window as unknown as Record<string, unknown>).__jazzInspectorOverlay;
  });

  it("detaches synchronously through the parent overlay", () => {
    const detach = vi.fn(() => true);
    (window as unknown as Record<string, unknown>).__jazzInspectorOverlay = { detach };
    const trigger = document.createElement("button");
    document.body.append(trigger);
    trigger.focus();

    requestDetachOverlay("/data-explorer/todos/data?filter=open");

    expect(detach).toHaveBeenCalledWith("/data-explorer/todos/data?filter=open");
    expect(document.activeElement).not.toBe(trigger);
    trigger.remove();
  });

  it("keeps focus when the popup is blocked", () => {
    const detach = vi.fn(() => false);
    (window as unknown as Record<string, unknown>).__jazzInspectorOverlay = { detach };
    const trigger = document.createElement("button");
    document.body.append(trigger);
    trigger.focus();

    requestDetachOverlay("/data-explorer");

    expect(document.activeElement).toBe(trigger);
    trigger.remove();
  });

  it("synchronizes the active route with the parent overlay", () => {
    const setActiveRoute = vi.fn();
    (window as unknown as Record<string, unknown>).__jazzInspectorOverlay = { setActiveRoute };

    setOverlayActiveRoute("/settings");

    expect(setActiveRoute).toHaveBeenCalledWith("/settings");
  });

  it("synchronizes a detached inspector route through its opener overlay", () => {
    const setActiveRoute = vi.fn();
    const previousOpener = window.opener;
    Object.defineProperty(window, "opener", {
      configurable: true,
      value: { __jazzInspectorOverlay: { setActiveRoute } },
    });
    window.history.replaceState(null, "", "?detached=1");

    setOverlayActiveRoute("/data-explorer/projects/data");

    expect(setActiveRoute).toHaveBeenCalledWith("/data-explorer/projects/data");
    Object.defineProperty(window, "opener", { configurable: true, value: previousOpener });
  });

  it("closes a detached inspector window", () => {
    const close = vi.spyOn(window, "close").mockImplementation(() => {});
    window.history.replaceState(null, "", "?detached=1");

    requestCloseOverlay();

    expect(close).toHaveBeenCalledOnce();
  });
});
