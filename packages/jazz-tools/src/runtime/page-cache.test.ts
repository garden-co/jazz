import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { holdAcrossPageCache, pageCacheForTest } from "./page-cache.js";

// One window for the file: the module listens on the first one it sees.
const page = new EventTarget();
const fire = (type: string, persisted = false) =>
  page.dispatchEvent(Object.assign(new Event(type), { persisted }));

function holder(log: string[], name: string, onReturn?: () => void | "stop" | Promise<void>) {
  return holdAcrossPageCache({
    leave: () => log.push(`${name} leaves`),
    return: async () => {
      log.push(`${name} returns`);
      return onReturn?.();
    },
  });
}

beforeAll(() => {
  vi.stubGlobal("window", page);
});

afterEach(() => {
  pageCacheForTest.reset();
});

describe("holdAcrossPageCache", () => {
  it("lets go at pagehide, once, and takes it again when the page comes back from the cache", async () => {
    const log: string[] = [];
    holder(log, "store");
    fire("pagehide", true);
    fire("pagehide", true);
    expect(log).toEqual(["store leaves"]);
    fire("pageshow", true);
    await pageCacheForTest.returned();
    expect(log).toEqual(["store leaves", "store returns"]);
  });

  it("lets go of nothing for a page that unloads instead", async () => {
    const log: string[] = [];
    holder(log, "store");
    fire("pagehide", false);
    expect(log).toEqual([]);
    expect(pageCacheForTest.isAway()).toBe(false);
  });

  it("ignores the first pageshow of a page that never left", async () => {
    const log: string[] = [];
    holder(log, "store");
    fire("pageshow", false);
    fire("pageshow", true);
    await pageCacheForTest.returned();
    expect(log).toEqual([]);
  });

  it("returns holders in the order they registered, each after the one before settled", async () => {
    const log: string[] = [];
    let storeHeld!: () => void;
    holder(
      log,
      "store",
      () =>
        new Promise<void>((resolve) => {
          storeHeld = () => {
            log.push("store held");
            resolve();
          };
        }),
    );
    holder(log, "connection");
    fire("pagehide", true);
    fire("pageshow", true);
    await vi.waitFor(() => expect(storeHeld).toBeDefined());
    expect(log).toEqual(["store leaves", "connection leaves", "store returns"]);
    storeHeld();
    await pageCacheForTest.returned();
    expect(log).toEqual([
      "store leaves",
      "connection leaves",
      "store returns",
      "store held",
      "connection returns",
    ]);
  });

  it("returns none of the later holders once one reloads the page instead", async () => {
    const log: string[] = [];
    holder(log, "store", () => "stop");
    holder(log, "connection");
    fire("pagehide", true);
    fire("pageshow", true);
    await pageCacheForTest.returned();
    expect(log).toEqual(["store leaves", "connection leaves", "store returns"]);
  });

  it("stops returning once the page left again, and returns the rest next time", async () => {
    const log: string[] = [];
    let storeHeld!: () => void;
    holder(log, "store", () => new Promise<void>((resolve) => (storeHeld = resolve)));
    holder(log, "connection");
    fire("pagehide", true);
    fire("pageshow", true);
    await vi.waitFor(() => expect(storeHeld).toBeDefined());
    fire("pagehide", true);
    storeHeld();
    await pageCacheForTest.returned();
    expect(log).toEqual([
      "store leaves",
      "connection leaves",
      "store returns",
      "store leaves",
      "connection leaves",
    ]);
    fire("pageshow", true);
    await vi.waitFor(() => expect(log.at(-1)).toBe("store returns"));
    storeHeld();
    await pageCacheForTest.returned();
    expect(log.slice(5)).toEqual(["store returns", "connection returns"]);
  });

  it("forgets a holder that unregistered", async () => {
    const log: string[] = [];
    const stop = holder(log, "store");
    stop();
    fire("pagehide", true);
    fire("pageshow", true);
    await pageCacheForTest.returned();
    expect(log).toEqual([]);
  });

  it("keeps returning the others when one holder throws", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const log: string[] = [];
    holder(log, "store", () => {
      throw new Error("lock manager gone");
    });
    holder(log, "connection");
    fire("pagehide", true);
    fire("pageshow", true);
    await pageCacheForTest.returned();
    expect(log).toEqual([
      "store leaves",
      "connection leaves",
      "store returns",
      "connection returns",
    ]);
    expect(warn).toHaveBeenCalledOnce();
    warn.mockRestore();
  });
});
