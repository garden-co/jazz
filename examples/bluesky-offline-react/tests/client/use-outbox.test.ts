import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  all: vi.fn(),
  update: vi.fn(),
  useEffect: vi.fn(),
}));

vi.mock("jazz-tools/react", () => ({
  useDb: () => ({ all: mocks.all, update: mocks.update }),
}));
vi.mock("react", () => ({
  useEffect: mocks.useEffect,
  useRef: <T>(value: T) => ({ current: value }),
}));

import { useOutbox } from "../../src/use-outbox.js";

beforeEach(() => {
  vi.restoreAllMocks();
  mocks.all.mockReset();
  mocks.update.mockReset();
  Object.defineProperty(globalThis, "navigator", { configurable: true, value: { onLine: true } });
});

describe("outbox delivery", () => {
  it("sends intentions chronologically and marks only the failed operation", async () => {
    const earlier = {
      id: "00000000-0000-0000-0000-000000000001",
      ownerDid: "did:plc:viewer",
      kind: "post",
      rkey: "earlier",
      payload: "{}",
      state: "queued",
      createdAt: "2026-07-16T10:00:00.000Z",
    };
    const later = {
      ...earlier,
      id: "00000000-0000-0000-0000-000000000002",
      rkey: "later",
      createdAt: "2026-07-16T10:00:01.000Z",
    };
    mocks.all.mockResolvedValue([later, earlier]);
    const fetch = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "Invalid post" }), { status: 400 }));
    const reportApiReachable = vi.fn();

    await useOutbox(earlier.ownerDid, true, reportApiReachable)();

    expect(fetch.mock.calls.map(([, init]) => JSON.parse(String(init?.body)))).toEqual([
      [earlier],
      [later],
    ]);
    expect(mocks.update).toHaveBeenCalledTimes(1);
    expect(mocks.update).toHaveBeenCalledWith(expect.anything(), later.id, {
      state: "failed",
      error: "Invalid post",
    });
    expect(reportApiReachable).toHaveBeenLastCalledWith(true);
  });
});
