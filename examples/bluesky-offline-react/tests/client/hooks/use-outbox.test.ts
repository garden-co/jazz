import { describe, expect, it, vi } from "vitest";
import { deliverOperations } from "../../../src/hooks/use-outbox.js";

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

describe("outbox delivery", () => {
  it("sends intentions chronologically and stops at the failed operation", async () => {
    const request = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "Invalid post" }), { status: 400 }),
      );
    const markFailed = vi.fn();
    const reportApiReachable = vi.fn();

    await deliverOperations([later, earlier], { request, markFailed, reportApiReachable });

    expect(request.mock.calls.map(([, init]) => JSON.parse(String(init?.body)))).toEqual([
      [earlier],
      [later],
    ]);
    expect(markFailed).toHaveBeenCalledOnce();
    expect(markFailed).toHaveBeenCalledWith(later.id, {
      state: "failed",
      error: "Invalid post",
    });
    expect(reportApiReachable).toHaveBeenLastCalledWith(true);
  });

  it("keeps an intention queued when authentication can be refreshed", async () => {
    const request = vi
      .fn()
      .mockResolvedValue(
        new Response(JSON.stringify({ error: "Session expired" }), { status: 401 }),
      );
    const markFailed = vi.fn();
    const reportApiReachable = vi.fn();

    await deliverOperations([earlier], { request, markFailed, reportApiReachable });

    expect(markFailed).toHaveBeenCalledWith(earlier.id, {
      state: "queued",
      error: "Session expired",
    });
    expect(reportApiReachable).toHaveBeenLastCalledWith(true);
  });
});
