import { describe, expect, it } from "vitest";
import { BrokerAuthRefreshController } from "./broker-auth-refresh.js";

describe("BrokerAuthRefreshController", () => {
  it("keeps the newest request terminal when A/B results complete in reverse order", () => {
    const auth = new BrokerAuthRefreshController();
    const a = auth.request("tab-a", '{"jwt_token":"a"}', { sub: "alice" });
    auth.dispatch(1);
    const b = auth.request("tab-b", '{"jwt_token":"b"}', { sub: "alice" });
    auth.dispatch(1);

    expect(auth.acceptResult(b.generation, 1, "authenticated").accepted).toBe(true);
    expect(auth.acceptResult(a.generation, 1, "invalid", "late A").accepted).toBe(false);
    expect(auth.snapshot()).toMatchObject({
      generation: b.generation,
      state: "authenticated",
      authJson: '{"jwt_token":"b"}',
      confirmedLeadershipId: 1,
    });
  });

  it("rejects stale leaders and makes retained credentials pending for a replacement", () => {
    const auth = new BrokerAuthRefreshController();
    const request = auth.request("follower", '{"jwt_token":"fresh"}', { sub: "alice" });
    auth.dispatch(1);
    expect(auth.acceptResult(request.generation, 1, "authenticated").accepted).toBe(true);

    expect(auth.beginLeadership(2)).toMatchObject({
      generation: request.generation,
      state: "pending",
      authJson: '{"jwt_token":"fresh"}',
      confirmedLeadershipId: null,
    });
    expect(auth.acceptResult(request.generation, 1, "authenticated").accepted).toBe(false);
    expect(auth.dispatch(2)).toMatchObject({ dispatchedLeadershipId: 2 });
    expect(auth.acceptResult(request.generation, 2, "authenticated").accepted).toBe(true);
    expect(auth.snapshot()).toMatchObject({
      state: "authenticated",
      confirmedLeadershipId: 2,
    });
  });

  it("leaves a disconnected carrier pending until reconnect replay", () => {
    const auth = new BrokerAuthRefreshController();
    const request = auth.request("leader", "{}", { sub: "alice" });
    auth.dispatch(4);
    const deferred = auth.acceptResult(request.generation, 4, "deferred");
    expect(deferred).toMatchObject({ accepted: true, stateChanged: false });
    expect(auth.snapshot()).toMatchObject({ state: "pending", dispatchedLeadershipId: null });
    expect(auth.dispatch(4)).toMatchObject({ dispatchedLeadershipId: 4 });
  });

  it("retains an invalid same-principal request after its requester shuts down", () => {
    const auth = new BrokerAuthRefreshController();
    const request = auth.request("departing-follower", '{"jwt_token":"bad"}', {
      sub: "alice",
    });
    auth.dispatch(5);
    expect(auth.acceptResult(request.generation, 5, "invalid", "invalid").accepted).toBe(true);
    // There is intentionally no requester-liveness transition: namespace auth
    // belongs to the broker, not to the tab that happened to request it.
    expect(auth.snapshot()).toMatchObject({
      requesterTabId: "departing-follower",
      authJson: '{"jwt_token":"bad"}',
      state: "invalid",
    });
    expect(auth.beginLeadership(6)).toMatchObject({
      state: "pending",
      authJson: '{"jwt_token":"bad"}',
    });
  });

  it("does not allow equal-generation terminal callbacks to regress state", () => {
    const auth = new BrokerAuthRefreshController();
    const request = auth.request("tab", "{}", {});
    auth.dispatch(8);
    expect(auth.acceptResult(request.generation, 8, "authenticated").accepted).toBe(true);
    expect(auth.acceptResult(request.generation, 8, "invalid", "duplicate").accepted).toBe(false);
    expect(auth.acceptResult(request.generation, 8, "deferred").accepted).toBe(false);
    expect(auth.snapshot()).toMatchObject({ state: "authenticated", reason: undefined });
  });
});
