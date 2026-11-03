import { describe, expect, it } from "vitest";
import { pinnedSocket, type CoreSocket } from "./create-jazz.js";

const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
const decode = (frame: Uint8Array) => JSON.parse(new TextDecoder().decode(frame));

function recorder() {
  const sent: Uint8Array[] = [];
  let closed = false;
  const socket: CoreSocket = {
    send(data) {
      sent.push(data);
      return true;
    },
    close() {
      closed = true;
    },
  };
  return { socket, sent, closed: () => closed };
}

describe("pinnedSocket", () => {
  it("replaces whatever bundle the browser stated with the backend's own", () => {
    const core = recorder();
    const pinned = pinnedSocket(core.socket, () => "ours");
    pinned.send(encode({ peer_identity: "someone", bundle: "theirs" }));
    pinned.send(encode({ bundle: "a later frame is not a prelude" }));
    expect(decode(core.sent[0]!)).toEqual({ peer_identity: "someone", bundle: "ours" });
    expect(decode(core.sent[1]!)).toEqual({ bundle: "a later frame is not a prelude" });
  });

  it.each([
    ["not JSON", new TextEncoder().encode("{not json")],
    ["not an object", encode(["bundle", "theirs"])],
    ["null", encode(null)],
  ])("closes instead of opening unpinned when the prelude is %s", (_label, prelude) => {
    const core = recorder();
    expect(pinnedSocket(core.socket, () => "ours").send(prelude)).toBe(false);
    expect(core.sent).toEqual([]);
    expect(core.closed()).toBe(true);
  });

  it("drops a stated bundle while none is published yet", () => {
    const core = recorder();
    pinnedSocket(core.socket, () => undefined).send(
      encode({ peer_identity: "x", bundle: "theirs" }),
    );
    expect(decode(core.sent[0]!)).toEqual({ peer_identity: "x" });
  });
});
