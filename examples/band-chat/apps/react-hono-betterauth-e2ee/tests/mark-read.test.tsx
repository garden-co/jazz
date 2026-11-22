// @vitest-environment jsdom
// A read marker is written only after this browser decrypted the room's newest
// message: the decrypted row's ID is the plain newest-message query's.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { db, resetWorld } from "./support/fake-jazz";

vi.mock("../jazz.generated", async () => ({ app: (await import("./support/fake-jazz")).fakeApp }));
vi.mock("jazz-tools/react", async () => (await import("./support/fake-jazz")).reactMock);

import { useMarkRead } from "../src/lib/mark-read";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

type Props = Parameters<typeof useMarkRead>[0];
function Reader(props: Props) {
  useMarkRead(props);
  return null;
}
const newest = { id: "m3", $createdAt: new Date("2026-01-03T00:00:00Z") };
const props = (over: Partial<Props>): Props => ({
  roomId: "room",
  author: "alice-account",
  newest,
  decryptedIds: new Set(["m3", "m2"]),
  markers: [],
  markersUnreadable: 0,
  ...over,
});
const writes = () => db.insert.mock.calls.length + db.update.mock.calls.length;

beforeEach(resetWorld);
afterEach(cleanup);

describe("read marker writes", () => {
  it("writes nothing when the reader has no marker and the newest message is locked", () => {
    render(<Reader {...props({ decryptedIds: new Set() })} />);
    expect(writes()).toBe(0);
  });

  it("writes nothing when the newest message is locked and an older one is readable", () => {
    render(<Reader {...props({ decryptedIds: new Set(["m1", "m2"]) })} />);
    expect(writes()).toBe(0);
  });

  it("writes nothing for a room with no message, or before the messages have loaded", () => {
    render(<Reader {...props({ newest: null, decryptedIds: new Set() })} />);
    render(<Reader {...props({ decryptedIds: undefined })} />);
    expect(writes()).toBe(0);
  });

  it("writes nothing when a marker exists that this browser cannot read", () => {
    render(<Reader {...props({ markersUnreadable: 1 })} />);
    expect(writes()).toBe(0);
  });

  it("writes one marker when the newest message is decrypted and newer than the marker", () => {
    render(<Reader {...props({})} />);
    expect(db.insert).toHaveBeenCalledTimes(1);
    const row = db.insert.mock.calls[0]![1] as { roomId: string; reader: string; lastReadAt: Date };
    expect(row.roomId).toBe("room");
    expect(row.lastReadAt.getTime()).toBeGreaterThanOrEqual(newest.$createdAt.getTime());
  });

  it("moves an existing older marker and leaves a current one alone", () => {
    const { unmount } = render(
      <Reader
        {...props({ markers: [{ id: "k", lastReadAt: new Date("2026-01-01T00:00:00Z") }] })}
      />,
    );
    expect(db.update).toHaveBeenCalledTimes(1);
    expect(db.update.mock.calls[0]![1]).toBe("k");
    unmount();
    db.update.mockClear();
    render(
      <Reader
        {...props({ markers: [{ id: "k", lastReadAt: new Date("2026-02-01T00:00:00Z") }] })}
      />,
    );
    expect(writes()).toBe(0);
  });

  it("does not retry a refused write until a newer message arrives", async () => {
    db.insert.mockImplementation(() => ({
      wait: async () => Promise.reject(new Error("refused")),
    }));
    const view = render(<Reader {...props({})} />);
    await Promise.resolve();
    view.rerender(<Reader {...props({ markers: [] })} />);
    expect(db.insert).toHaveBeenCalledTimes(1);
    const later = { id: "m4", $createdAt: new Date("2026-01-04T00:00:00Z") };
    view.rerender(<Reader {...props({ newest: later, decryptedIds: new Set(["m4"]) })} />);
    expect(db.insert).toHaveBeenCalledTimes(2);
  });
});
