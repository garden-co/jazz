// @vitest-environment jsdom
// The members dialog and the membership writes: one membership truth (Jazz's
// member rows), admission and removal as the plan says, and every failed or
// waiting state shown with its retry.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { db, resetWorld, world } from "./support/fake-jazz";

vi.mock("../jazz.generated", async () => ({ app: (await import("./support/fake-jazz")).fakeApp }));
vi.mock("jazz-tools/react", async () => (await import("./support/fake-jazz")).reactMock);

import { MembersDialog } from "../src/components/MembersDialog";
import { removeMember } from "../src/lib/membership";
import { ProfileDirectoryProvider } from "../src/lib/profiles";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const alice = { id: "p-alice", author: "alice-account", displayName: "Alice" };
const bobProfile = { id: "p-bob", author: "bob-account", displayName: "Bob" };
const member = (id: string, accountId: string) => ({
  id,
  scopeId: "scope",
  identifier: "room",
  accountId,
});

function open(members: unknown[] | undefined, requests: unknown[] = []) {
  world.answer = (q) => (q.table === "profiles" ? { data: [alice, bobProfile] } : undefined);
  return render(
    <ProfileDirectoryProvider me={alice as never}>
      <MembersDialog
        isOpen
        onOpenChange={() => undefined}
        roomId="room"
        author="alice-account"
        isCreator
        members={members as never}
        requests={requests as never}
      />
    </ProfileDirectoryProvider>,
  );
}

/** A transaction handle that records what is staged and answers reads from `rows`. */
function tx(rows: Record<string, unknown[]>) {
  const deleted: Array<[string, string]> = [];
  return {
    deleted,
    handle: {
      all: async (q: { table: string }) => rows[q.table] ?? [],
      delete: (table: { table: string }, id: string) => void deleted.push([table.table, id]),
      insert: vi.fn(),
    },
  };
}

beforeEach(resetWorld);
afterEach(cleanup);

describe("the members dialog lists Jazz's member rows", () => {
  it("shows an account with no profile as unknown and one with no roomMembers row by name", async () => {
    open([member("1", "alice-account"), member("2", "bob-account"), member("3", "stranger")]);
    expect(await screen.findByText("Bob")).toBeTruthy();
    expect(screen.getByText("Unknown")).toBeTruthy();
    expect(screen.getByText("3 members")).toBeTruthy();
  });
});

describe("admission", () => {
  const request = { id: "req", roomId: "room", requester: "bob-account", profileId: "p-bob" };

  it("inserts the member row and the roomMembers row in the transaction that clears the request, then delivers keys", async () => {
    open([member("1", "alice-account")], [request]);
    const staged = tx({});
    db.transaction.mockImplementation(async (run) => {
      run(staged.handle);
      return { wait: async () => undefined };
    });
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Admit" })));
    await waitFor(() => expect(db.encryption.keys.deliver).toHaveBeenCalledTimes(1));
    expect(staged.handle.insert.mock.calls.map((c) => (c[0] as { table: string }).table)).toEqual([
      "__e2ee_members",
      "roomMembers",
    ]);
    expect(staged.deleted).toEqual([["joinRequests", "req"]]);
  });

  it("says the key is not delivered when delivery reports a failure, and retries", async () => {
    db.encryption.keys.deliver.mockResolvedValueOnce({ delivered: 0, failed: 1 });
    open([member("1", "alice-account")], [request]);
    db.transaction.mockImplementation(async (run) => {
      run(tx({}).handle);
      return { wait: async () => undefined };
    });
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Admit" })));
    expect(await screen.findByText("Key not delivered yet")).toBeTruthy();
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Retry" })));
    await waitFor(() => expect(screen.queryByText("Key not delivered yet")).toBeNull());
    expect(db.encryption.keys.deliver).toHaveBeenCalledTimes(2);
  });

  it("reports rejected key delivery after admission and retries only the delivery", async () => {
    const wait = vi.fn(async () => undefined);
    db.transaction.mockImplementation(async (run) => {
      run(tx({}).handle);
      return { wait };
    });
    db.encryption.keys.deliver.mockRejectedValueOnce(new Error("Connection lost"));
    open([member("1", "alice-account")], [request]);

    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Admit" })));
    expect(await screen.findByText("Key not delivered yet")).toBeTruthy();
    expect(screen.getByText(/They are admitted.*Connection lost/)).toBeTruthy();
    expect(screen.queryByText(/Admission refused/)).toBeNull();
    expect(wait).toHaveBeenCalledTimes(1);

    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Retry" })));
    expect(screen.queryByText("Key not delivered yet")).toBeNull();
    expect(db.encryption.keys.deliver).toHaveBeenCalledTimes(2);
    expect(db.transaction).toHaveBeenCalledTimes(1);
  });

  it("shows a refused admission and keeps the request to try again", async () => {
    open([member("1", "alice-account")], [request]);
    db.transaction.mockImplementation(async () => ({
      wait: async () => Promise.reject(new Error("policy denied")),
    }));
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Admit" })));
    expect(await screen.findByText(/Admission refused: policy denied/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Admit" })).toBeTruthy();
    expect(db.encryption.keys.deliver).not.toHaveBeenCalled();
  });
});

describe("removal", () => {
  it("deletes the member row and the roomMembers row in one serializable transaction, then renews the key", async () => {
    const staged = tx({
      __e2ee_members: [member("2", "bob-account")],
      roomMembers: [{ id: "rm-bob" }],
    });
    db.serializableTransaction.mockImplementation(async (run) => {
      const count = await run(staged.handle);
      return { wait: async () => count };
    });
    open([member("1", "alice-account"), member("2", "bob-account")]);
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Remove" })));
    await waitFor(() => expect(db.encryption.keys.rotate).toHaveBeenCalledTimes(1));
    expect(staged.deleted).toEqual([
      ["__e2ee_members", "2"],
      ["roomMembers", "rm-bob"],
    ]);
    expect(db.transaction).not.toHaveBeenCalled();
    expect(db.encryption.keys.rotate).toHaveBeenCalledWith({ table: "rooms", id: "room" });
  });

  it("shows when the removal found no visible member rows", async () => {
    db.serializableTransaction.mockImplementation(async () => ({ wait: async () => 0 }));
    open([member("1", "alice-account"), member("2", "bob-account")]);
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Remove" })));
    expect(
      await screen.findByText(
        "No member rows were removed. You may not be able to see that member's rows.",
      ),
    ).toBeTruthy();
    expect(db.encryption.keys.rotate).not.toHaveBeenCalled();
  });

  it("renews the key after removal in a room with no message yet", async () => {
    db.serializableTransaction.mockImplementation(async () => ({ wait: async () => 1 }));
    db.all.mockResolvedValue([]);
    db.encryption.keys.rotate.mockResolvedValueOnce(0);
    open([member("1", "alice-account"), member("2", "bob-account")]);
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Remove" })));
    await waitFor(() => expect(db.encryption.keys.rotate).toHaveBeenCalledTimes(1));
    expect(db.encryption.keys.rotate).toHaveBeenCalledWith({ table: "rooms", id: "room" });
    expect(screen.queryByText(/key is not renewed yet/)).toBeNull();
  });

  it("does not renew the key when the removal is refused", async () => {
    db.serializableTransaction.mockRejectedValue(new Error("not allowed"));
    open([member("1", "alice-account"), member("2", "bob-account")]);
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Remove" })));
    expect(await screen.findByText(/Could not remove them: not allowed/)).toBeTruthy();
    expect(db.encryption.keys.rotate).not.toHaveBeenCalled();
  });

  it("says the member is removed but the key is not renewed when rotation fails, and retries", async () => {
    db.serializableTransaction.mockImplementation(async () => ({ wait: async () => 1 }));
    db.encryption.keys.rotate.mockRejectedValueOnce(new Error("offline"));
    open([member("1", "alice-account"), member("2", "bob-account")]);
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Remove" })));
    expect(await screen.findByText(/key is not renewed yet/)).toBeTruthy();
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Retry" })));
    await waitFor(() => expect(screen.queryByText(/key is not renewed yet/)).toBeNull());
    expect(db.encryption.keys.rotate).toHaveBeenCalledTimes(2);
  });

  it("shows a key-code rotation failure and offers a retry", async () => {
    db.serializableTransaction.mockImplementation(async () => ({ wait: async () => 1 }));
    db.encryption.keys.rotate.mockRejectedValueOnce(
      Object.assign(new Error("E2EE operation refused"), { code: "rotation-needs-key" }),
    );
    open([member("1", "alice-account"), member("2", "bob-account")]);
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Remove" })));
    expect(await screen.findByText(/key is not renewed yet/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Retry" })).toBeTruthy();
  });

  it("stops retrying a removal after three persistent conflicts", async () => {
    const { PersistedWriteRejectedError } = await import("jazz-tools");
    const conflict = Object.assign(Object.create(PersistedWriteRejectedError.prototype), {
      code: "serialization_conflict",
    });
    db.serializableTransaction.mockRejectedValue(conflict);
    open([member("1", "alice-account"), member("2", "bob-account")]);
    await act(async () => fireEvent.click(await screen.findByRole("button", { name: "Remove" })));
    expect(await screen.findByText(/Could not remove them:/)).toBeTruthy();
    expect(db.serializableTransaction).toHaveBeenCalledTimes(3);
  });
});

describe("a removal racing an admission of the same account", () => {
  it("runs again on fresh rows after a conflict, so the late member row goes too", async () => {
    const { PersistedWriteRejectedError } = await import("jazz-tools");
    const conflict = Object.assign(Object.create(PersistedWriteRejectedError.prototype), {
      code: "serialization_conflict",
    });
    const attempts: Array<Array<[string, string]>> = [];
    let admitted = false;
    db.serializableTransaction.mockImplementation(async (run) => {
      const staged = tx({
        __e2ee_members: admitted ? [member("late", "bob-account")] : [],
        roomMembers: admitted ? [{ id: "rm-late" }] : [],
      });
      const count = await run(staged.handle);
      attempts.push(staged.deleted);
      if (!admitted) {
        admitted = true; // the second admission commits first
        throw conflict;
      }
      return { wait: async () => count };
    });
    const removed = await removeMember({
      db: db as never,
      roomId: "room",
      accountId: "bob-account",
    });
    expect(removed).toBe(1);
    expect(attempts).toEqual([
      [],
      [
        ["__e2ee_members", "late"],
        ["roomMembers", "rm-late"],
      ],
    ]);
  });

  it("shows the refusal when the conflict does not clear", async () => {
    const { PersistedWriteRejectedError } = await import("jazz-tools");
    const conflict = Object.assign(Object.create(PersistedWriteRejectedError.prototype), {
      code: "serialization_conflict",
    });
    db.serializableTransaction.mockRejectedValue(conflict);
    await expect(
      removeMember({ db: db as never, roomId: "room", accountId: "bob-account" }),
    ).rejects.toBe(conflict);
    expect(db.serializableTransaction).toHaveBeenCalledTimes(3);
  });
});
