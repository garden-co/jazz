// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { db, resetWorld } from "./support/fake-jazz";

vi.mock("../jazz.generated", async () => ({ app: (await import("./support/fake-jazz")).fakeApp }));
vi.mock("jazz-tools/react", async () => (await import("./support/fake-jazz")).reactMock);

import { NewRoomDialog } from "../src/components/NewRoomDialog";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
afterEach(cleanup);

it("creates one room when submitted twice while membership is pending", async () => {
  resetWorld();
  let resolve!: (value: { scopeId: string; identifier: string; accountId: string }) => void;
  db.encryption.members.row.mockImplementation(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const tx = {
    insert: vi.fn(),
    commit: vi.fn(() => ({ wait: vi.fn().mockResolvedValue(undefined) })),
    rollback: vi.fn(),
  };
  db.beginTransaction.mockReturnValue(tx as never);
  const onCreated = vi.fn();
  render(
    <NewRoomDialog
      isOpen
      onOpenChange={vi.fn()}
      author="alice"
      profile={{ id: "profile" } as never}
      onCreated={onCreated}
    />,
  );
  fireEvent.change(screen.getByLabelText(/Room name/), { target: { value: "Rehearsal" } });
  const form = screen.getByLabelText(/Room name/).closest("form")!;
  fireEvent.submit(form);
  fireEvent.submit(form);
  expect(db.encryption.members.row).toHaveBeenCalledTimes(1);
  await act(async () => resolve({ scopeId: "scope", identifier: "room", accountId: "alice" }));
  await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(1));
  expect(tx.commit).toHaveBeenCalledTimes(1);
});
