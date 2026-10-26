import { Cause, Effect, Exit } from "effect";
import { describe, expect, it, vi } from "vitest";
import type { Db, Transaction } from "../runtime/db.js";
import { Jazz, type JazzTransaction } from "./jazz.js";

vi.mock("../runtime/default-create-db.js", () => ({ createDb: vi.fn() }));

// This suite checks the Effect adapter's ownership of an opened transaction;
// jazz.test.ts covers the real database's commit and rollback behaviour.
describe.each(["mergeable", "serializable"] as const)("%s Effect transaction", (kind) => {
  it.each([
    { failure: "throw", rollbackFails: false },
    { failure: "die", rollbackFails: false },
    { failure: "throw", rollbackFails: true },
  ])(
    "rolls back and preserves the body defect ($failure, rollback fails: $rollbackFails)",
    async ({ failure, rollbackFails }) => {
      const defect = new Error("constructing the body failed");
      const rollbackError = new Error("rollback failed");
      const staged: unknown[] = [];
      const insert = vi.fn((_table: unknown, data: unknown) => staged.push(data));
      const rollback = vi.fn(async () => {
        if (rollbackFails) throw rollbackError;
        staged.length = 0;
        return true;
      });
      const commit = vi.fn();
      const transaction = { rollback, commit, insert } as unknown as Transaction;
      const beginTransaction = vi.fn(() => transaction);
      const beginSerializableTransaction = vi.fn(() => transaction);
      const jazz = Jazz.fromDb({
        beginTransaction,
        beginSerializableTransaction,
      } as unknown as Db);
      const body = vi.fn((tx: JazzTransaction): Effect.Effect<never> => {
        tx.transaction.insert({} as never, { title: "discarded" });
        if (failure === "throw") throw defect;
        return Effect.die(defect);
      });
      const program =
        kind === "mergeable" ? jazz.transaction(body) : jazz.serializableTransaction(body);

      // Construction is lazy, including acquisition of the core transaction.
      expect(body).not.toHaveBeenCalled();
      expect(beginTransaction).not.toHaveBeenCalled();
      expect(beginSerializableTransaction).not.toHaveBeenCalled();

      const exit = await Effect.runPromiseExit(program);

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(defect);
      expect(body).toHaveBeenCalledOnce();
      expect(
        kind === "mergeable" ? beginTransaction : beginSerializableTransaction,
      ).toHaveBeenCalledOnce();
      expect(
        kind === "mergeable" ? beginSerializableTransaction : beginTransaction,
      ).not.toHaveBeenCalled();
      expect(rollback).toHaveBeenCalledOnce();
      expect(commit).not.toHaveBeenCalled();
      expect(insert).toHaveBeenCalledOnce();
      if (!rollbackFails) expect(staged).toEqual([]);
    },
  );

  it("still rolls back when the running body is interrupted", async () => {
    const rollback = vi.fn(async () => true);
    const commit = vi.fn();
    const begin = () => ({ rollback, commit });
    const jazz = Jazz.fromDb({
      beginTransaction: begin,
      beginSerializableTransaction: begin,
    } as unknown as Db);
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const body = () => Effect.andThen(Effect.sync(entered), Effect.never);
    const program =
      kind === "mergeable" ? jazz.transaction(body) : jazz.serializableTransaction(body);
    const controller = new AbortController();
    const completed = Effect.runPromiseExit(program, { signal: controller.signal });

    await started;
    controller.abort();
    const exit = await completed;

    expect(Exit.isFailure(exit)).toBe(true);
    expect(rollback).toHaveBeenCalledOnce();
    expect(commit).not.toHaveBeenCalled();
  });
});
