import { beforeEach, describe, expect, test } from "vitest";
import {
  Account,
  CoValueLoadingState,
  Group,
  co,
  deleteCoValues,
  z,
} from "../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { assertLoaded, waitFor } from "./utils.js";

beforeEach(async () => {
  await setupJazzTestSync();
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("deleteCoValues", () => {
  test("deletes a resolved graph (tombstones) and loads as DELETED", async () => {
    const Meta = co.map({
      tag: z.string(),
    });

    const Note = co.map({
      text: co.plainText(),
      meta: Meta,
    });

    const owner = Account.getMe();
    const group = Group.create(owner).makePublic("reader");

    const note = Note.create(
      {
        text: "hello",
        meta: { tag: "t1" },
      },
      group,
    );

    const text = note.text;
    const meta = note.meta;

    await note.$jazz.raw.core.waitForSync();

    await deleteCoValues(Note, note.$jazz.id, {
      loadAs: owner,
      resolve: { text: true, meta: true },
    });

    expect(note.$jazz.raw.core.isDeleted).toBe(true);
    expect(text.$jazz.raw.core.isDeleted).toBe(true);
    expect(meta.$jazz.raw.core.isDeleted).toBe(true);

    const viewer = await createJazzTestAccount();
    const loaded = await Note.load(note.$jazz.id, {
      loadAs: viewer,
      skipRetry: true,
    });

    expect(loaded.$isLoaded).toBe(false);
    expect(loaded.$jazz.loadingState).toBe(CoValueLoadingState.DELETED);
  });

  test("rejects deletion for non-admin on a group-owned value", async () => {
    const Meta = co.map({
      tag: z.string(),
    });

    const Note = co.map({
      meta: Meta,
    });

    const owner = Account.getMe();
    const writer = await createJazzTestAccount();

    const group = Group.create(owner);
    group.addMember(writer, "writer");

    const note = Note.create({ meta: { tag: "t1" } }, group);
    await note.$jazz.raw.core.waitForSync();

    await expect(
      deleteCoValues(Note, note.$jazz.id, {
        loadAs: writer,
        resolve: { meta: true },
      }),
    ).rejects.toThrow(/admin permissions/i);

    expect(note.$jazz.raw.core.isDeleted).toBe(false);
  });

  test("rejects deletion when a resolved child is not deletable (error includes path)", async () => {
    const Child = co.map({
      value: z.string(),
    });

    const Root = co.map({
      child: Child,
    });

    const owner = Account.getMe();
    const otherOwner = await createJazzTestAccount();

    const groupA = Group.create(owner).makePublic("reader");
    const groupB = Group.create({ owner: otherOwner }).makePublic("reader");

    const child = Child.create({ value: "child" }, groupB);
    await child.$jazz.raw.core.waitForSync();

    const root = Root.create({ child }, groupA);
    await root.$jazz.raw.core.waitForSync();

    await expect(
      deleteCoValues(Root, root.$jazz.id, {
        loadAs: owner,
        resolve: { child: true },
      }),
    ).rejects.toThrow(
      new RegExp(
        `Subscription starts from ${root.$jazz.id}.*path ${child.$jazz.id}`,
      ),
    );

    expect(root.$jazz.raw.core.isDeleted).toBe(false);
    expect(child.$jazz.raw.core.isDeleted).toBe(false);
  });

  test("rejects deletion when the value cannot be loaded", async () => {
    const Root = co.map({
      value: z.string(),
    });

    const owner = Account.getMe();
    const otherOwner = await createJazzTestAccount();
    const root = Root.create(
      { value: "root" },
      Group.create({ owner: otherOwner }),
    );
    await root.$jazz.raw.core.waitForSync();

    await expect(
      deleteCoValues(Root, root.$jazz.id, {
        loadAs: owner,
      }),
    ).rejects.toThrow(new RegExp(`Jazz Authorization Error`));

    expect(root.$jazz.raw.core.isDeleted).toBe(false);
  });

  test("rejects deletion when a child could not be loaded", async () => {
    const Child = co.map({
      value: z.string(),
    });

    const Root = co.map({
      child: Child,
    });

    const owner = Account.getMe();
    const otherOwner = await createJazzTestAccount();

    const groupA = Group.create(owner).makePublic("reader");
    const groupB = Group.create({ owner: otherOwner });

    const child = Child.create({ value: "child" }, groupB);
    await child.$jazz.raw.core.waitForSync();

    const root = Root.create({ child }, groupA);
    await root.$jazz.raw.core.waitForSync();

    await expect(
      deleteCoValues(Root, root.$jazz.id, {
        loadAs: owner,
        resolve: { child: true },
      }),
    ).rejects.toThrow(new RegExp(`Jazz Authorization Error`));

    expect(root.$jazz.raw.core.isDeleted).toBe(false);
    expect(child.$jazz.raw.core.isDeleted).toBe(false);
  });

  test("delete the CoValue when the child cannot be loaded but is marked with $onError", async () => {
    const Child = co.map({
      value: z.string(),
    });

    const Root = co.map({
      child: Child,
    });

    const owner = Account.getMe();
    const otherOwner = await createJazzTestAccount();

    const groupA = Group.create(owner).makePublic("reader");
    const groupB = Group.create({ owner: otherOwner });

    const child = Child.create({ value: "child" }, groupB);
    await child.$jazz.raw.core.waitForSync();

    const root = Root.create({ child }, groupA);
    await root.$jazz.raw.core.waitForSync();

    await deleteCoValues(Root, root.$jazz.id, {
      loadAs: owner,
      resolve: { child: { $onError: "catch" } },
    });

    expect(root.$jazz.raw.core.isDeleted).toBe(true);
    expect(child.$jazz.raw.core.isDeleted).toBe(false);
  });

  test("skips Account and Group CoValues", async () => {
    const me = Account.getMe();

    await deleteCoValues(Account, me.$jazz.id, {
      loadAs: me,
    });

    expect(me.$jazz.raw.core.isDeleted).toBe(false);

    const group = Group.create(me).makePublic("reader");

    await deleteCoValues(Group, group.$jazz.id, {
      loadAs: me,
    });

    expect(group.$jazz.raw.core.isDeleted).toBe(false);
  });
});
