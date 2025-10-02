import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, beforeEach, describe, expect, test } from "vitest";
import { CoMap, Group, z } from "../exports.js";
import {
  Account,
  Loaded,
  MaybeLoaded,
  Ref,
  co,
  CoValueLoadingState,
} from "../internal.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import {
  assertLoaded,
  loadCoValueOrFail,
  setupTwoNodes,
  waitFor,
} from "./utils.js";

const Crypto = await WasmCrypto.create();

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("Custom accounts and groups", async () => {
  test("Custom account and group", async () => {
    const CustomProfile = co.profile({
      name: z.string(),
      color: z.string(),
    });

    const Root = co.map({});
    const CustomAccount = co
      .account({
        profile: CustomProfile,
        root: Root,
      })
      .withMigration((account, creationProps?: { name: string }) => {
        // making sure that the inferred type of account.root & account.profile considers the root/profile not being loaded
        type R = typeof account.root;
        const _r: R = {} as MaybeLoaded<Loaded<typeof Root>>;
        type P = typeof account.profile;
        const _p: P = {} as MaybeLoaded<Loaded<typeof CustomProfile>>;
        if (creationProps) {
          const profileGroup = Group.create({ owner: account });
          profileGroup.addMember("everyone", "reader");
          account.$jazz.set(
            "profile",
            CustomProfile.create(
              { name: creationProps.name, color: "blue" },
              profileGroup,
            ),
          );
        }
      });

    const me = await createJazzTestAccount({
      creationProps: { name: "Hermes Puggington" },
      isCurrentActiveAccount: true,
      AccountSchema: CustomAccount,
    });

    expect(me.profile).toBeDefined();
    expect(me.profile?.name).toBe("Hermes Puggington");
    expect(me.profile?.color).toBe("blue");

    const group = Group.create({ owner: me });
    group.addMember("everyone", "reader");

    expect(group.members).toMatchObject([{ id: me.$jazz.id, role: "admin" }]);

    const meAsMember = group.members.find(
      (member) => member.id === me.$jazz.id,
    );
    const memberAccount = meAsMember?.account;
    assert(memberAccount);
    assertLoaded(memberAccount.profile);
    expect(memberAccount.profile.name).toBe("Hermes Puggington");
  });
});

describe("Group inheritance", () => {
  const TestMap = co.map({
    title: z.string(),
  });

  test("Group inheritance", async () => {
    const me = await co.account().create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

    const parentGroup = Group.create({ owner: me });
    const group = Group.create({ owner: me });

    group.addMember(parentGroup);

    const reader = await co.account().createAs(me, {
      creationProps: { name: "Reader" },
    });

    parentGroup.addMember(reader, "reader");

    const mapInChild = TestMap.create({ title: "In Child" }, { owner: group });

    const mapAsReader = await TestMap.load(mapInChild.$jazz.id, {
      loadAs: reader,
    });
    assertLoaded(mapAsReader);
    expect(mapAsReader.title).toBe("In Child");

    await parentGroup.removeMember(reader);

    mapInChild.$jazz.set("title", "In Child (updated)");

    await waitFor(async () => {
      const mapAsReaderAfterUpdate = await TestMap.load(mapInChild.$jazz.id, {
        loadAs: reader,
      });
      expect(mapAsReaderAfterUpdate.$jazzState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });
  });

  test("Group inheritance with grand-children", async () => {
    const me = await co.account().create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

    const grandParentGroup = Group.create({ owner: me });
    const parentGroup = Group.create({ owner: me });
    const group = Group.create({ owner: me });

    group.addMember(parentGroup);
    parentGroup.addMember(grandParentGroup);

    const reader = await co.account().createAs(me, {
      creationProps: { name: "Reader" },
    });

    grandParentGroup.addMember(reader, "reader");

    const mapInGrandChild = TestMap.create(
      { title: "In Grand Child" },
      { owner: group },
    );

    const mapAsReader = await TestMap.load(mapInGrandChild.$jazz.id, {
      loadAs: reader,
    });
    assertLoaded(mapAsReader);
    expect(mapAsReader.title).toBe("In Grand Child");

    await grandParentGroup.removeMember(reader);

    await grandParentGroup.$jazz.waitForSync();

    mapInGrandChild.$jazz.set("title", "In Grand Child (updated)");

    const mapAsReaderAfterUpdate = await TestMap.load(
      mapInGrandChild.$jazz.id,
      {
        loadAs: reader,
      },
    );
    expect(mapAsReaderAfterUpdate.$jazzState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
  });

  test("Group.getParentGroups should return the parent groups", async () => {
    const me = await co.account().create({
      creationProps: { name: "Test Owner" },
      crypto: Crypto,
    });

    const grandParentGroup = Group.create({ owner: me });
    const parentGroup = Group.create({ owner: me });
    const childGroup = Group.create({ owner: me });

    childGroup.addMember(parentGroup);
    parentGroup.addMember(grandParentGroup);

    const parentGroups = childGroup.getParentGroups();

    expect(parentGroups).toHaveLength(1);
    expect(parentGroups).toContainEqual(
      expect.objectContaining({
        $jazz: expect.objectContaining({ id: parentGroup.$jazz.id }),
      }),
    );

    expect(parentGroups[0]?.getParentGroups()).toContainEqual(
      expect.objectContaining({
        $jazz: expect.objectContaining({ id: grandParentGroup.$jazz.id }),
      }),
    );
  });

  test("waitForSync should resolve when the value is uploaded", async () => {
    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const group = Group.create({ owner: clientAccount });

    await group.$jazz.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the map from it
    clientNode.gracefulShutdown();

    const loadedGroup = await serverNode.load(group.$jazz.raw.id);

    expect(loadedGroup).not.toBe(CoValueLoadingState.UNAVAILABLE);
  });

  test("everyone is valid only for reader, writer and writeOnly roles", () => {
    const group = Group.create();
    group.addMember("everyone", "reader");

    expect(group.getRoleOf("everyone")).toBe("reader");

    group.addMember("everyone", "writer");

    expect(group.getRoleOf("everyone")).toBe("writer");

    // @ts-expect-error - admin is not a valid role for everyone
    expect(() => group.addMember("everyone", "admin")).toThrow();

    expect(group.getRoleOf("everyone")).toBe("writer");

    group.addMember("everyone", "writeOnly");

    expect(group.getRoleOf("everyone")).toBe("writeOnly");
  });

  test("makePublic should add everyone as a reader", () => {
    const group = Group.create();
    group.makePublic();
    expect(group.getRoleOf("everyone")).toBe("reader");
  });

  test("makePublic should add everyone as a writer", () => {
    const group = Group.create();
    group.makePublic("writer");
    expect(group.getRoleOf("everyone")).toBe("writer");
  });

  test("typescript should show an error when adding a member with a non-account role", async () => {
    const account = await createJazzTestAccount({});
    await account.$jazz.waitForAllCoValuesSync();

    const group = Group.create();

    // @ts-expect-error - Even though readerInvite is a valid role for an account, we don't allow it to not create confusion when using the intellisense
    group.addMember(account, "readerInvite");
    // @ts-expect-error - Only groups can have an `inherit` role, not accounts
    group.addMember(account, "inherit");
    // @ts-expect-error - Only groups can be added without a role, not accounts
    group.addMember(account, undefined);

    expect(group.members).not.toContainEqual(
      expect.objectContaining({
        id: account.$jazz.id,
        role: "readerInvite",
      }),
    );

    expect(group.getRoleOf(account.$jazz.id)).toBe("readerInvite");
  });

  test("adding a group member as writeOnly should fail", async () => {
    const account = await createJazzTestAccount({});
    await account.$jazz.waitForAllCoValuesSync();

    const parentGroup = Group.create();
    const group = Group.create();
    expect(() => {
      // @ts-expect-error
      group.addMember(parentGroup, "writeOnly");
    }).toThrow();
  });

  test("Removing member group", async () => {
    const alice = await createJazzTestAccount({});
    await alice.$jazz.waitForAllCoValuesSync();
    const bob = await createJazzTestAccount({});
    await bob.$jazz.waitForAllCoValuesSync();

    const loadedAlice = await Account.load(alice.$jazz.id);
    const loadedBob = await Account.load(bob.$jazz.id);

    assertLoaded(loadedBob);
    assertLoaded(loadedAlice);

    const parentGroup = Group.create();
    // `parentGroup` has `alice` as a writer
    parentGroup.addMember(loadedAlice, "writer");
    expect(parentGroup.getRoleOf(alice.$jazz.id)).toBe("writer");

    const group = Group.create();
    // `group` has `bob` as a reader
    group.addMember(loadedBob, "reader");
    expect(group.getRoleOf(bob.$jazz.id)).toBe("reader");

    group.addMember(parentGroup);
    // `group` has `parentGroup`'s members (in this case, `alice` as a writer)
    expect(group.getRoleOf(bob.$jazz.id)).toBe("reader");
    expect(group.getRoleOf(alice.$jazz.id)).toBe("writer");

    // `group` no longer has `parentGroup`'s members
    await group.removeMember(parentGroup);
    expect(group.getRoleOf(bob.$jazz.id)).toBe("reader");
    expect(group.getRoleOf(alice.$jazz.id)).toBe(undefined);
  });

  describe("when creating nested CoValues from a JSON object", () => {
    const Task = co.plainText();
    const Column = co.list(Task);
    const Board = co.map({
      title: z.string(),
      columns: co.list(Column),
    });

    let board: ReturnType<typeof Board.create>;

    beforeEach(async () => {
      const me = co.account().getMe();
      const writeAccess = Group.create();
      writeAccess.addMember(me, "writer");

      board = Board.create(
        {
          title: "My board",
          columns: [
            ["Task 1.1", "Task 1.2"],
            ["Task 2.1", "Task 2.2"],
          ],
        },
        writeAccess,
      );
    });

    test("nested CoValues inherit permissions from the referencing CoValue", async () => {
      const me = co.account().getMe();
      const task = board.columns[0]![0]!;

      const boardAsWriter = await Board.load(board.$jazz.id, { loadAs: me });
      assertLoaded(boardAsWriter);
      expect(boardAsWriter.title).toEqual("My board");
      const taskAsWriter = await Task.load(task.$jazz.id, { loadAs: me });
      // TODO check why assertLoaded is not being required here
      expect(taskAsWriter?.toString()).toEqual("Task 1.1");
    });

    test("nested CoValues inherit permissions from the referencing CoValue", async () => {
      const me = co.account().getMe();
      const reader = await co.account().createAs(me, {
        creationProps: { name: "Reader" },
      });

      const task = board.columns[0]![0]!;
      const taskGroup = task.$jazz.owner;
      taskGroup.addMember(reader, "reader");

      const taskAsReader = await Task.load(task.$jazz.id, { loadAs: reader });
      expect(taskAsReader?.toString()).toEqual("Task 1.1");
      const boardAsReader = await Board.load(board.$jazz.id, {
        loadAs: reader,
      });
      expect(boardAsReader.$jazzState).toBe(CoValueLoadingState.UNAUTHORIZED);
    });
  });
});

describe("Group.getRoleOf", () => {
  beforeEach(async () => {
    await createJazzTestAccount({ isCurrentActiveAccount: true });
  });

  test("returns correct role for admin", async () => {
    const group = Group.create();
    const admin = await createJazzTestAccount({});
    await admin.$jazz.waitForAllCoValuesSync();
    group.addMember(admin, "admin");
    expect(group.getRoleOf(admin.$jazz.id)).toBe("admin");
    expect(group.getRoleOf("me")).toBe("admin");
  });

  test("returns correct role for writer", async () => {
    const group = Group.create();
    const writer = await createJazzTestAccount({});
    await writer.$jazz.waitForAllCoValuesSync();
    group.addMember(writer, "writer");
    expect(group.getRoleOf(writer.$jazz.id)).toBe("writer");
  });

  test("returns correct role for reader", async () => {
    const group = Group.create();
    const reader = await createJazzTestAccount({});
    await reader.$jazz.waitForAllCoValuesSync();
    group.addMember(reader, "reader");
    expect(group.getRoleOf(reader.$jazz.id)).toBe("reader");
  });

  test("returns correct role for writeOnly", async () => {
    const group = Group.create();
    const writeOnly = await createJazzTestAccount({});
    await writeOnly.$jazz.waitForAllCoValuesSync();
    group.addMember(writeOnly, "writeOnly");
    expect(group.getRoleOf(writeOnly.$jazz.id)).toBe("writeOnly");
  });

  test("returns correct role for everyone", () => {
    const group = Group.create();
    group.addMember("everyone", "reader");
    expect(group.getRoleOf("everyone")).toBe("reader");
  });
});

describe("Group.getRoleOf with 'me' parameter", () => {
  beforeEach(async () => {
    await createJazzTestAccount({ isCurrentActiveAccount: true });
  });

  test("returns correct role for 'me' when current account is admin", () => {
    const group = Group.create();
    expect(group.getRoleOf("me")).toBe("admin");
  });

  test("returns correct role for 'me' when current account is writer", async () => {
    const account = await createJazzTestAccount();
    await account.$jazz.waitForAllCoValuesSync();
    const group = Group.create({ owner: account });

    group.addMember(co.account().getMe(), "writer");

    expect(group.getRoleOf("me")).toBe("writer");
  });

  test("returns correct role for 'me' when current account is reader", async () => {
    const account = await createJazzTestAccount();
    await account.$jazz.waitForAllCoValuesSync();
    const group = Group.create({ owner: account });

    group.addMember(co.account().getMe(), "reader");

    expect(group.getRoleOf("me")).toBe("reader");
  });

  test("returns undefined for 'me' when current account has no role", async () => {
    const account = await createJazzTestAccount();
    await account.$jazz.waitForAllCoValuesSync();
    const group = Group.create({ owner: account });

    expect(group.getRoleOf("me")).toBeUndefined();
  });
});

describe("Account permissions", () => {
  beforeEach(async () => {
    await createJazzTestAccount({ isCurrentActiveAccount: true });
  });

  test("getRoleOf returns admin only for self and me", async () => {
    const account = await co.account().create({
      creationProps: { name: "Test Account" },
      crypto: Crypto,
    });

    // Account should be admin of itself
    expect(account.getRoleOf(account.$jazz.id)).toBe("admin");

    // The GlobalMe is not this account
    expect(account.getRoleOf("me")).toBe(undefined);
    expect(co.account().getMe().getRoleOf("me")).toBe("admin");

    // Other accounts should have no role
    const otherAccount = await co.account().create({
      creationProps: { name: "Other Account" },
      crypto: Crypto,
    });
    expect(account.getRoleOf(otherAccount.$jazz.id)).toBeUndefined();

    // Everyone should have no role
    expect(account.getRoleOf("everyone")).toBeUndefined();
  });
});

describe("Account permissions", () => {
  test("canRead permissions for different roles", async () => {
    // Create test accounts
    const admin = await co.account().create({
      creationProps: { name: "Admin" },
      crypto: Crypto,
    });

    const group = Group.create({ owner: admin });
    const testObject = CoMap.create({}, { owner: group });

    const writer = await co.account().createAs(admin, {
      creationProps: { name: "Writer" },
    });
    const reader = await co.account().createAs(admin, {
      creationProps: { name: "Reader" },
    });
    const writeOnly = await co.account().createAs(admin, {
      creationProps: { name: "WriteOnly" },
    });

    // Set up roles
    group.addMember(writer, "writer");
    group.addMember(reader, "reader");
    group.addMember(writeOnly, "writeOnly");

    // Test canRead permissions
    expect(admin.canRead(testObject)).toBe(true);
    expect(writer.canRead(testObject)).toBe(true);
    expect(reader.canRead(testObject)).toBe(true);
    expect(writeOnly.canRead(testObject)).toBe(true);
  });

  test("canWrite permissions for different roles", async () => {
    // Create test accounts
    const admin = await co.account().create({
      creationProps: { name: "Admin" },
      crypto: Crypto,
    });

    const group = Group.create({ owner: admin });
    const testObject = CoMap.create({}, { owner: group });

    const writer = await co.account().createAs(admin, {
      creationProps: { name: "Writer" },
    });
    const reader = await co.account().createAs(admin, {
      creationProps: { name: "Reader" },
    });
    const writeOnly = await co.account().createAs(admin, {
      creationProps: { name: "WriteOnly" },
    });

    // Set up roles
    group.addMember(writer, "writer");
    group.addMember(reader, "reader");
    group.addMember(writeOnly, "writeOnly");

    // Test canWrite permissions
    expect(admin.canWrite(testObject)).toBe(true);
    expect(writer.canWrite(testObject)).toBe(true);
    expect(reader.canWrite(testObject)).toBe(false);
    expect(writeOnly.canWrite(testObject)).toBe(true);
  });

  test("canAdmin permissions for different roles", async () => {
    // Create test accounts
    const admin = await co.account().create({
      creationProps: { name: "Admin" },
      crypto: Crypto,
    });

    const group = Group.create({ owner: admin });
    const testObject = CoMap.create({}, { owner: group });

    const writer = await co.account().createAs(admin, {
      creationProps: { name: "Writer" },
    });
    const reader = await co.account().createAs(admin, {
      creationProps: { name: "Reader" },
    });
    const writeOnly = await co.account().createAs(admin, {
      creationProps: { name: "WriteOnly" },
    });

    // Set up roles
    group.addMember(writer, "writer");
    group.addMember(reader, "reader");
    group.addMember(writeOnly, "writeOnly");

    // Test canAdmin permissions
    expect(admin.canAdmin(testObject)).toBe(true);
    expect(writer.canAdmin(testObject)).toBe(false);
    expect(reader.canAdmin(testObject)).toBe(false);
    expect(writeOnly.canAdmin(testObject)).toBe(false);
  });

  test("permissions for non-members", async () => {
    const admin = await co.account().create({
      creationProps: { name: "Admin" },
      crypto: Crypto,
    });

    const group = Group.create({ owner: admin });
    const testObject = CoMap.create({}, { owner: group });

    const nonMember = await co.account().createAs(admin, {
      creationProps: { name: "NonMember" },
    });

    // Test permissions for non-member
    expect(nonMember.canRead(testObject)).toBe(false);
    expect(nonMember.canWrite(testObject)).toBe(false);
    expect(nonMember.canAdmin(testObject)).toBe(false);
  });

  describe("permissions over Groups and Accounts", () => {
    describe("read", () => {
      test("can read all Accounts", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        expect(account.canRead(otherAccount)).toBe(true);
      });

      test("can read all groups", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const group = Group.create();

        expect(account.canRead(group)).toBe(true);
      });
    });

    describe("write", () => {
      test("can write Account if it's itself", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        expect(account.canWrite(account)).toBe(true);
      });

      test("cannot write other accounts", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        expect(account.canWrite(otherAccount)).toBe(false);
      });

      test("can write Group if it's a writer for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        group.addMember(account, "writer");

        expect(account.canWrite(group)).toBe(true);
      });

      test("can write Group if it's an admin for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        group.addMember(account, "admin");

        expect(account.canWrite(group)).toBe(true);
      });

      test("cannot write Group if it has writeOnly permissions for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        group.addMember(account, "writeOnly");

        expect(account.canWrite(group)).toBe(false);
      });

      test("cannot write Group if it's a reader for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        group.addMember(account, "reader");

        expect(account.canWrite(group)).toBe(false);
      });

      test("cannot write Group if it has no permissions for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        expect(account.canWrite(group)).toBe(false);
      });
    });

    describe("admin", () => {
      test("can admin Account if it's itself", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        expect(account.canAdmin(account)).toBe(true);
      });

      test("cannot admin other accounts", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        expect(account.canAdmin(otherAccount)).toBe(false);
      });

      test("can admin Group if it's an admin for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        group.addMember(account, "admin");

        expect(account.canAdmin(group)).toBe(true);
      });

      test("cannot admin Group if it's a writer for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        group.addMember(account, "writer");

        expect(account.canAdmin(group)).toBe(false);
      });

      test("cannot admin Group if it has writeOnly permissions for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        group.addMember(account, "writeOnly");

        expect(account.canAdmin(group)).toBe(false);
      });

      test("cannot write Group if it's a reader for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        group.addMember(account, "reader");

        expect(account.canAdmin(group)).toBe(false);
      });

      test("cannot write Group if it has no permissions for that group", async () => {
        const account = await co.account().create({
          creationProps: { name: "Test Account" },
          crypto: Crypto,
        });
        const otherAccount = await co.account().create({
          creationProps: { name: "Other Account" },
          crypto: Crypto,
        });
        const group = Group.create({ owner: otherAccount });

        expect(account.canAdmin(group)).toBe(false);
      });
    });
  });
});

describe("Group.members", () => {
  test("should return the members of the group", async () => {
    const childGroup = Group.create();

    const bob = await createJazzTestAccount({});
    await bob.$jazz.waitForAllCoValuesSync();

    childGroup.addMember(bob, "reader");
    expect(childGroup.getRoleOf(bob.$jazz.id)).toBe("reader");

    await waitFor(() => {
      expect(childGroup.members).toEqual([
        expect.objectContaining({
          account: expect.objectContaining({
            $jazz: expect.objectContaining({
              id: co.account().getMe().$jazz.id,
            }),
          }),
          role: "admin",
        }),
        expect.objectContaining({
          account: expect.objectContaining({
            $jazz: expect.objectContaining({
              id: bob.$jazz.id,
            }),
          }),
          role: "reader",
        }),
      ]);
    });
  });

  test("should return the members of the parent group", async () => {
    const childGroup = Group.create();
    const parentGroup = Group.create();

    const bob = await createJazzTestAccount({});
    await bob.$jazz.waitForAllCoValuesSync();

    parentGroup.addMember(bob, "writer");
    childGroup.addMember(parentGroup, "reader");

    expect(childGroup.getRoleOf(bob.$jazz.id)).toBe("reader");

    await waitFor(() => {
      expect(childGroup.members).toEqual([
        expect.objectContaining({
          account: expect.objectContaining({
            $jazz: expect.objectContaining({
              id: co.account().getMe().$jazz.id,
            }),
          }),
          role: "admin",
        }),
        expect.objectContaining({
          account: expect.objectContaining({
            $jazz: expect.objectContaining({
              id: bob.$jazz.id,
            }),
          }),
          role: "reader",
        }),
      ]);
    });
  });

  test("should not return everyone", async () => {
    const childGroup = Group.create();

    childGroup.addMember("everyone", "reader");
    expect(childGroup.getRoleOf("everyone")).toBe("reader");

    expect(childGroup.members).toEqual([
      expect.objectContaining({
        account: expect.objectContaining({
          $jazz: expect.objectContaining({
            id: co.account().getMe().$jazz.id,
          }),
        }),
        role: "admin",
      }),
    ]);
  });

  test("should not return revoked members", async () => {
    const childGroup = Group.create();

    const bob = await createJazzTestAccount({});
    await bob.$jazz.waitForAllCoValuesSync();

    childGroup.addMember(bob, "reader");
    await childGroup.removeMember(bob);

    expect(childGroup.getRoleOf(bob.$jazz.id)).toBeUndefined();

    expect(childGroup.members).toEqual([
      expect.objectContaining({
        account: expect.objectContaining({
          $jazz: expect.objectContaining({
            id: co.account().getMe().$jazz.id,
          }),
        }),
        role: "admin",
      }),
    ]);
  });
});

describe("Group.getDirectMembers", () => {
  test("should return only the direct members of the group", async () => {
    const parentGroup = Group.create();
    const childGroup = Group.create();

    const bob = await createJazzTestAccount({});
    await bob.$jazz.waitForAllCoValuesSync();

    // Add bob to parent group
    parentGroup.addMember(bob, "reader");

    // Add parent group to child group
    childGroup.addMember(parentGroup);

    // Child group should inherit bob through parent, but bob is not a direct member
    await waitFor(() => {
      expect(childGroup.members).toEqual([
        expect.objectContaining({
          account: expect.objectContaining({
            $jazz: expect.objectContaining({
              id: co.account().getMe().$jazz.id,
            }),
          }),
        }),
        expect.objectContaining({
          account: expect.objectContaining({
            $jazz: expect.objectContaining({
              id: bob.$jazz.id,
            }),
          }),
        }),
      ]);
    });

    // directMembers should only show the admin, not the inherited bob
    expect(childGroup.getDirectMembers()).toEqual([
      expect.objectContaining({
        account: expect.objectContaining({
          $jazz: expect.objectContaining({
            id: co.account().getMe().$jazz.id,
          }),
        }),
      }),
    ]);

    // Explicitly verify bob is not in directMembers
    expect(childGroup.getDirectMembers()).not.toContainEqual(
      expect.objectContaining({
        account: expect.objectContaining({
          $jazz: expect.objectContaining({
            id: bob.$jazz.id,
          }),
        }),
      }),
    );

    // Parent group's direct members should include both admin and bob
    expect(parentGroup.getDirectMembers()).toEqual([
      expect.objectContaining({
        account: expect.objectContaining({
          $jazz: expect.objectContaining({
            id: co.account().getMe().$jazz.id,
          }),
        }),
      }),
      expect.objectContaining({
        account: expect.objectContaining({
          $jazz: expect.objectContaining({
            id: bob.$jazz.id,
          }),
        }),
      }),
    ]);
  });
});
