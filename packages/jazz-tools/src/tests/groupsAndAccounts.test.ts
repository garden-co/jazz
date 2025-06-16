import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, beforeEach, describe, expect, test } from "vitest";
import { CoMap, Group, z } from "../exports.js";
import { Loaded, Ref, co, zodSchemaToCoSchema } from "../internal.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { setupTwoNodes, waitFor } from "./utils.js";

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

    const CustomAccount = co
      .account({
        profile: CustomProfile,
        root: co.map({}),
      })
      .withMigration((account, creationProps?: { name: string }) => {
        // making sure that the inferred type of account.root & account.profile considers the root/profile not being loaded
        type R = typeof account.root;
        const _r: R = {} as Loaded<typeof CustomAccount.def.shape.root> | null;
        type P = typeof account.profile;
        const _p: P = {} as Loaded<typeof CustomProfile> | null;
        if (creationProps) {
          console.log("In migration!");
          const profileGroup = Group.create({ owner: account });
          profileGroup.addMember("everyone", "reader");
          account.profile = CustomProfile.create(
            { name: creationProps.name, color: "blue" },
            profileGroup,
          );
        }
      });

    const me = await createJazzTestAccount({
      creationProps: { name: "Hermes Puggington" },
      isCurrentActiveAccount: true,
      AccountSchema: zodSchemaToCoSchema(CustomAccount),
    });

    expect(me.profile).toBeDefined();
    expect(me.profile?.name).toBe("Hermes Puggington");
    expect(me.profile?.color).toBe("blue");

    const group = Group.create({ owner: me });
    group.addMember("everyone", "reader");

    expect(group.members).toMatchObject([{ id: me.id, role: "admin" }]);

    const meAsMember = group.members.find((member) => member.id === me.id);
    assert(meAsMember?.account);
    expect((meAsMember?.account).profile?.name).toBe("Hermes Puggington");
  });

  test("Should throw when creating a profile with an account as owner", async () => {
    const CustomAccount = co
      .account()
      .withMigration(
        (
          account: Loaded<typeof CustomAccount>,
          creationProps?: { name: string },
        ) => {
          if (creationProps) {
            account.profile = co.profile().create(
              { name: creationProps.name },
              // @ts-expect-error - only groups can own profiles, but we want to also perform a runtime check
              account,
            );
          }
        },
      );

    await expect(() =>
      CustomAccount.create({
        creationProps: { name: "Hermes Puggington" },
        crypto: Crypto,
      }),
    ).rejects.toThrowError("Profile must be owned by a Group");
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

    const mapAsReader = await TestMap.load(mapInChild.id, { loadAs: reader });
    expect(mapAsReader?.title).toBe("In Child");

    await parentGroup.removeMember(reader);

    mapInChild.title = "In Child (updated)";

    await waitFor(async () => {
      const mapAsReaderAfterUpdate = await TestMap.load(mapInChild.id, {
        loadAs: reader,
      });
      expect(mapAsReaderAfterUpdate).toBe(null);
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

    const mapAsReader = await TestMap.load(mapInGrandChild.id, {
      loadAs: reader,
    });
    expect(mapAsReader?.title).toBe("In Grand Child");

    await grandParentGroup.removeMember(reader);

    await grandParentGroup.waitForSync();

    mapInGrandChild.title = "In Grand Child (updated)";

    const mapAsReaderAfterUpdate = await TestMap.load(mapInGrandChild.id, {
      loadAs: reader,
    });
    expect(mapAsReaderAfterUpdate).toBe(null);
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
      expect.objectContaining({ id: parentGroup.id }),
    );

    expect(parentGroups[0]?.getParentGroups()).toContainEqual(
      expect.objectContaining({ id: grandParentGroup.id }),
    );
  });

  test("Account.getParentGroups should return an empty array", async () => {
    const account = await co.account().create({
      creationProps: { name: "Test Account" },
      crypto: Crypto,
    });

    const parentGroups = account.getParentGroups();

    expect(parentGroups).toEqual([]);
  });

  test("waitForSync should resolve when the value is uploaded", async () => {
    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const group = Group.create({ owner: clientAccount });

    await group.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the map from it
    clientNode.gracefulShutdown();

    const loadedGroup = await serverNode.load(group._raw.id);

    expect(loadedGroup).not.toBe("unavailable");
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
    await account.waitForAllCoValuesSync();

    const group = Group.create();

    // @ts-expect-error - Even though readerInvite is a valid role for an account, we don't allow it to not create confusion when using the intellisense
    group.addMember(account, "readerInvite");
    // @ts-expect-error - Only groups can have an `inherit` role, not accounts
    group.addMember(account, "inherit");
    // @ts-expect-error - Only groups can be added without a role, not accounts
    group.addMember(account, undefined);

    expect(group.members).not.toContainEqual(
      expect.objectContaining({
        id: account.id,
        role: "readerInvite",
      }),
    );

    expect(group.getRoleOf(account.id)).toBe("readerInvite");
  });

  test("adding a group member as writeOnly should fail", async () => {
    const account = await createJazzTestAccount({});
    await account.waitForAllCoValuesSync();

    const parentGroup = Group.create();
    const group = Group.create();
    expect(() => {
      // @ts-expect-error
      group.addMember(parentGroup, "writeOnly");
    }).toThrow();
  });

  test("Removing member group", async () => {
    const alice = await createJazzTestAccount({});
    await alice.waitForAllCoValuesSync();
    const bob = await createJazzTestAccount({});
    await bob.waitForAllCoValuesSync();

    const parentGroup = Group.create();
    // `parentGroup` has `alice` as a writer
    parentGroup.addMember(alice, "writer");
    expect(parentGroup.getRoleOf(alice.id)).toBe("writer");

    const group = Group.create();
    // `group` has `bob` as a reader
    group.addMember(bob, "reader");
    expect(group.getRoleOf(bob.id)).toBe("reader");

    group.addMember(parentGroup);
    // `group` has `parentGroup`'s members (in this case, `alice` as a writer)
    expect(group.getRoleOf(bob.id)).toBe("reader");
    expect(group.getRoleOf(alice.id)).toBe("writer");

    // `group` no longer has `parentGroup`'s members
    await group.removeMember(parentGroup);
    expect(group.getRoleOf(bob.id)).toBe("reader");
    expect(group.getRoleOf(alice.id)).toBe(undefined);
  });
});

describe("Group.getRoleOf", () => {
  beforeEach(async () => {
    await createJazzTestAccount({ isCurrentActiveAccount: true });
  });

  test("returns correct role for admin", async () => {
    const group = Group.create();
    const admin = await createJazzTestAccount({});
    await admin.waitForAllCoValuesSync();
    group.addMember(admin, "admin");
    expect(group.getRoleOf(admin.id)).toBe("admin");
    expect(group.getRoleOf("me")).toBe("admin");
  });

  test("returns correct role for writer", async () => {
    const group = Group.create();
    const writer = await createJazzTestAccount({});
    await writer.waitForAllCoValuesSync();
    group.addMember(writer, "writer");
    expect(group.getRoleOf(writer.id)).toBe("writer");
  });

  test("returns correct role for reader", async () => {
    const group = Group.create();
    const reader = await createJazzTestAccount({});
    await reader.waitForAllCoValuesSync();
    group.addMember(reader, "reader");
    expect(group.getRoleOf(reader.id)).toBe("reader");
  });

  test("returns correct role for writeOnly", async () => {
    const group = Group.create();
    const writeOnly = await createJazzTestAccount({});
    await writeOnly.waitForAllCoValuesSync();
    group.addMember(writeOnly, "writeOnly");
    expect(group.getRoleOf(writeOnly.id)).toBe("writeOnly");
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
    await account.waitForAllCoValuesSync();
    const group = Group.create({ owner: account });

    group.addMember(co.account().getMe(), "writer");

    expect(group.getRoleOf("me")).toBe("writer");
  });

  test("returns correct role for 'me' when current account is reader", async () => {
    const account = await createJazzTestAccount();
    await account.waitForAllCoValuesSync();
    const group = Group.create({ owner: account });

    group.addMember(co.account().getMe(), "reader");

    expect(group.getRoleOf("me")).toBe("reader");
  });

  test("returns undefined for 'me' when current account has no role", async () => {
    const account = await createJazzTestAccount();
    await account.waitForAllCoValuesSync();
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
    expect(account.getRoleOf(account.id)).toBe("admin");

    // The GlobalMe is not this account
    expect(account.getRoleOf("me")).toBe(undefined);
    expect(co.account().getMe().getRoleOf("me")).toBe("admin");

    // Other accounts should have no role
    const otherAccount = await co.account().create({
      creationProps: { name: "Other Account" },
      crypto: Crypto,
    });
    expect(account.getRoleOf(otherAccount.id)).toBeUndefined();

    // Everyone should have no role
    expect(account.getRoleOf("everyone")).toBeUndefined();
  });

  test("members array only contains self as admin", async () => {
    const account = await co.account().create({
      creationProps: { name: "Test Account" },
      crypto: Crypto,
    });

    expect(account.members).toEqual([
      { id: account.id, role: "admin", account: account, ref: expect.any(Ref) },
    ]);
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
});

describe("Group.members", () => {
  test("should return the members of the group", async () => {
    const childGroup = Group.create();

    const bob = await createJazzTestAccount({});
    await bob.waitForAllCoValuesSync();

    childGroup.addMember(bob, "reader");
    expect(childGroup.getRoleOf(bob.id)).toBe("reader");

    expect(childGroup.members).toEqual([
      expect.objectContaining({
        account: expect.objectContaining({
          id: co.account().getMe().id,
        }),
        role: "admin",
      }),
      expect.objectContaining({
        account: expect.objectContaining({
          id: bob.id,
        }),
        role: "reader",
      }),
    ]);
  });

  test("should return the members of the parent group", async () => {
    const childGroup = Group.create();
    const parentGroup = Group.create();

    const bob = await createJazzTestAccount({});
    await bob.waitForAllCoValuesSync();

    parentGroup.addMember(bob, "writer");
    childGroup.addMember(parentGroup, "reader");

    expect(childGroup.getRoleOf(bob.id)).toBe("reader");

    expect(childGroup.members).toEqual([
      expect.objectContaining({
        account: expect.objectContaining({
          id: co.account().getMe().id,
        }),
        role: "admin",
      }),
      expect.objectContaining({
        account: expect.objectContaining({
          id: bob.id,
        }),
        role: "reader",
      }),
    ]);
  });

  test("should not return everyone", async () => {
    const childGroup = Group.create();

    childGroup.addMember("everyone", "reader");
    expect(childGroup.getRoleOf("everyone")).toBe("reader");

    expect(childGroup.members).toEqual([
      expect.objectContaining({
        account: expect.objectContaining({
          id: co.account().getMe().id,
        }),
        role: "admin",
      }),
    ]);
  });

  test("should not return revoked members", async () => {
    const childGroup = Group.create();

    const bob = await createJazzTestAccount({});
    await bob.waitForAllCoValuesSync();

    childGroup.addMember(bob, "reader");
    await childGroup.removeMember(bob);

    expect(childGroup.getRoleOf(bob.id)).toBeUndefined();

    expect(childGroup.members).toEqual([
      expect.objectContaining({
        account: expect.objectContaining({
          id: co.account().getMe().id,
        }),
        role: "admin",
      }),
    ]);
  });
});
