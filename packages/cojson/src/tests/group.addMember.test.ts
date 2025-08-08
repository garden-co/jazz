import { beforeEach, describe, expect, test } from "vitest";
import { expectMap } from "../coValue.js";
import {
  loadCoValueOrFail,
  SyncMessagesLog,
  setupTestAccount,
  setupTestNode,
  TEST_NODE_CONFIG,
  waitFor,
} from "./testUtils.js";

beforeEach(async () => {
  SyncMessagesLog.clear();
  setupTestNode({ isSyncServer: true });
});

describe("Group.addMember", () => {
  test("an admin should be able to grant read access", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const reader = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const personOnReaderNode = await loadCoValueOrFail(reader.node, person.id);

    expect(personOnReaderNode.get("name")).toEqual(undefined);

    const readerOnAdminNode = await loadCoValueOrFail(
      admin.node,
      reader.accountID,
    );
    group.addMember(readerOnAdminNode, "reader");

    expect(group.roleOf(reader.accountID)).toEqual("reader");

    await waitFor(() => {
      expect(
        expectMap(personOnReaderNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });
  });

  test("an admin should be able to grant write access", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const writer = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const personOnWriterNode = await loadCoValueOrFail(writer.node, person.id);

    expect(personOnWriterNode.get("name")).toEqual(undefined);

    const writerOnAdminNode = await loadCoValueOrFail(
      admin.node,
      writer.accountID,
    );
    group.addMember(writerOnAdminNode, "writer");
    expect(group.roleOf(writer.accountID)).toEqual("writer");

    await waitFor(() => {
      expect(
        expectMap(personOnWriterNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });

    personOnWriterNode.set("name", "Jane Doe");
    expect(personOnWriterNode.get("name")).toEqual("Jane Doe");
  });

  test("an admin should be able to grant writeOnly access", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const writeOnlyUser = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const writeOnlyUserOnAdminNode = await loadCoValueOrFail(
      admin.node,
      writeOnlyUser.accountID,
    );
    group.addMember(writeOnlyUserOnAdminNode, "writeOnly");
    expect(group.roleOf(writeOnlyUser.accountID)).toEqual("writeOnly");
    const personOnWriteOnlyNode = await loadCoValueOrFail(
      writeOnlyUser.node,
      person.id,
    );

    // Should not be able to read
    expect(personOnWriteOnlyNode.get("name")).toEqual(undefined);

    // Should be able to write
    personOnWriteOnlyNode.set("name", "Jane Doe");

    expect(personOnWriteOnlyNode.get("name")).toEqual("Jane Doe");
  });

  test("an admin should be able to grant admin access", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const otherAdmin = await setupTestAccount({
      connected: true,
    });

    const reader = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const otherAdminOnAdminNode = await loadCoValueOrFail(
      admin.node,
      otherAdmin.accountID,
    );
    group.addMember(otherAdminOnAdminNode, "admin");

    const personOnReaderNode = await loadCoValueOrFail(reader.node, person.id);

    expect(personOnReaderNode.get("name")).toEqual(undefined);

    const readerOnOtherAdminNode = await loadCoValueOrFail(
      otherAdmin.node,
      reader.accountID,
    );
    group.addMember(readerOnOtherAdminNode, "reader");

    await waitFor(() => {
      expect(
        expectMap(personOnReaderNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });
  });

  test("an admin should be able downgrade a writer to reader", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const writer = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const writerOnAdminNode = await loadCoValueOrFail(
      admin.node,
      writer.accountID,
    );
    group.addMember(writerOnAdminNode, "writer");
    group.addMember(writerOnAdminNode, "reader");

    expect(group.roleOf(writer.accountID)).toEqual("reader");

    // Verify writer can read and write
    const personOnWriterNode = await loadCoValueOrFail(writer.node, person.id);

    // Should not be able to write
    personOnWriterNode.set("name", "Jane Doe");

    expect(personOnWriterNode.get("name")).toEqual("John Doe");
  });

  test("an admin should be able downgrade a reader to writeOnly", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const reader = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();

    const readerOnAdminNode = await loadCoValueOrFail(
      admin.node,
      reader.accountID,
    );
    group.addMember(readerOnAdminNode, "reader");
    group.addMember(readerOnAdminNode, "writeOnly");

    expect(group.roleOf(reader.accountID)).toEqual("writeOnly");

    const person = group.createMap({
      name: "John Doe",
    });

    // Verify reader can read
    const personOnReaderNode = await loadCoValueOrFail(reader.node, person.id);

    expect(personOnReaderNode.get("name")).toEqual(undefined);
  });

  test("an admin should not be able downgrade an admin", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const otherAdmin = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const otherAdminOnAdminNode = await loadCoValueOrFail(
      admin.node,
      otherAdmin.accountID,
    );
    group.addMember(otherAdminOnAdminNode, "admin");

    // Try to downgrade other admin
    try {
      group.addMember(otherAdminOnAdminNode, "writer");
    } catch (e) {
      expect(e).toBeDefined();
    }

    expect(group.roleOf(otherAdmin.accountID)).toEqual("admin");

    // Verify other admin still has admin access by adding a new member
    const reader = await setupTestAccount({
      connected: true,
    });

    const readerOnOtherAdminNode = await loadCoValueOrFail(
      otherAdmin.node,
      reader.accountID,
    );
    group.addMember(readerOnOtherAdminNode, "reader");

    const personOnReaderNode = await loadCoValueOrFail(reader.node, person.id);

    await waitFor(() => {
      expect(
        expectMap(personOnReaderNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });
  });

  test("an admin should be able downgrade themselves", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();

    const account = await loadCoValueOrFail(admin.node, admin.accountID);

    // Downgrade self to writer
    group.addMember(account, "writer");
    expect(group.roleOf(admin.accountID)).toEqual("writer");
  });

  test("an admin should be able downgrade a writeOnly to reader", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const writeOnlyUser = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const writeOnlyUserOnAdminNode = await loadCoValueOrFail(
      admin.node,
      writeOnlyUser.accountID,
    );
    group.addMember(writeOnlyUserOnAdminNode, "writeOnly");
    group.addMember(writeOnlyUserOnAdminNode, "reader");

    expect(group.roleOf(writeOnlyUser.accountID)).toEqual("reader");

    const personOnWriteOnlyNode = await loadCoValueOrFail(
      writeOnlyUser.node,
      person.id,
    );

    expect(personOnWriteOnlyNode.get("name")).toEqual("John Doe");
  });

  test("a reader should not be able to add a member", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const reader = await setupTestAccount({
      connected: true,
    });

    const newUser = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const readerOnAdminNode = await loadCoValueOrFail(
      admin.node,
      reader.accountID,
    );
    group.addMember(readerOnAdminNode, "reader");

    const newUserOnReaderNode = await loadCoValueOrFail(
      reader.node,
      newUser.accountID,
    );

    const groupOnReaderNode = await loadCoValueOrFail(reader.node, group.id);

    // Try to add member as reader
    try {
      groupOnReaderNode.addMember(newUserOnReaderNode, "reader");
      throw new Error("Should not be able to add member as reader");
    } catch (e) {
      expect(e).toBeDefined();
    }

    expect(groupOnReaderNode.roleOf(newUser.accountID)).toBeUndefined();
  });

  test("a writer should not be able to add a member", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const writer = await setupTestAccount({
      connected: true,
    });

    const newUser = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const writerOnAdminNode = await loadCoValueOrFail(
      admin.node,
      writer.accountID,
    );
    group.addMember(writerOnAdminNode, "writer");

    const newUserOnWriterNode = await loadCoValueOrFail(
      writer.node,
      newUser.accountID,
    );

    const groupOnWriterNode = await loadCoValueOrFail(writer.node, group.id);

    // Try to add member as writer
    try {
      groupOnWriterNode.addMember(newUserOnWriterNode, "reader");
      throw new Error("Should not be able to add member as writer");
    } catch (e) {
      expect(e).toBeDefined();
    }

    expect(groupOnWriterNode.roleOf(newUser.accountID)).toBeUndefined();
  });

  test("a writeOnly should not be able to add a member", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const writeOnlyUser = await setupTestAccount({
      connected: true,
    });

    const newUser = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const writeOnlyUserOnAdminNode = await loadCoValueOrFail(
      admin.node,
      writeOnlyUser.accountID,
    );
    group.addMember(writeOnlyUserOnAdminNode, "writeOnly");

    const newUserOnWriteOnlyNode = await loadCoValueOrFail(
      writeOnlyUser.node,
      newUser.accountID,
    );

    const groupOnWriteOnlyNode = await loadCoValueOrFail(
      writeOnlyUser.node,
      group.id,
    );

    // Try to add member as writeOnly user
    try {
      groupOnWriteOnlyNode.addMember(newUserOnWriteOnlyNode, "reader");
      throw new Error("Should not be able to add member as writeOnly user");
    } catch (e) {
      expect(e).toBeDefined();
    }

    expect(groupOnWriteOnlyNode.roleOf(newUser.accountID)).toBeUndefined();
  });
});
