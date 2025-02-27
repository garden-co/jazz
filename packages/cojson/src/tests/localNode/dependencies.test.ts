import { describe, expect, test } from "vitest";
import { SessionID } from "../../exports.js";
import { subscribe } from "../../localNode/actions/subscribing.js";
import { stageLoadDeps } from "../../localNode/stages/1_loadDeps.js";
import { emptyNode } from "../../localNode/structure.js";
import {
  addMemberTestTransaction,
  addParentGroupTestTransaction,
  createTestCoMap,
  createTestGroup,
} from "./testUtils.js";

describe("Loading dependencies", () => {
  test("stageLoadDeps does nothing for CoValues without listeners or dependents", () => {
    const node = emptyNode();

    const group = createTestGroup("sealer_z1/signer_z1_session1", "group1");
    const coValue = createTestCoMap(group.id, "coMap1");

    node.coValues = {
      [coValue.id]: coValue,
      [group.id]: group,
    };
    node.peers = [];

    const coValuesBefore = structuredClone(node.coValues);

    stageLoadDeps(node);
    expect(node.coValues).toEqual(coValuesBefore);
  });

  test("stageLoadDeps adds dependent covalues to an existing coValue's dependencies if the dependent has listeners (ownedByGroup)", () => {
    const node = emptyNode();

    const group = createTestGroup("sealer_z1/signer_z1_session1", "group1");
    const coValue = createTestCoMap(group.id, "coMap1");

    node.coValues = {
      [coValue.id]: coValue,
      [group.id]: group,
    };

    node.peers = [];

    const _ = subscribe(node, coValue.id);

    stageLoadDeps(node);

    expect(node.coValues[group.id].dependents).toEqual([coValue.id]);

    stageLoadDeps(node);

    // idempotency
    expect(node.coValues[group.id].dependents).toEqual([coValue.id]);
  });

  test("stageLoadDeps adds dependents and adds a new entry on missing dependency if the dependent has listeners (ownedByGroup)", () => {
    const node = emptyNode();

    const group = createTestGroup("sealer_z1/signer_z1_session1", "group1");
    const coValue = createTestCoMap(group.id, "coMap1");

    node.coValues = {
      [coValue.id]: coValue,
    };
    node.peers = [];

    const _ = subscribe(node, coValue.id);

    stageLoadDeps(node);

    expect(node.coValues[group.id].dependents).toEqual([coValue.id]);

    stageLoadDeps(node);

    // idempotency
    expect(node.coValues[group.id].dependents).toEqual([coValue.id]);
  });

  test("stageLoadDeps adds dependent covalues to an existing coValue's dependencies if the dependent has listeners (group member)", () => {
    const node = emptyNode();

    const group = createTestGroup("sealer_z1/signer_z1_session1", "group1");
    const member = createTestCoMap(null, "member");

    addMemberTestTransaction(group, member.id, "session1" as SessionID);

    node.coValues = {
      [group.id]: group,
      [member.id]: member,
    };
    node.peers = [];

    const _ = subscribe(node, group.id);

    stageLoadDeps(node);

    expect(node.coValues[member.id].dependents).toEqual([group.id]);

    stageLoadDeps(node);

    // idempotency
    expect(node.coValues[member.id].dependents).toEqual([group.id]);
  });

  test("stageLoadDeps adds dependents and adds a new entry on missing dependency if the dependent has listeners (group member)", () => {
    const node = emptyNode();

    const group = createTestGroup("sealer_z1/signer_z1_session1", "group1");
    const member = createTestCoMap(null, "member");

    addMemberTestTransaction(group, member.id, "session1" as SessionID);

    node.coValues = {
      [group.id]: group,
    };
    node.peers = [];

    const _ = subscribe(node, group.id);

    stageLoadDeps(node);

    expect(node.coValues[member.id].dependents).toEqual([group.id]);

    stageLoadDeps(node);

    // idempotency
    expect(node.coValues[member.id].dependents).toEqual([group.id]);
  });

  test("stageLoadDeps adds dependent covalues to an existing coValue's dependencies if the dependent has listeners (extended group)", () => {
    const node = emptyNode();

    const group = createTestGroup("sealer_z1/signer_z1_session1", "group1");
    const parent = createTestGroup("sealer_z1/signer_z1_session1", "parent");

    addParentGroupTestTransaction(group, parent.id, "session1" as SessionID);

    node.coValues = {
      [group.id]: group,
      [parent.id]: parent,
    };
    node.peers = [];

    const _ = subscribe(node, group.id);

    stageLoadDeps(node);

    expect(node.coValues[parent.id].dependents).toEqual([group.id]);

    stageLoadDeps(node);

    // idempotency
    expect(node.coValues[parent.id].dependents).toEqual([group.id]);
  });

  test("stageLoadDeps adds dependents and adds a new entry on missing dependency if the dependent has listeners (extended group)", () => {
    const node = emptyNode();

    const group = createTestGroup("sealer_z1/signer_z1_session1", "group1");
    const parent = createTestGroup("sealer_z1/signer_z1_session1", "parent");

    addParentGroupTestTransaction(group, parent.id, "session1" as SessionID);

    node.coValues = {
      [group.id]: group,
    };
    node.peers = [];

    const _ = subscribe(node, group.id);

    stageLoadDeps(node);

    expect(node.coValues[parent.id].dependents).toEqual([group.id]);
    stageLoadDeps(node);

    // idempotency
    expect(node.coValues[parent.id].dependents).toEqual([group.id]);
  });
});
