import { describe, expect, test } from "vitest";
import type { CoValueCore } from "../exports";
import { setupTestNode } from "./testUtils";

function checkDependencyLoops(coValue: CoValueCore, depth = 0) {
  if (depth > 1000) {
    throw new Error("Circular dependency detected");
  }

  for (const dependency of coValue.dependencies) {
    checkDependencyLoops(coValue.node.getCoValue(dependency), depth + 1);
  }
}

function checkDependantLoops(coValue: CoValueCore, depth = 0) {
  if (depth > 1000) {
    throw new Error("Circular dependency detected");
  }

  for (const dependant of coValue.dependant) {
    checkDependantLoops(coValue.node.getCoValue(dependant), depth + 1);
  }
}

function checkForCircularDependency(coValue: CoValueCore) {
  checkDependencyLoops(coValue);
  checkDependantLoops(coValue);
}

describe("isCircularDependency", () => {
  test("should detect self-dependency (A -> A)", async () => {
    const node = setupTestNode().node;

    const group = node.createGroup();

    // Create a self-extension by bypassing the extend method's circular dependency prevention
    group.set(`parent_${group.id}`, "extend", "trusting");

    // The check should have prevented the circular dependency
    checkForCircularDependency(group.core);
  });

  test("should detect simple cycle (A -> B -> A)", async () => {
    const node = setupTestNode().node;

    const groupA = node.createGroup();
    const groupB = node.createGroup();

    // Create A -> B -> A cycle
    groupA.set(`parent_${groupB.id}`, "extend", "trusting");
    groupB.set(`parent_${groupA.id}`, "extend", "trusting");

    // The check should have prevented the circular dependency
    checkForCircularDependency(groupA.core);
  });

  test("should detect longer cycle (A -> B -> C -> A)", async () => {
    const node = setupTestNode().node;

    const groupA = node.createGroup();
    const groupB = node.createGroup();
    const groupC = node.createGroup();

    // Create A -> B -> C -> A cycle
    groupA.set(`parent_${groupB.id}`, "extend", "trusting");
    groupB.set(`parent_${groupC.id}`, "extend", "trusting");
    groupC.set(`parent_${groupA.id}`, "extend", "trusting");

    // The check should have prevented the circular dependency
    checkForCircularDependency(groupA.core);
  });

  test("should not detect circular dependency in diamond pattern (A -> B, A -> C, B -> D, C -> D)", async () => {
    const node = setupTestNode().node;

    const groupA = node.createGroup();
    const groupB = node.createGroup();
    const groupC = node.createGroup();
    const groupD = node.createGroup();

    // Create diamond: A -> B, A -> C, B -> D, C -> D
    groupA.set(`parent_${groupB.id}`, "extend", "trusting");
    groupA.set(`parent_${groupC.id}`, "extend", "trusting");
    groupB.set(`parent_${groupD.id}`, "extend", "trusting");
    groupC.set(`parent_${groupD.id}`, "extend", "trusting");

    // No circular dependencies should be detected
    expect(groupA.core.getDependedOnCoValues()).toEqual(
      new Set([groupB.core.id, groupC.core.id]),
    );
    expect(groupB.core.getDependedOnCoValues()).toEqual(
      new Set([groupD.core.id]),
    );
    expect(groupC.core.getDependedOnCoValues()).toEqual(
      new Set([groupD.core.id]),
    );
  });

  test("should detect cycle with multiple paths (A -> B -> D, A -> C -> D, D -> A)", async () => {
    const node = setupTestNode().node;

    const groupA = node.createGroup();
    const groupB = node.createGroup();
    const groupC = node.createGroup();
    const groupD = node.createGroup();

    // Create: A -> B -> D, A -> C -> D, D -> A (cycle)
    groupA.set(`parent_${groupB.id}`, "extend", "trusting");
    groupA.set(`parent_${groupC.id}`, "extend", "trusting");
    groupB.set(`parent_${groupD.id}`, "extend", "trusting");
    groupC.set(`parent_${groupD.id}`, "extend", "trusting");
    groupD.set(`parent_${groupA.id}`, "extend", "trusting");

    // Should detect circular dependency
    checkForCircularDependency(groupA.core);
    checkForCircularDependency(groupD.core);
  });
});
