/**
 * Tests for subscribeAll with includes
 */

import { describe, it, expect, beforeAll } from "vitest";
import { createDatabase, type Database } from "../src/generated/client";

// We need to load the WASM module
let db: Database;

beforeAll(async () => {
  // Dynamic import of the WASM module
  const wasm = await import("groove-wasm");
  await wasm.default();

  const wasmDb = new wasm.WasmDatabase();

  // Initialize schema
  wasmDb.init_schema(`
    CREATE TABLE Users (
      name STRING NOT NULL,
      email STRING NOT NULL,
      avatarColor STRING NOT NULL
    );

    CREATE TABLE Projects (
      name STRING NOT NULL,
      color STRING NOT NULL,
      description STRING
    );

    CREATE TABLE Issues (
      title STRING NOT NULL,
      description STRING,
      status STRING NOT NULL,
      priority STRING NOT NULL,
      project REFERENCES Projects NOT NULL,
      createdAt I64 NOT NULL,
      updatedAt I64 NOT NULL
    );

    CREATE TABLE Labels (
      name STRING NOT NULL,
      color STRING NOT NULL
    );

    CREATE TABLE IssueLabels (
      issue REFERENCES Issues NOT NULL,
      label REFERENCES Labels NOT NULL
    );

    CREATE TABLE IssueAssignees (
      issue REFERENCES Issues NOT NULL,
      user REFERENCES Users NOT NULL
    );
  `);

  db = createDatabase(wasmDb);

  // Insert test data
  const userId = db.users.create({
    name: "Alice",
    email: "alice@test.com",
    avatarColor: "#ff0000"
  });

  const projectId = db.projects.create({
    name: "Test Project",
    color: "#00ff00",
    description: "A test project"
  });

  const labelId = db.labels.create({
    name: "Bug",
    color: "#ff0000"
  });

  const issueId = db.issues.create({
    title: "Test Issue",
    description: "Test description",
    status: "open",
    priority: "high",
    project: projectId,
    createdAt: BigInt(Date.now()),
    updatedAt: BigInt(Date.now())
  });

  // Create junction table entries
  db.issuelabels.create({
    issue: issueId,
    label: labelId
  });

  db.issueassignees.create({
    issue: issueId,
    user: userId
  });
});

describe("subscribeAll without includes", () => {
  it("should return plain issues", async () => {
    const issues = await new Promise<any[]>((resolve) => {
      // Use closure variable to avoid timing issue where callback is called
      // synchronously before unsub is assigned
      let unsubscribe: (() => void) | undefined;
      unsubscribe = db.issues.subscribeAll((rows) => {
        // Use setTimeout to defer unsubscribe to next tick
        setTimeout(() => unsubscribe?.(), 0);
        resolve(rows);
      });
    });

    expect(issues.length).toBe(1);
    expect(issues[0].title).toBe("Test Issue");
    expect(typeof issues[0].project).toBe("string"); // FK, not resolved
  });

  it("should return plain users", async () => {
    const users = await new Promise<any[]>((resolve) => {
      let unsubscribe: (() => void) | undefined;
      unsubscribe = db.users.subscribeAll((rows) => {
        setTimeout(() => unsubscribe?.(), 0);
        resolve(rows);
      });
    });

    expect(users.length).toBe(1);
    expect(users[0].name).toBe("Alice");
  });
});

describe("subscribeAll with forward ref include", () => {
  it("should resolve project ref", async () => {
    const issues = await new Promise<any[]>((resolve) => {
      let unsubscribe: (() => void) | undefined;
      unsubscribe = db.issues.with({ project: true }).subscribeAll((rows) => {
        setTimeout(() => unsubscribe?.(), 0);
        resolve(rows);
      });
    });

    console.log("Issues with project:", JSON.stringify(issues, (_, v) => typeof v === 'bigint' ? v.toString() : v, 2));

    expect(issues.length).toBe(1);
    expect(issues[0].title).toBe("Test Issue");
    expect(typeof issues[0].project).toBe("object"); // Should be resolved
    expect(issues[0].project.name).toBe("Test Project");
  });
});

describe("subscribeAll with reverse ref include", () => {
  it("should resolve IssueLabels reverse ref", async () => {
    const issues = await new Promise<any[]>((resolve) => {
      let unsubscribe: (() => void) | undefined;
      unsubscribe = db.issues.with({ IssueLabels: true }).subscribeAll((rows) => {
        setTimeout(() => unsubscribe?.(), 0);
        resolve(rows);
      });
    });

    console.log("Issues with IssueLabels:", JSON.stringify(issues, (_, v) => typeof v === 'bigint' ? v.toString() : v, 2));

    expect(issues.length).toBe(1);
    expect(Array.isArray(issues[0].IssueLabels)).toBe(true);
    expect(issues[0].IssueLabels.length).toBe(1);
  });
});

describe("subscribeAll with mixed includes", () => {
  it("should resolve forward ref and reverse ref together", async () => {
    const issues = await new Promise<any[]>((resolve) => {
      let unsubscribe: (() => void) | undefined;
      // For now, only test forward ref + reverse ref without nested includes
      // Nested includes within reverse refs need additional work
      unsubscribe = db.issues.with({
        project: true,
        IssueLabels: true,
        IssueAssignees: true
      }).subscribeAll((rows) => {
        setTimeout(() => unsubscribe?.(), 0);
        resolve(rows);
      });
    });

    console.log("Issues with mixed includes:", JSON.stringify(issues, (_, v) => typeof v === 'bigint' ? v.toString() : v, 2));

    expect(issues.length).toBe(1);
    // Forward ref should be resolved
    expect(issues[0].project.name).toBe("Test Project");
    // Reverse refs should be arrays with junction table rows
    expect(Array.isArray(issues[0].IssueLabels)).toBe(true);
    expect(issues[0].IssueLabels.length).toBe(1);
    expect(Array.isArray(issues[0].IssueAssignees)).toBe(true);
    expect(issues[0].IssueAssignees.length).toBe(1);
  });

  it("should resolve nested includes within reverse refs", async () => {
    const issues = await new Promise<any[]>((resolve) => {
      let unsubscribe: (() => void) | undefined;
      // Test nested includes: resolve label FK within IssueLabels, user FK within IssueAssignees
      unsubscribe = db.issues.with({
        project: true,
        IssueLabels: { label: true },
        IssueAssignees: { user: true }
      }).subscribeAll((rows) => {
        setTimeout(() => unsubscribe?.(), 0);
        resolve(rows);
      });
    });

    console.log("Issues with nested includes:", JSON.stringify(issues, (_, v) => typeof v === 'bigint' ? v.toString() : v, 2));

    expect(issues.length).toBe(1);
    // Forward ref should be resolved
    expect(issues[0].project.name).toBe("Test Project");
    // Reverse refs should be arrays with resolved nested refs
    expect(Array.isArray(issues[0].IssueLabels)).toBe(true);
    expect(issues[0].IssueLabels.length).toBe(1);
    // The label FK should be resolved to the full Labels row
    expect(typeof issues[0].IssueLabels[0].label).toBe("object");
    expect(issues[0].IssueLabels[0].label.name).toBe("Bug");

    expect(Array.isArray(issues[0].IssueAssignees)).toBe(true);
    expect(issues[0].IssueAssignees.length).toBe(1);
    // The user FK should be resolved to the full Users row
    expect(typeof issues[0].IssueAssignees[0].user).toBe("object");
    expect(issues[0].IssueAssignees[0].user.name).toBe("Alice");
  });
});

describe("subscribeAll with filter and includes", () => {
  it("should filter by junction table while including nested refs", async () => {
    // First get the label ID
    const labels = await new Promise<any[]>((resolve) => {
      let unsubscribe: (() => void) | undefined;
      unsubscribe = db.labels.subscribeAll((rows) => {
        setTimeout(() => unsubscribe?.(), 0);
        resolve(rows);
      });
    });
    const bugLabel = labels.find(l => l.name === "Bug");
    expect(bugLabel).toBeDefined();

    // Now filter issues by that label
    const issues = await new Promise<any[]>((resolve) => {
      let unsubscribe: (() => void) | undefined;
      unsubscribe = db.issues.with({
        project: true,
        IssueLabels: { label: true },
        IssueAssignees: { user: true }
      }).where({
        IssueLabels: { some: { label: bugLabel.id } }
      }).subscribeAll((rows) => {
        setTimeout(() => unsubscribe?.(), 0);
        resolve(rows);
      });
    });

    console.log("Issues filtered by label:", JSON.stringify(issues, (_, v) => typeof v === 'bigint' ? v.toString() : v, 2));

    expect(issues.length).toBe(1);
    expect(issues[0].title).toBe("Test Issue");
    // All includes should still work
    expect(issues[0].project.name).toBe("Test Project");
    expect(issues[0].IssueLabels[0].label.name).toBe("Bug");
    expect(issues[0].IssueAssignees[0].user.name).toBe("Alice");
  });
});
