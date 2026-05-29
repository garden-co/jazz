import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import {
  LEADER_PROTOCOL_VERSION,
  buildLeaderScope,
  buildLeaderWorkerName,
  buildLockName,
  isLeaderToTab,
  isTabToLeader,
} from "./protocol.js";
import { JAZZ_PACKAGE_VERSION } from "./package-version.js";

describe("shared-worker-leader protocol", () => {
  it("LEADER_PROTOCOL_VERSION is 1 in the initial release", () => {
    expect(LEADER_PROTOCOL_VERSION).toBe(1);
  });

  it("isTabToLeader accepts CHECK_CAPABILITY (no body)", () => {
    expect(isTabToLeader({ t: "CHECK_CAPABILITY" })).toBe(true);
  });

  it("isLeaderToTab accepts CAPABILITY_RESULT with a boolean", () => {
    expect(isLeaderToTab({ t: "CAPABILITY_RESULT", supported: true })).toBe(true);
    expect(isLeaderToTab({ t: "CAPABILITY_RESULT", supported: false })).toBe(true);
    expect(isLeaderToTab({ t: "CAPABILITY_RESULT" })).toBe(false);
  });

  it("buildLeaderScope is a deterministic function of (appId, dbName)", () => {
    expect(buildLeaderScope("app", "db")).toEqual(buildLeaderScope("app", "db"));
    expect(buildLeaderScope("app", "db1")).not.toEqual(buildLeaderScope("app", "db2"));
  });

  it("buildLeaderWorkerName has the documented prefix", () => {
    expect(buildLeaderWorkerName(buildLeaderScope("app", "db"))).toMatch(
      /^jazz-shared-worker-leader:/,
    );
  });

  it("buildLockName is stable, version-independent", () => {
    expect(buildLockName("app", "db")).toBe("jazz-worker:app:db");
    expect(buildLockName("app", "db")).not.toMatch(/\d+\.\d+/);
  });

  it("isTabToLeader accepts a well-formed CONNECT", () => {
    expect(
      isTabToLeader({
        t: "CONNECT",
        tabId: "tab-1",
        bornAt: 12345,
        scope: "scope-x",
        protocolVersion: 1,
        jazzPackageVersion: "0.0.0",
        appId: "app",
        dbName: "db",
        schemaJson: "{}",
      }),
    ).toBe(true);
  });

  it("isTabToLeader rejects CONNECT missing required fields", () => {
    expect(isTabToLeader({ t: "CONNECT" })).toBe(false);
    expect(
      isTabToLeader({
        t: "CONNECT",
        tabId: "tab-1",
        bornAt: 0,
        scope: "s",
        protocolVersion: 1,
        jazzPackageVersion: "0",
        // missing appId / dbName / schemaJson
      }),
    ).toBe(false);
  });

  it("isTabToLeader accepts GOODBYE", () => {
    expect(isTabToLeader({ t: "GOODBYE" })).toBe(true);
  });

  it("isLeaderToTab accepts PEER_PORT (port reference is identity-checked)", () => {
    const ch = new MessageChannel();
    expect(
      isLeaderToTab({
        t: "PEER_PORT",
        port: ch.port1,
        generation: 1,
      }),
    ).toBe(true);
  });

  it("isLeaderToTab accepts LEADER_FAULT", () => {
    expect(
      isLeaderToTab({
        t: "LEADER_FAULT",
        reason: "version-mismatch",
      }),
    ).toBe(true);
  });
});

describe("JAZZ_PACKAGE_VERSION", () => {
  it("matches the version in package.json (drift guard)", () => {
    const pkg = JSON.parse(
      readFileSync(new URL("../../../package.json", import.meta.url), "utf8"),
    ) as { version: string };
    expect(JAZZ_PACKAGE_VERSION).toBe(pkg.version);
  });
});
