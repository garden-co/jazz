import { beforeEach, describe, expect, it, vi } from "vitest";
import { Group } from "../coValues/group";
import { Account } from "../exports";
import {
  parseCoValueCreateOptions,
  parseGroupCreateOptions,
  TypeSym,
} from "../internal";
import { createJazzTestAccount } from "../testing";

beforeEach(async () => {
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("parseCoValueCreateOptions", () => {
  it("should create a new group when no options provided", () => {
    const result = parseCoValueCreateOptions(undefined);
    expect(result.owner[TypeSym]).toBe("Group");
    expect(
      result.owner.$jazz
        .castAs(Group)
        .$jazz.raw.roleOf(Account.getMe().$jazz.raw.id),
    ).toBe("admin");
    expect(result.uniqueness).toBeUndefined();
  });

  it("should use the account as the owner when passing an Account", async () => {
    const account = await createJazzTestAccount();
    const result = parseCoValueCreateOptions(account);
    expect(result.owner).toBe(account);
    expect(result.uniqueness).toBeUndefined();
  });

  it("should use existing group when passing a Group", () => {
    const group = Group.create();
    const result = parseCoValueCreateOptions(group);
    expect(result.owner).toBe(group);
    expect(
      result.owner.$jazz
        .castAs(Group)
        .$jazz.raw.roleOf(Account.getMe().$jazz.raw.id),
    ).toBe("admin");
    expect(result.uniqueness).toBeUndefined();
  });

  it("should handle options with uniqueness", () => {
    const group = Group.create();
    const result = parseCoValueCreateOptions({
      unique: "per-group",
      owner: group,
    });
    expect(result.owner).toBe(group);
    expect(result.uniqueness?.uniqueness).toBe("per-group");
  });

  it("should use the account as the owner when passing an Account", async () => {
    const account = await createJazzTestAccount();
    const result = parseCoValueCreateOptions({
      owner: account,
      unique: "per-group",
    });
    expect(result.owner).toBe(account);
    expect(result.uniqueness?.uniqueness).toBe("per-group");
  });
});

describe("parseGroupCreateOptions", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should use active account when no options provided", () => {
    const result = parseGroupCreateOptions(undefined);
    expect(result.owner).toBe(Account.getMe());
  });

  it("should use provided account when passing an Account", async () => {
    const account = await createJazzTestAccount();
    const result = parseGroupCreateOptions(account);
    expect(result.owner).toBe(account);
  });

  it("should use active account when passing empty options", () => {
    const result = parseGroupCreateOptions({});
    expect(result.owner).toBe(Account.getMe());
  });

  it("should use provided account in options.owner", async () => {
    const account = await createJazzTestAccount();
    const result = parseGroupCreateOptions({ owner: account });
    expect(result.owner).toBe(account);
  });
});
