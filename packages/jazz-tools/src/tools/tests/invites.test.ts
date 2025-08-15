import { beforeEach, describe, expect, test } from "vitest";
import { Account, Group } from "../exports";
import {
  consumeInviteLink,
  createInviteLink,
  parseInviteLink,
} from "../implementation/invites";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";

describe("Invite Links", () => {
  let account: Account;
  let group: Group;
  const baseURL = "https://example.com";

  beforeEach(async () => {
    await setupJazzTestSync(); // Required to sync the invite between accounts
    account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
    group = Group.create({ owner: account });
  });

  test("createInviteLink generates correct format", () => {
    const inviteLink = createInviteLink(group, "writer", baseURL);

    expect(inviteLink).toMatch(
      new RegExp(`^${baseURL}#/invite/${group.$jazz.id}/[A-Za-z0-9_-]+$`),
    );
  });

  test("createInviteLink with valueHint", () => {
    const inviteLink = createInviteLink(group, "writer", baseURL, "myGroup");

    expect(inviteLink).toMatch(
      new RegExp(
        `^${baseURL}#/invite/myGroup/${group.$jazz.id}/[A-Za-z0-9_-]+$`,
      ),
    );
  });

  test("parseInviteLink correctly parses valid link", () => {
    const inviteLink = createInviteLink(group, "writer", baseURL, "myGroup");
    const result = parseInviteLink(inviteLink);

    expect(result).toBeDefined();
    expect(result?.valueID).toBe(group.$jazz.id);
    expect(result?.valueHint).toBe("myGroup");
    expect(result?.inviteSecret).toBeDefined();
  });

  test("parseInviteLink returns undefined for invalid link", () => {
    const invalidLink = "https://example.com/not-an-invite";
    const result = parseInviteLink(invalidLink);

    expect(result).toBeUndefined();
  });

  test("consumeInviteLink accepts valid invite", async () => {
    const inviteLink = createInviteLink(group, "writer", baseURL, "myGroup");
    const newAccount = await createJazzTestAccount();

    const result = await consumeInviteLink({
      inviteURL: inviteLink,
      as: newAccount,
      forValueHint: "myGroup",
      invitedObjectSchema: Group,
    });

    expect(result).toBeDefined();
    expect(result?.valueID).toBe(group.$jazz.id);
    expect(result?.valueHint).toBe("myGroup");
  });

  test("consumeInviteLink returns undefined for mismatched valueHint", async () => {
    const inviteLink = createInviteLink(group, "writer", baseURL, "myGroup");
    const newAccount = await createJazzTestAccount();

    const result = await consumeInviteLink({
      inviteURL: inviteLink,
      as: newAccount,
      forValueHint: "wrongHint",
      invitedObjectSchema: Group,
    });

    expect(result).toBeUndefined();
  });
});
