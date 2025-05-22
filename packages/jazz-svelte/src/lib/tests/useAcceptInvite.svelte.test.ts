import { render, waitFor } from "@testing-library/svelte";
import { Account, Group, co, z, type CoValue, type CoValueOrZodSchema, type ID } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { createInviteLink } from "../index.js";
import { createJazzTestAccount, createJazzTestContext, linkAccounts } from "../testing.js";
import UseAcceptInvite from "./components/useAcceptInvite.svelte";

function setup<T extends CoValueOrZodSchema>(options: {
  account: Account;
  invitedObjectSchema: CoValueOrZodSchema;
}) {
  const result = { current: undefined } as { current: ID<T> | undefined };

    render(UseAcceptInvite, {
      context: createJazzTestContext({ account: options.account }),
      props: {
        invitedObjectSchema: options.invitedObjectSchema,
        onAccept: (id: ID<CoValue>) => {
          result.current = id as ID<T>;
        },
      },
    });

  return result;
}

describe("useAcceptInvite", () => {
  it("should accept the invite", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount();
    const inviteSender = await createJazzTestAccount();

    await linkAccounts(account, inviteSender);

    const invitelink = createInviteLink(
      TestMap.create(
        { value: "hello" },
        { owner: Group.create({ owner: inviteSender }) },
      ),
      "reader",
    );

    location.href = invitelink;

    const result = setup({
      account,
      invitedObjectSchema: TestMap,
    });

    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    const accepted = await TestMap.load(result.current!, {
      loadAs: account,
    });

    expect(accepted?.value).toEqual("hello");
  });
});