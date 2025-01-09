import { render, waitFor } from "@testing-library/svelte";
import { Account, CoMap, Group, co, type CoValue, type CoValueClass, type ID } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { createInviteLink } from "../index.js";
import { createJazzTestAccount, createJazzTestContext, linkAccounts } from "../testing.js";
import UseAcceptInvite from "./components/useAcceptInvite.svelte";

function setup<T extends CoValue>(options: {
  account: Account;
  invitedObjectSchema: CoValueClass<T>;
}) {
  const result = { current: undefined } as { current: ID<T> | undefined };

    render(UseAcceptInvite, {
      context: createJazzTestContext({ account: options.account }),
      props: {
        invitedObjectSchema: options.invitedObjectSchema,
        onAccept: (id: ID<T>) => {
          result.current = id;
        },
      },
    });

  return result;
}

describe("useAcceptInvite", () => {
  it("should accept the invite", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const account = await createJazzTestAccount();
    const inviteSender = await createJazzTestAccount();

    linkAccounts(account, inviteSender);

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

    const accepted = await TestMap.load(result.current!, account, {});

    expect(accepted?.value).toEqual("hello");
  });
});