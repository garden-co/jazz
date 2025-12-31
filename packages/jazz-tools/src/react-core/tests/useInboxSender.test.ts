// @vitest-environment happy-dom

import { CoMap, Group, Inbox, Loaded, co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { describe, expect, it } from "vitest";
import { experimental_useInboxSender, useJazzContext } from "../index.js";
import {
  createJazzTestAccount,
  linkAccounts,
  setupJazzTestSync,
} from "../testing.js";
import { act, renderHook } from "./testUtils.js";

describe("useInboxSender", () => {
  it("should send the message to the inbox", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount();
    const inboxReceiver = await createJazzTestAccount();

    await linkAccounts(account, inboxReceiver);

    const { result } = renderHook(
      () =>
        experimental_useInboxSender<
          Loaded<typeof TestMap>,
          Loaded<typeof TestMap>
        >(inboxReceiver.$jazz.id),
      {
        account,
      },
    );

    const sendMessage = result.current;

    const promise = sendMessage(
      TestMap.create(
        { value: "hello" },
        { owner: Group.create({ owner: account }) },
      ),
    );

    const inbox = await Inbox.load(inboxReceiver);

    const incoming = await new Promise<Loaded<typeof TestMap>>((resolve) => {
      inbox.subscribe(TestMap, async (message) => {
        resolve(message);

        return TestMap.create(
          { value: "got it" },
          { owner: message.$jazz.owner },
        );
      });
    });

    expect(incoming.value).toEqual("hello");
    const response = await promise;
    const responseMap = await TestMap.load(response, {
      loadAs: account,
    });

    assertLoaded(responseMap);
    expect(responseMap.value).toEqual("got it");
  });

  it("should regenerate the InboxSender if the active account changes", async () => {
    const account1 = await setupJazzTestSync();
    const account2 = await createJazzTestAccount();
    const inboxReceiver = await createJazzTestAccount();

    const { result } = renderHook(
      () => {
        const ctx = useJazzContext();
        const send = experimental_useInboxSender(inboxReceiver.$jazz.id);
        return { ctx, send };
      },
      { account: account1 },
    );

    const before = result.current.send;

    await act(async () => {
      await result.current.ctx.authenticate({
        accountID: account2.$jazz.id,
        accountSecret: account2.$jazz.localNode.getCurrentAgent().agentSecret,
      });
    });

    const after = result.current.send;
    expect(after).not.toBe(before);
  });
});
