// @vitest-environment happy-dom

import { CoMap, Group, Inbox, co } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { experimental_useInboxSender } from "../index.js";
import { createJazzTestAccount, linkAccounts } from "../testing.js";
import { renderHook } from "./testUtils.js";

describe("useInboxSender", () => {
  it("should send the message to the inbox", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const account = await createJazzTestAccount();
    const inboxReceiver = await createJazzTestAccount();

    await linkAccounts(account, inboxReceiver);

    const { result } = renderHook(
      () => experimental_useInboxSender<TestMap, TestMap>(inboxReceiver.id),
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

    const incoming = await new Promise<TestMap>((resolve) => {
      inbox.subscribe(TestMap, async (message) => {
        resolve(message);

        return TestMap.create({ value: "got it" }, { owner: message._owner });
      });
    });

    expect(incoming.value).toEqual("hello");
    const response = await promise;
    const responseMap = await TestMap.load(response, {
      loadAs: account,
    });

    expect(responseMap!.value).toEqual("got it");
  });
});
