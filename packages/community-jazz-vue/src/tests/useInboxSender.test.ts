// @vitest-environment happy-dom

import { CoMap, Group, Inbox, type Loaded, co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { describe, expect, it } from "vitest";
import { experimental_useInboxSender } from "../composables.js";
import { createJazzTestAccount, linkAccounts } from "../testing.js";
import { withJazzTestSetup } from "./testUtils.js";
import { ref } from "vue";

describe("useInboxSender", () => {
  it("should send the message to the inbox", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount();
    const inboxReceiver = await createJazzTestAccount();

    await linkAccounts(account, inboxReceiver);

    const [result] = withJazzTestSetup(
      () =>
        experimental_useInboxSender<
          Loaded<typeof TestMap>,
          Loaded<typeof TestMap>
        >(inboxReceiver.$jazz.id),
      {
        account,
      },
    );

    const sendMessage = result;

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

  it("should accept reactive input", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount();
    const inboxReceiver1 = await createJazzTestAccount();
    const inboxReceiver2 = await createJazzTestAccount();

    await linkAccounts(account, inboxReceiver1);
    await linkAccounts(account, inboxReceiver2);

    const id = ref(inboxReceiver1.$jazz.id);
    const [result] = withJazzTestSetup(
      () =>
        experimental_useInboxSender<
          Loaded<typeof TestMap>,
          Loaded<typeof TestMap>
        >(id),
      {
        account,
      },
    );

    const sendMessage = result;

    sendMessage(
      TestMap.create(
        { value: "hello" },
        { owner: Group.create({ owner: account }) },
      ),
    );

    const inbox1 = await Inbox.load(inboxReceiver1);

    const incoming1 = await new Promise<Loaded<typeof TestMap>>((resolve) => {
      inbox1.subscribe(TestMap, async (message) => resolve(message));
    });

    expect(incoming1.value).toEqual("hello");

    // now the second inbox
    id.value = inboxReceiver2.$jazz.id;

    sendMessage(
      TestMap.create(
        { value: "hello to you too" },
        { owner: Group.create({ owner: account }) },
      ),
    );
    const inbox2 = await Inbox.load(inboxReceiver2);

    const incoming2 = await new Promise<Loaded<typeof TestMap>>((resolve) => {
      inbox2.subscribe(TestMap, async (message) => resolve(message));
    });

    expect(incoming2.value).toEqual("hello to you too");
  });
});
