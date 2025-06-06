// @vitest-environment happy-dom

import { CoMap, Group, Loaded, Service, co, z } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { experimental_useServiceSender } from "../index.js";
import { createJazzTestAccount, linkAccounts } from "../testing.js";
import { renderHook } from "./testUtils.js";

describe("useServiceSender", () => {
  it("should send the message to the service", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount();
    const serviceReceiver = await createJazzTestAccount();

    await linkAccounts(account, serviceReceiver);

    const { result } = renderHook(
      () =>
        experimental_useServiceSender<
          Loaded<typeof TestMap>,
          Loaded<typeof TestMap>
        >(serviceReceiver.id),
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

    const service = await Service.load(serviceReceiver);

    const incoming = await new Promise<Loaded<typeof TestMap>>((resolve) => {
      service.subscribe(TestMap, async (message) => {
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
