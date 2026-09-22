// @vitest-environment jsdom
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { createApp, h, nextTick, shallowRef } from "vue";
import type { AccountDbConfig } from "../accounts/context.js";
import { makeFakeAccount } from "../react-core/test-utils.js";

const mocks = vi.hoisted(() => ({ createJazzClient: vi.fn() }));
vi.mock("./create-jazz-client.js", () => ({ createJazzClient: mocks.createJazzClient }));
import { JazzProvider } from "./provider.js";

function client() {
  return {
    db: { onAuthChanged: vi.fn(() => () => {}) },
    session: null,
    shutdown: vi.fn().mockResolvedValue(undefined),
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

const cleanups: (() => void)[] = [];
beforeEach(() => mocks.createJazzClient.mockReset());
afterEach(() => cleanups.splice(0).forEach((cleanup) => cleanup()));

function mount() {
  const config = shallowRef<AccountDbConfig>({
    appId: "retry",
    driver: { type: "memory" },
    account: makeFakeAccount("retry"),
  });
  const errors: unknown[] = [];
  const app = createApp({
    render: () =>
      h(
        JazzProvider,
        { config: config.value, autoAttachDevTools: false },
        {
          default: () => h("p", "ready"),
          fallback: () => h("p", "loading"),
        },
      ),
  });
  app.config.errorHandler = (error) => {
    errors.push(error);
  };
  const element = document.createElement("div");
  document.body.append(element);
  app.mount(element);
  cleanups.push(() => {
    app.unmount();
    element.remove();
  });
  return { config, errors, element };
}

it("retries an equivalent config after initialization fails", async () => {
  const failure = new Error("temporary storage failure");
  mocks.createJazzClient.mockRejectedValueOnce(failure).mockResolvedValue(client());
  const { config, errors, element } = mount();
  await vi.waitFor(() => expect(errors).toEqual([failure]));
  config.value = { account: config.value.account, driver: { type: "memory" }, appId: "retry" };
  await vi.waitFor(() => expect(element.textContent).toBe("ready"));
  expect(mocks.createJazzClient).toHaveBeenCalledTimes(2);
});

it("retries an equivalent replacement after handover shutdown fails", async () => {
  const first = client();
  const failure = new Error("temporary shutdown failure");
  first.shutdown.mockRejectedValueOnce(failure);
  mocks.createJazzClient.mockResolvedValueOnce(first).mockResolvedValue(client());
  const { config, errors, element } = mount();
  await vi.waitFor(() => expect(element.textContent).toBe("ready"));
  config.value = {
    account: config.value.account,
    appId: "replacement",
    driver: { type: "memory" },
  };
  await vi.waitFor(() => expect(errors).toEqual([failure]));
  expect(mocks.createJazzClient).toHaveBeenCalledTimes(1);
  config.value = {
    account: config.value.account,
    driver: { type: "memory" },
    appId: "replacement",
  };
  await vi.waitFor(() => expect(element.textContent).toBe("ready"));
  expect(mocks.createJazzClient).toHaveBeenCalledTimes(2);
});

it("a stale failure preserves the newer pending and ready config identity", async () => {
  const first = deferred<ReturnType<typeof client>>();
  const second = deferred<ReturnType<typeof client>>();
  const ready = client();
  mocks.createJazzClient.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
  const { config, errors, element } = mount();
  await vi.waitFor(() => expect(mocks.createJazzClient).toHaveBeenCalledTimes(1));
  config.value = {
    account: config.value.account,
    appId: "replacement",
    driver: { type: "memory" },
  };
  await nextTick();
  first.reject(new Error("stale initialization failure"));
  await vi.waitFor(() => expect(mocks.createJazzClient).toHaveBeenCalledTimes(2));
  config.value = {
    account: config.value.account,
    driver: { type: "memory" },
    appId: "replacement",
  };
  await nextTick();
  second.resolve(ready);
  await vi.waitFor(() => expect(element.textContent).toBe("ready"));
  config.value = {
    account: config.value.account,
    appId: "replacement",
    driver: { type: "memory" },
  };
  await nextTick();
  expect(mocks.createJazzClient).toHaveBeenCalledTimes(2);
  expect(ready.shutdown).not.toHaveBeenCalled();
  expect(errors).toEqual([]);
});
