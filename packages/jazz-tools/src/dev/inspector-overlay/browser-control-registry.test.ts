import { afterEach, describe, expect, it } from "vitest";
import {
  deserializeBrowserRelayError,
  type BrowserInspectorControlEvent,
  type BrowserInspectorControlRequest,
} from "../../runtime/native-runtime/browser-worker-protocol.js";
import {
  openAggregatedBrowserInspectorControlPort,
  registerBrowserInspectorControl,
} from "./browser-control-registry.js";

const disposers: Array<() => void> = [];

afterEach(() => {
  for (const dispose of disposers.splice(0)) dispose();
});

function waitForEvent<T extends BrowserInspectorControlEvent>(
  port: MessagePort,
  predicate: (event: BrowserInspectorControlEvent) => event is T,
): Promise<T> {
  return new Promise((resolve) => {
    const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
      if (!predicate(event.data)) return;
      port.removeEventListener("message", onMessage);
      resolve(event.data);
    };
    port.addEventListener("message", onMessage);
  });
}

describe("aggregated inspector browser control", () => {
  it("fails closed when a registered control returns a malformed relay error", async () => {
    const channel = new MessageChannel();
    channel.port2.addEventListener(
      "message",
      (event: MessageEvent<BrowserInspectorControlRequest>) => {
        const request = event.data;
        if (request.type === "list-contexts") {
          channel.port2.postMessage({
            type: "contexts",
            id: request.id,
            contexts: [
              {
                workerRealmId: "realm",
                key: "context",
                appId: "app",
                dbName: "db",
                schema: {} as never,
              },
            ],
          });
        } else if (request.type === "attach-context") {
          // A control port is untrusted input to this multiplexing layer. The
          // callback must not expose its malformed payload as a raw string.
          channel.port2.postMessage({ type: "result", id: request.id, error: { message: 1 } });
        }
      },
    );
    channel.port2.start();
    disposers.push(registerBrowserInspectorControl(async () => channel.port1));

    const outer = await openAggregatedBrowserInspectorControlPort(async () => {
      throw new Error("fallback should not be used");
    });
    const listed = waitForEvent(
      outer,
      (event): event is Extract<BrowserInspectorControlEvent, { type: "contexts" }> =>
        event.type === "contexts" && event.id === 0,
    );
    outer.postMessage({ type: "list-contexts", id: 0 } satisfies BrowserInspectorControlRequest);
    await listed;
    const result = waitForEvent(
      outer,
      (event): event is Extract<BrowserInspectorControlEvent, { type: "result" }> =>
        event.type === "result" && event.id === 1,
    );
    const attached = new MessageChannel();
    outer.postMessage(
      {
        type: "attach-context",
        id: 1,
        contextKey: "1:context",
        tabId: "tab",
        port: attached.port1,
      } satisfies BrowserInspectorControlRequest,
      [attached.port1],
    );

    const event = await result;
    // This is deliberately an assertion about the bytes crossing the outer
    // MessagePort, not merely the receiver's fallback. Replacing the outer
    // catch with `error: String(error)` must make this fail.
    expect(event.error).toEqual(
      expect.objectContaining({
        name: "BrowserRelayErrorProtocolError",
        message: "Invalid browser relay error payload",
        code: "browser_relay_error_protocol_violation",
      }),
    );
    expect(Object.hasOwn(event.error!, "name")).toBe(true);
    expect(Object.hasOwn(event.error!, "message")).toBe(true);
    expect(Object.hasOwn(event.error!, "code")).toBe(true);
    expect(deserializeBrowserRelayError(event.error!)).toMatchObject({
      name: "BrowserRelayErrorProtocolError",
    });
    outer.close();
    attached.port2.close();
    channel.port2.close();
  });

  it("serializes an outer inspector callback failure before returning it", async () => {
    // Keep the registry on its aggregate path. The command below is rejected
    // before this control is contacted.
    const channel = new MessageChannel();
    channel.port2.start();
    disposers.push(registerBrowserInspectorControl(async () => channel.port1));
    const outer = await openAggregatedBrowserInspectorControlPort(async () => {
      throw new Error("fallback should not be used");
    });
    const result = waitForEvent(
      outer,
      (event): event is Extract<BrowserInspectorControlEvent, { type: "result" }> =>
        event.type === "result" && event.id === 7,
    );

    // The aggregate-control callback itself rejects this direct-only command.
    // Keep this distinct from the malformed inner-control case above so the
    // outer catch's structured envelope has a direct regression guard.
    outer.postMessage({ type: "terminate-worker", id: 7 } satisfies BrowserInspectorControlRequest);

    const event = await result;
    expect(event.error).toEqual(
      expect.objectContaining({
        name: "Error",
        message: "Worker termination is only available on a direct browser control port",
      }),
    );
    expect(Object.hasOwn(event.error!, "name")).toBe(true);
    expect(Object.hasOwn(event.error!, "message")).toBe(true);
    expect(deserializeBrowserRelayError(event.error!)).toMatchObject({
      name: "Error",
      message: "Worker termination is only available on a direct browser control port",
    });
    outer.close();
    channel.port2.close();
  });
});
