import type { Endpoint } from "comlink";

type RuntimePortListener = (message: unknown) => void;

type RuntimePortListenerStore = {
  addListener(listener: RuntimePortListener): void;
  removeListener(listener: RuntimePortListener): void;
};

export interface ChromeRuntimePortLike {
  postMessage(message: unknown): void;
  onMessage: RuntimePortListenerStore;
}

function dispatchMessageEvent(listener: EventListenerOrEventListenerObject, data: unknown): void {
  const event = { data } as MessageEvent;
  if (typeof listener === "function") {
    listener(event);
    return;
  }
  listener.handleEvent(event);
}

export function createChromeRuntimePortEndpoint(port: ChromeRuntimePortLike): Endpoint {
  const listenerMap = new Map<EventListenerOrEventListenerObject, RuntimePortListener>();

  return {
    postMessage(message) {
      port.postMessage(message);
    },
    addEventListener(type, listener) {
      if (type !== "message" || listenerMap.has(listener)) {
        return;
      }

      const runtimeListener = (message: unknown) => {
        dispatchMessageEvent(listener, message);
      };

      listenerMap.set(listener, runtimeListener);
      port.onMessage.addListener(runtimeListener);
    },
    removeEventListener(type, listener) {
      if (type !== "message") {
        return;
      }

      const runtimeListener = listenerMap.get(listener);
      if (!runtimeListener) {
        return;
      }

      listenerMap.delete(listener);
      port.onMessage.removeListener(runtimeListener);
    },
    start() {
      // chrome.runtime.Port does not need an explicit start step.
    },
  };
}
