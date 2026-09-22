// @vitest-environment happy-dom
import React, { act, StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vitest";
import { useJazzSessionOwner } from "jazz-tools/react-core";
import type { JazzSession } from "jazz-tools/react";

it("retains the owned session through Strict Mode and closes on real unmount", async () => {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  const close = vi.fn(async () => {});
  const session = { close } as unknown as JazzSession<never>;
  const create = vi.fn(async () => session);
  function App() {
    const owner = useJazzSessionOwner({}, create);
    return React.createElement("p", null, owner.session ? "ready" : "loading");
  }
  const element = document.createElement("div");
  const root = createRoot(element);
  await act(async () =>
    root.render(React.createElement(StrictMode, null, React.createElement(App))),
  );
  expect(element.textContent).toBe("ready");
  expect(create).toHaveBeenCalledOnce();
  expect(close).not.toHaveBeenCalled();
  await act(async () => root.unmount());
  await vi.waitFor(() => expect(close).toHaveBeenCalledOnce());
});
