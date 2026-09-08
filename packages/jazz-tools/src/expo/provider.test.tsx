import React from "react";
import { act, cleanup, render } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
const host = vi.hoisted(() => ({ create: vi.fn() }));
vi.mock("./create-jazz-session.js", () => ({ createJazzSession: host.create }));
vi.mock("react-native", () => ({
  View: "native-view",
  Text: "native-text",
  Pressable: "native-pressable",
}));
import { JazzProvider } from "./provider.js";
afterEach(cleanup);
it("uses the Expo host factory and native loading/recovery controls", async () => {
  host.create.mockRejectedValue(new Error("secure store unavailable"));
  const view = render(
    <JazzProvider appId="expo-app" serverUrl="https://sync.example.test">
      <span>data</span>
    </JazzProvider>,
  );
  expect(view.container.querySelector("native-view native-text")?.textContent).toBe("Loading…");
  await act(async () => {
    await Promise.resolve();
  });
  expect(host.create).toHaveBeenCalledWith(expect.objectContaining({ initial: "local-first" }));
  expect(view.container.querySelector("native-pressable")?.textContent).toBe("Try again");
  expect(view.container.querySelector("section, p, button")).toBeNull();
});
