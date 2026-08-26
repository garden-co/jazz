import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, expect, it, vi } from "vitest";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const controls = vi.hoisted(() => ({
  authChanged: null as ((state: { error?: string }) => void) | null,
  session: { user: { id: "principal-a" } } as { user: { id: string } } | null,
  tokenRequests: [] as Array<(token: string | null) => void>,
  updateAuthToken: vi.fn(),
}));

vi.mock("@/src/lib/auth-client", () => ({
  authClient: {
    useSession: () => ({ data: controls.session }),
  },
  getJwtFromBetterAuth: () =>
    new Promise<string | null>((resolve) => controls.tokenRequests.push(resolve)),
}));

vi.mock("jazz-tools/react", () => ({
  JazzProvider: ({
    children,
    config,
  }: {
    children: React.ReactNode;
    config: { jwtToken?: string };
  }) => <section data-token={config.jwtToken}>{children}</section>,
  useDb: () => ({
    onAuthChanged(callback: (state: { error?: string }) => void) {
      controls.authChanged = callback;
      return () => {
        if (controls.authChanged === callback) controls.authChanged = null;
      };
    },
    updateAuthToken: controls.updateAuthToken,
  }),
}));

import { JazzProvider } from "../../components/jazz-provider";

afterEach(() => {
  controls.authChanged = null;
  controls.session = { user: { id: "principal-a" } };
  controls.tokenRequests.splice(0);
  controls.updateAuthToken.mockClear();
});

it("never renders A for B or applies A's delayed refresh after a principal swap", async () => {
  const element = document.createElement("div");
  document.body.append(element);
  const root = createRoot(element);
  await act(async () => root.render(<JazzProvider>rooms</JazzProvider>));

  await act(async () => controls.tokenRequests.shift()!("token-a"));
  expect(element.querySelector("[data-token]")?.getAttribute("data-token")).toBe("token-a");

  await act(async () => controls.authChanged?.({ error: "expired" }));
  const resolveLateARefresh = controls.tokenRequests.shift()!;
  controls.session = { user: { id: "principal-b" } };
  await act(async () => root.render(<JazzProvider>rooms</JazzProvider>));
  expect(element.querySelector("[data-token]")).toBeNull();

  const resolveBConnection = controls.tokenRequests.shift()!;
  await act(async () => resolveLateARefresh("token-b-returned-to-a-refresh"));
  expect(controls.updateAuthToken).not.toHaveBeenCalled();
  await act(async () => resolveBConnection("token-b"));
  expect(element.querySelector("[data-token]")?.getAttribute("data-token")).toBe("token-b");

  await act(async () => root.unmount());
  element.remove();
});
