// @vitest-environment jsdom
// A browser whose sign-in the server ended (another of the user's browsers
// removed a lost device) must reach the sign-in form, not wait on
// "Signing in…" while the page still holds the old session.
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";

const mocks = vi.hoisted(() => ({
  /** What the server says about the Better Auth session. */
  server: { data: null as unknown, error: null as unknown },
  refetch: vi.fn(async () => {}),
  jazzRefresh: vi.fn(async () => {}),
}));

vi.mock("jazz-tools/react", () => ({
  JazzProvider: () => null,
  useJazzSession: () => ({ refresh: mocks.jazzRefresh }),
}));
vi.mock("@/src/lib/auth-client", () => ({
  authClient: {
    useSession: () => ({ refetch: mocks.refetch }),
    getSession: async () => mocks.server,
  },
}));

const { SignedOutScreen } = await import("../components/jazz-provider");

beforeEach(() => {
  mocks.server = { data: null, error: null };
  mocks.refetch.mockClear();
  mocks.jazzRefresh.mockClear();
});
afterEach(() => cleanup());

it("updates the page's session when the server has none, so the sign-in form shows", async () => {
  render(<SignedOutScreen />);
  expect(screen.getByText("Signing in…")).not.toBeNull();
  await waitFor(() => expect(mocks.refetch).toHaveBeenCalled());
  expect(mocks.jazzRefresh).not.toHaveBeenCalled();
});

it("signs Jazz in again when the server still has the session", async () => {
  mocks.server = { data: { user: { id: "u" }, session: {} }, error: null };
  render(<SignedOutScreen />);
  await waitFor(() => expect(mocks.jazzRefresh).toHaveBeenCalled());
  expect(mocks.refetch).not.toHaveBeenCalled();
});

it("asks again after a failed request, and only then acts", async () => {
  mocks.server = { data: null, error: { status: 0, message: "offline" } };
  render(<SignedOutScreen retryMs={20} />);
  await new Promise((resolve) => setTimeout(resolve, 60));
  expect(mocks.refetch).not.toHaveBeenCalled();
  mocks.server = { data: null, error: null };
  await waitFor(() => expect(mocks.refetch).toHaveBeenCalled());
});
