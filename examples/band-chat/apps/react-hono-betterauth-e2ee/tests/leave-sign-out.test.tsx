// @vitest-environment jsdom
// The page's half of leaving a browser: it signs out once it sees a new
// device ID for the account that asked to leave, and only then.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { within, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { DevicesPanel } from "../src/components/DevicesPanel";
import { clearLeaveRecord, readLeaveRecord, writeLeaveRecord } from "../src/lib/leave-record";
import { clearPendingMarkerIds } from "../src/lib/mark-read";
import { encryptionError, fakeEncryption } from "./fake-encryption";

const ACCOUNT = "account-alice";

const mocks = vi.hoisted(() => ({
  fake: null as unknown as ReturnType<typeof import("./fake-encryption").fakeEncryption>,
  accountId: "account-alice",
  showDevices: false,
  authSignOut: vi.fn(async () => ({ data: { success: true }, error: null }) as unknown),
  jazzRefresh: vi.fn(async () => {}),
  lifecycleSignOut: vi.fn(async () => {}),
}));

vi.mock("jazz-tools/react", () => ({
  useDb: () => ({ encryption: mocks.fake.encryption }),
  useJazzSession: () => ({ user: { accountId: mocks.accountId }, refresh: mocks.jazzRefresh }),
}));
vi.mock("../components/jazz-provider", () => ({
  JazzProvider: ({ children }: { children: React.ReactNode }) => children,
  useBandChatLifecycle: () => ({ signOut: mocks.lifecycleSignOut }),
}));
vi.mock("../components/theme-provider", () => ({
  ThemeProvider: ({ children }: { children: React.ReactNode }) => children,
}));
vi.mock("../components/sign-in-form", () => ({ SignInForm: () => <p>Sign in</p> }));
vi.mock("../src/BandChat", () => ({
  BandChat: ({ onSignOut }: { onSignOut: () => void }) => (
    <>
      <button type="button" onClick={onSignOut}>
        Sign out
      </button>
      {mocks.showDevices ? <DevicesPanel accountName="Alice" /> : null}
    </>
  ),
}));
vi.mock("../src/lib/auth-client", () => ({
  authClient: {
    useSession: () => ({ data: { user: { name: "Alice" } }, isPending: false }),
    signOut: mocks.authSignOut,
  },
}));

const { App } = await import("../src/App");

beforeEach(() => {
  localStorage.clear();
  mocks.accountId = ACCOUNT;
  mocks.showDevices = false;
  mocks.authSignOut.mockClear();
  mocks.jazzRefresh.mockClear();
  mocks.lifecycleSignOut.mockClear();
  mocks.lifecycleSignOut.mockImplementation(async () => {
    const { error } = (await mocks.authSignOut()) as { error: { message?: string } | null };
    if (error) throw new Error(error.message);
    clearPendingMarkerIds();
    clearLeaveRecord();
    await mocks.jazzRefresh();
  });
});

afterEach(() => cleanup());

/** Lets a few checks run. */
async function settle(fake: ReturnType<typeof fakeEncryption>) {
  await waitFor(() => expect(fake.handle.deviceId.mock.calls.length).toBeGreaterThan(0));
  await new Promise((resolve) => setTimeout(resolve, 50));
}

describe("a browser waiting for approval", () => {
  it.each(["awaiting-approval", "reenrolled"] as const)(
    "is told so wherever the user is (%s), with the code to type elsewhere",
    async (status) => {
      const fake = (mocks.fake = fakeEncryption({ status, devices: [] }));
      render(<App />);
      await screen.findByText("This browser is waiting for approval");
      expect(screen.getByText(/restore with your recovery phrase or passkey/)).not.toBeNull();
      fake.challenge();
      await screen.findByText("654321");
    },
  );
});

describe("finishing leaving this browser", () => {
  it("signs out after the page closes before it hears back about its removal request", async () => {
    const fake = (mocks.fake = fakeEncryption());
    mocks.showDevices = true;
    fake.handle.devices.requestRemoval.mockReturnValueOnce(new Promise<void>(() => {}));
    const firstPage = render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Leave this browser" }));
    fireEvent.click(
      within(screen.getByRole("alertdialog")).getByRole("button", { name: "Leave this browser" }),
    );
    expect(fake.handle.devices.requestRemoval).toHaveBeenCalledOnce();
    firstPage.unmount();

    // The request reached the other device even though the page missed its answer.
    mocks.fake = fakeEncryption({
      deviceId: "device-new",
      status: "awaiting-approval",
      devices: [],
    });
    render(<App />);
    await waitFor(() => expect(mocks.authSignOut).toHaveBeenCalledOnce());
    expect(readLeaveRecord()).toBeNull();
  });

  it.each(["reenrolled", "awaiting-approval"] as const)(
    "signs out at start-up when the device ID changed (%s), then drops the record",
    async (status) => {
      writeLeaveRecord({ accountId: ACCOUNT, deviceId: "device-old" });
      mocks.fake = fakeEncryption({ deviceId: "device-new", status, devices: [] });
      render(<App />);
      await waitFor(() => expect(mocks.authSignOut).toHaveBeenCalledTimes(1));
      await waitFor(() => expect(readLeaveRecord()).toBeNull());
      expect(mocks.jazzRefresh).toHaveBeenCalled();
    },
  );

  it("does not sign out while the device ID is the same", async () => {
    writeLeaveRecord({ accountId: ACCOUNT, deviceId: "device-this" });
    mocks.fake = fakeEncryption({ deviceId: "device-this" });
    render(<App />);
    await settle(mocks.fake);
    expect(mocks.authSignOut).not.toHaveBeenCalled();
    expect(readLeaveRecord()).toEqual({ accountId: ACCOUNT, deviceId: "device-this" });
  });

  it("signs out when the device list changes to a new device ID, wherever the user is", async () => {
    writeLeaveRecord({ accountId: ACCOUNT, deviceId: "device-this" });
    const fake = (mocks.fake = fakeEncryption({
      deviceId: "device-this",
      devices: [
        { id: "device-this", status: "active" },
        { id: "device-other", status: "active" },
      ],
    }));
    render(<App />);
    await settle(fake);
    expect(mocks.authSignOut).not.toHaveBeenCalled();
    // Another device completed the removal; Jazz erased the keys and
    // enrolled this browser again.
    fake.state.deviceId = "device-renewed";
    fake.state.status = "awaiting-approval";
    fake.state.devices = [
      { id: "device-other", status: "active" },
      { id: "device-renewed", status: "waiting" },
    ];
    await waitFor(() => expect(mocks.authSignOut).toHaveBeenCalledTimes(1), { timeout: 5000 });
    await waitFor(() => expect(readLeaveRecord()).toBeNull());
  });

  it("finishes leaving when the replacement device cannot list devices", async () => {
    writeLeaveRecord({ accountId: ACCOUNT, deviceId: "device-this" });
    const fake = (mocks.fake = fakeEncryption());
    render(<App />);
    await settle(fake);
    expect(mocks.authSignOut).not.toHaveBeenCalled();

    fake.state.deviceId = "device-renewed";
    fake.state.status = "awaiting-approval";
    fake.handle.devices.list.mockRejectedValue(encryptionError("device-not-active"));

    await waitFor(() => expect(mocks.authSignOut).toHaveBeenCalledTimes(1), { timeout: 5000 });
    await waitFor(() => expect(readLeaveRecord()).toBeNull());
  });

  it("stays signed in, keeps the record and offers a retry when erasing the keys failed", async () => {
    writeLeaveRecord({ accountId: ACCOUNT, deviceId: "device-this" });
    mocks.fake = fakeEncryption({ deviceId: "device-this" });
    mocks.fake.state.failure = encryptionError("closed", "the encryption runtime is closed");
    render(<App />);
    await screen.findByText("Leaving this browser did not finish");
    expect(
      screen.getByText(/its keys could not be erased, so they may still be here/),
    ).not.toBeNull();
    expect(screen.getByRole("button", { name: "Try again" })).not.toBeNull();
    await settle(mocks.fake);
    expect(mocks.authSignOut).not.toHaveBeenCalled();
    expect(readLeaveRecord()).toEqual({ accountId: ACCOUNT, deviceId: "device-this" });
  });

  it("ignores a wish to leave recorded by another account", async () => {
    writeLeaveRecord({ accountId: "account-bob", deviceId: "device-bob" });
    mocks.fake = fakeEncryption({ deviceId: "device-this" });
    render(<App />);
    await settle(mocks.fake);
    expect(mocks.authSignOut).not.toHaveBeenCalled();
    expect(readLeaveRecord()).toEqual({ accountId: "account-bob", deviceId: "device-bob" });
  });

  it("an ordinary sign-out drops the record and keeps the keys", async () => {
    writeLeaveRecord({ accountId: ACCOUNT, deviceId: "device-this" });
    mocks.fake = fakeEncryption({ deviceId: "device-this" });
    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Sign out" }));
    await waitFor(() => expect(readLeaveRecord()).toBeNull());
    expect(mocks.lifecycleSignOut).toHaveBeenCalledTimes(1);
    expect(mocks.fake.handle.devices.requestRemoval).not.toHaveBeenCalled();
    expect(mocks.fake.handle.devices.remove).not.toHaveBeenCalled();
  });

  it("reports a menu sign-out failure and keeps the leave record", async () => {
    writeLeaveRecord({ accountId: ACCOUNT, deviceId: "device-this" });
    mocks.fake = fakeEncryption({ deviceId: "device-this" });
    mocks.authSignOut.mockResolvedValueOnce({ data: null, error: { message: "network down" } });
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Sign out" }));

    expect(await screen.findByText("Could not sign out")).not.toBeNull();
    expect(screen.getByText("network down")).not.toBeNull();
    expect(readLeaveRecord()).toEqual({ accountId: ACCOUNT, deviceId: "device-this" });
  });

  it("says so, with a retry, when the sign-out itself fails", async () => {
    writeLeaveRecord({ accountId: ACCOUNT, deviceId: "device-old" });
    mocks.fake = fakeEncryption({
      deviceId: "device-new",
      status: "awaiting-approval",
      devices: [],
    });
    mocks.authSignOut.mockResolvedValueOnce({ data: null, error: { message: "network down" } });
    render(<App />);
    await screen.findByText("This browser has left your account, but signing out failed");
    expect(readLeaveRecord()).not.toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Finish signing out" }));
    await waitFor(() => expect(readLeaveRecord()).toBeNull());
    expect(mocks.authSignOut).toHaveBeenCalledTimes(2);
  });
});
