// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { DevicesPanel } from "../src/components/DevicesPanel";
import { EncryptionNotices } from "../src/components/EncryptionNotices";
import { DeviceStateProvider } from "../src/lib/device-state";
import { readLeaveRecord, writeLeaveRecord } from "../src/lib/leave-record";
import { encryptionError, fakeEncryption } from "./fake-encryption";

const ACCOUNT = "account-alice";

function show(fake: ReturnType<typeof fakeEncryption>) {
  return render(
    <DeviceStateProvider encryption={fake.encryption} accountId={ACCOUNT} pollMs={20}>
      <EncryptionNotices />
      <DevicesPanel accountName="Alice" />
    </DeviceStateProvider>,
  );
}

function row(deviceId: string): HTMLElement {
  const element = document.querySelector<HTMLElement>(`[data-device-id="${deviceId}"]`);
  if (!element) throw new Error(`no row for ${deviceId}`);
  return element;
}

const fetchMock = vi.fn<typeof fetch>();

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  fetchMock.mockReset();
  fetchMock.mockImplementation(
    async () => new Response(JSON.stringify({ ended: 2 }), { status: 200 }),
  );
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const threeDevices = () =>
  fakeEncryption({
    deviceId: "device-this",
    devices: [
      { id: "device-this", status: "active" },
      { id: "device-lost", status: "active" },
      { id: "device-new", status: "waiting" },
    ],
  });

describe("removing a lost device", () => {
  it("is offered on other active devices only, never on this browser or a waiting one", async () => {
    show(threeDevices());
    await screen.findByText("Device device-los…");
    expect(
      within(row("device-lost")).queryByRole("button", { name: "Remove lost device" }),
    ).not.toBeNull();
    expect(
      within(row("device-this")).queryByRole("button", { name: "Remove lost device" }),
    ).toBeNull();
    // A waiting entry, such as the one a browser that left becomes, has no remove button.
    expect(
      within(row("device-new")).queryByRole("button", { name: "Remove lost device" }),
    ).toBeNull();
  });

  it("states the cost before the user confirms, and does nothing until then", async () => {
    const fake = threeDevices();
    show(fake);
    fireEvent.click(
      await within(await findRow("device-lost")).findByRole("button", {
        name: "Remove lost device",
      }),
    );
    const confirm = screen.getByRole("alertdialog", { name: "Remove this lost device?" });
    expect(confirm.textContent).toContain(
      "Every other browser where you're signed in to BandChat is signed out",
    );
    expect(confirm.textContent).toContain("has to sign in again");
    expect(confirm.textContent).toContain("changes they haven't synced yet may be lost");
    expect(fetchMock).not.toHaveBeenCalled();
    expect(fake.handle.devices.remove).not.toHaveBeenCalled();
  });

  it("ends the other sign-ins on the server first, then removes that device only", async () => {
    const fake = threeDevices();
    const order: string[] = [];
    fetchMock.mockImplementation(async (url) => {
      order.push(`server:${String(url)}`);
      return new Response(JSON.stringify({ ended: 2 }), { status: 200 });
    });
    fake.handle.devices.remove.mockImplementation(async (id: string) => {
      order.push(`remove:${id}`);
      fake.state.devices = fake.state.devices.filter((device) => device.id !== id);
    });
    show(fake);
    await confirmRemoval("device-lost");
    await waitFor(() =>
      expect(document.querySelector('[data-device-id="device-lost"]')).toBeNull(),
    );
    expect(order).toEqual(["server:/api/end-other-sign-ins", "remove:device-lost"]);
    expect(fetchMock.mock.calls[0]![1]).toMatchObject({ method: "POST" });
  });

  it("removes nothing when the server step fails, and says so with a retry", async () => {
    const fake = threeDevices();
    fetchMock.mockResolvedValueOnce(new Response("Forbidden", { status: 403 }));
    show(fake);
    await confirmRemoval("device-lost");
    await screen.findByText(/Your other browsers were not signed out, so nothing was removed/);
    expect(fake.handle.devices.remove).not.toHaveBeenCalled();
    await confirmRemoval("device-lost");
    await waitFor(() => expect(fake.handle.devices.remove).toHaveBeenCalledWith("device-lost"));
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("runs both steps again after an interrupted removal", async () => {
    const fake = threeDevices();
    fake.handle.devices.remove.mockRejectedValueOnce(encryptionError("database", "offline"));
    show(fake);
    await confirmRemoval("device-lost");
    await screen.findByText(/Your other browsers were signed out, but the device was not removed/);
    // Still listed, and the same button runs the server step again.
    await confirmRemoval("device-lost");
    await waitFor(() => expect(fake.handle.devices.remove).toHaveBeenCalledTimes(2));
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});

describe("approving a new browser", () => {
  it("says a wrong code approved nothing, and approves with the right one", async () => {
    const fake = threeDevices();
    show(fake);
    fireEvent.click(
      await within(await findRow("device-new")).findByRole("button", { name: "Approve" }),
    );
    const input = await screen.findByLabelText(/Code shown on the new browser/);
    fireEvent.change(input, { target: { value: "000000" } });
    fireEvent.click(screen.getByRole("button", { name: "Approve this code" }));
    await screen.findByText(/That code doesn't match. Nothing was approved/);
    expect(fake.state.devices.find((device) => device.id === "device-new")?.status).toBe("waiting");
    fireEvent.change(screen.getByLabelText(/Code shown on the new browser/), {
      target: { value: "123 456" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Approve this code" }));
    await waitFor(() => expect(within(row("device-new")).queryByText("Active")).not.toBeNull());
    expect(fake.calls).toContain("approve:123456");
  });

  it("can start the approval again, as after the new browser reloaded", async () => {
    const fake = threeDevices();
    show(fake);
    fireEvent.click(
      await within(await findRow("device-new")).findByRole("button", { name: "Approve" }),
    );
    await screen.findByLabelText(/Code shown on the new browser/);
    fireEvent.click(screen.getByRole("button", { name: "Start again" }));
    await waitFor(() =>
      expect(fake.calls.filter((call) => call === "challenge:device-new")).toHaveLength(2),
    );
  });
});

describe("a browser waiting for approval", () => {
  for (const status of ["awaiting-approval", "reenrolled"] as const) {
    it(`shows it is waiting (${status}) and offers every way forward`, async () => {
      show(fakeEncryption({ status }));
      await screen.findAllByText("This browser is waiting for approval");
      expect(screen.getAllByRole("button", { name: "Show approval code" }).length).toBeGreaterThan(
        0,
      );
      expect(screen.getByLabelText("Recovery phrase")).not.toBeNull();
      expect(screen.getByRole("button", { name: "Restore with passkey" })).not.toBeNull();
    });
  }

  it("answers a challenge and shows the code to type on the other device", async () => {
    const fake = fakeEncryption({ status: "awaiting-approval" });
    show(fake);
    await screen.findAllByText("This browser is waiting for approval");
    fake.challenge();
    expect((await screen.findAllByText("654321")).length).toBeGreaterThan(0);
  });

  it("after a reload that came between answering and approval, says what to do", async () => {
    const fake = fakeEncryption({ status: "awaiting-approval" });
    fake.handle.devices.answer.mockRejectedValueOnce(encryptionError("approval-answered"));
    show(fake);
    await screen.findAllByText("This browser is waiting for approval");
    fake.challenge();
    expect(
      (await screen.findAllByText(/already answered that approval, perhaps before a reload/))
        .length,
    ).toBeGreaterThan(0);
    // The other device started again: the code can be shown afresh.
    fireEvent.click(screen.getAllByRole("button", { name: "Show approval code" })[0]!);
    expect((await screen.findAllByText("654321")).length).toBeGreaterThan(0);
  });

  it("restores with the recovery phrase, which leaves the page as soon as it is used", async () => {
    const fake = fakeEncryption({ status: "awaiting-approval" });
    show(fake);
    const phrase = "abandon ability able about above absent absorb abstract";
    fireEvent.change(await screen.findByLabelText("Recovery phrase"), {
      target: { value: phrase },
    });
    fireEvent.click(screen.getByRole("button", { name: "Restore with phrase" }));
    await waitFor(() => expect(fake.handle.recovery.restore).toHaveBeenCalledWith(phrase));
    await waitFor(() =>
      expect(screen.queryAllByText("This browser is waiting for approval")).toHaveLength(0),
    );
    expect(document.body.innerHTML).not.toContain("abandon ability");
  });

  it("restores with a passkey", async () => {
    const fake = fakeEncryption({ status: "awaiting-approval" });
    show(fake);
    fireEvent.click(await screen.findByRole("button", { name: "Restore with passkey" }));
    await waitFor(() =>
      expect(fake.handle.recovery.restoreFromPasskey).toHaveBeenCalledWith({ appName: "BandChat" }),
    );
    await screen.findByText("Your devices");
  });

  it("says when a phrase does not belong to the account", async () => {
    const fake = fakeEncryption({ status: "awaiting-approval" });
    fake.handle.recovery.restore.mockRejectedValueOnce(encryptionError("recovery-root-mismatch"));
    show(fake);
    fireEvent.change(await screen.findByLabelText("Recovery phrase"), {
      target: { value: "x y z" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Restore with phrase" }));
    await screen.findByText(/doesn't belong to this account/);
    // The phrase is not kept in the field after it was used.
    expect((screen.getByLabelText("Recovery phrase") as HTMLTextAreaElement).value).toBe("");
  });
});

describe("a key check that gave up", () => {
  it("is shown with a way to check again", async () => {
    const fake = fakeEncryption();
    show(fake);
    await screen.findByText("Your devices");
    act(() =>
      fake.report({ type: "ready-failed", code: "protocol", message: "refused by the server" }),
    );
    expect(
      (await screen.findAllByText("This browser could not check its keys")).length,
    ).toBeGreaterThan(0);
    fireEvent.click(screen.getAllByRole("button", { name: "Check again" })[0]!);
    await waitFor(() =>
      expect(screen.queryAllByText("This browser could not check its keys")).toHaveLength(0),
    );
  });
});

describe("recovery set-up", () => {
  it("with a passkey: says so and shows no phrase", async () => {
    const fake = fakeEncryption();
    show(fake);
    fireEvent.click(await screen.findByRole("button", { name: "Set up recovery" }));
    await screen.findByText(/Your recovery key is saved in a passkey/);
    expect(fake.handle.recovery.enable).toHaveBeenCalledWith({
      appName: "BandChat",
      accountName: "Alice",
    });
    expect(document.querySelector("[data-recovery-phrase]")).toBeNull();
  });

  it("with the passkey declined: shows the phrase once, keeps it nowhere and sends it nowhere", async () => {
    const fake = fakeEncryption();
    const phrase = Array.from({ length: 24 }, (_, i) => `word${i}`).join(" ");
    fake.handle.recovery.enable.mockResolvedValueOnce({ method: "phrase", phrase });
    const setItem = vi.spyOn(Storage.prototype, "setItem");
    show(fake);
    fireEvent.click(await screen.findByRole("button", { name: "Set up recovery" }));
    expect((await screen.findByText(phrase)).hasAttribute("data-recovery-phrase")).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "I've written it down" }));
    expect(document.body.innerHTML).not.toContain("word0 word1");
    expect(screen.getByText("Recovery is set up for your account.")).not.toBeNull();
    for (const call of setItem.mock.calls) expect(String(call[1])).not.toContain("word0");
    for (const storage of [localStorage, sessionStorage])
      for (let i = 0; i < storage.length; i++)
        expect(storage.getItem(storage.key(i)!)).not.toContain("word0");
    for (const call of fetchMock.mock.calls) expect(JSON.stringify(call)).not.toContain("word0");
    setItem.mockRestore();
  });
});

describe("leaving this browser", () => {
  it("is a button on this browser's row only", async () => {
    show(threeDevices());
    await screen.findByText("This browser");
    expect(
      within(row("device-this")).queryByRole("button", { name: "Leave this browser" }),
    ).not.toBeNull();
    expect(
      within(row("device-lost")).queryByRole("button", { name: "Leave this browser" }),
    ).toBeNull();
  });

  it("asks for removal after a confirmation, records the wish, and shows removal pending", async () => {
    const fake = threeDevices();
    show(fake);
    fireEvent.click(
      await within(await findRow("device-this")).findByRole("button", {
        name: "Leave this browser",
      }),
    );
    expect(fake.handle.devices.requestRemoval).not.toHaveBeenCalled();
    const confirm = screen.getByRole("alertdialog", { name: "Leave this browser?" });
    fireEvent.click(within(confirm).getByRole("button", { name: "Leave this browser" }));
    await waitFor(() => expect(fake.handle.devices.requestRemoval).toHaveBeenCalledTimes(1));
    expect(readLeaveRecord()).toEqual({ accountId: ACCOUNT, deviceId: "device-this" });
    await screen.findAllByText("Removal pending");
    expect(
      within(row("device-this")).queryByRole("button", { name: "Leave this browser" }),
    ).toBeNull();
  });

  it("is refused on a sole device, which is told so and keeps its keys", async () => {
    const fake = fakeEncryption();
    fake.handle.devices.requestRemoval.mockRejectedValueOnce(encryptionError("no-other-device"));
    show(fake);
    fireEvent.click(
      await within(await findRow("device-this")).findByRole("button", {
        name: "Leave this browser",
      }),
    );
    fireEvent.click(
      within(screen.getByRole("alertdialog")).getByRole("button", { name: "Leave this browser" }),
    );
    await screen.findByText(/This is your only device, so it can't leave/);
    expect(readLeaveRecord()).toBeNull();
    expect(screen.queryByText("Removal pending")).toBeNull();
  });

  it("shows removal pending after a reload while the device ID is unchanged", async () => {
    writeLeaveRecord({ accountId: ACCOUNT, deviceId: "device-this" });
    show(threeDevices());
    await screen.findAllByText("Removal pending");
    expect(
      screen.getByText(/Waiting for another of your devices to remove this browser/),
    ).not.toBeNull();
  });
});

async function findRow(deviceId: string): Promise<HTMLElement> {
  await waitFor(() => row(deviceId));
  return row(deviceId);
}

async function confirmRemoval(deviceId: string) {
  fireEvent.click(
    await within(await findRow(deviceId)).findByRole("button", { name: "Remove lost device" }),
  );
  fireEvent.click(
    within(screen.getByRole("alertdialog", { name: "Remove this lost device?" })).getByRole(
      "button",
      {
        name: "Sign out other browsers and remove",
      },
    ),
  );
}
