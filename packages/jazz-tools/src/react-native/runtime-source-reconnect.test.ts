import { describe, expect, it, vi } from "vitest";
import { ReactNativeRuntimeSource } from "./runtime-source.js";

vi.mock("jazz-rn/relay", () => ({ installNativeForegroundRuntime: vi.fn() }));
// The app's bearer, as an admitted account config carries it.
vi.mock("../accounts/config-capability.js", async (importOriginal) => ({
  ...(await importOriginal<object>()),
  accountConfigBearer: (config: { bearer?: () => Promise<string> }) => config.bearer,
}));
vi.mock("../runtime/default-runtime-source.js", () => {
  throw new Error("React Native must not load the WASM runtime");
});

/** A source that admitted a backend user's scope, with a client open on it. */
function admittedSource(authRejected: string | null) {
  const capability = new Uint8Array(32).fill(5);
  const refreshAccountSession = vi.fn();
  const reconnectNativeUpstream = vi.fn();
  const source = new ReactNativeRuntimeSource();
  Object.assign(source, {
    ownsAdmission: true,
    admittedCapability: capability,
    foregroundFactory: { refreshAccountSession },
    lifecycleForeground: {
      nativeConnectionStatus: () => ({ authRejected }),
      reconnectNativeUpstream,
    },
  });
  const bearer = vi.fn(async () => "bearer-renewed");
  const config = { appId: "app", cookieSession: { claims: {} }, bearer } as never;
  const reconnect = () =>
    (
      source as unknown as { reconnectOwnedUpstream(config: never): Promise<void> }
    ).reconnectOwnedUpstream(config);
  return { capability, refreshAccountSession, reconnectNativeUpstream, bearer, reconnect };
}

describe("React Native reconnect of an admitted backend user", () => {
  it("hands a refused worker the app's current bearer before it reconnects", async () => {
    const source = admittedSource("token expired");
    await source.reconnect();
    expect(source.refreshAccountSession).toHaveBeenCalledExactlyOnceWith(
      source.capability,
      JSON.stringify({ bearer: "bearer-renewed", claims: {} }),
    );
    expect(source.reconnectNativeUpstream).toHaveBeenCalledOnce();
  });

  // Asking the app on every reconnect would hold each one, and a disconnect
  // queued behind it, on the app's bearer.
  it("reconnects a worker that was not refused without waiting on the app", async () => {
    const source = admittedSource(null);
    await source.reconnect();
    expect(source.bearer).not.toHaveBeenCalled();
    expect(source.refreshAccountSession).not.toHaveBeenCalled();
    expect(source.reconnectNativeUpstream).toHaveBeenCalledOnce();
  });
});
