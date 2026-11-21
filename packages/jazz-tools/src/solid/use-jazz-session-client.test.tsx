import { render } from "solid-js/web";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SolidJazzClient } from "./create-solid-jazz-client.js";
import { JazzClientProvider, useSession } from "./provider.js";
import { useJazzSession, type UseJazzSession } from "./session.js";
import { SessionContext } from "./session-context.js";

vi.mock("../runtime/db.js", () => ({}));
vi.mock("../session/create-jazz-session.js", () => ({ openBrowserSessionCore: vi.fn() }));
vi.mock("../web/create-jazz-client.js", () => ({ openJazzClient: vi.fn() }));

function makeClient(): SolidJazzClient {
  return {
    db: {} as never,
    session: {
      user: "00000000-0000-4000-8000-000000000002",
      identity: { issuer: "urn:jazz:local-first", subject: "device-1" },
      claims: {},
      authMode: "device-account",
    },
    loading: false,
    error: undefined,
    state: "ready",
    authState: null,
    shutdown: vi.fn(async () => undefined),
  };
}

describe("solid/useJazzSession under <JazzClientProvider client>", () => {
  let container: HTMLDivElement;
  beforeEach(() => {
    container = document.createElement("div");
    document.body.append(container);
  });
  afterEach(() => container.remove());

  it("is ready with the client's user, and refuses session operations", async () => {
    const client = makeClient();
    let session!: UseJazzSession;
    const dispose = render(
      () => (
        <JazzClientProvider client={client}>
          {(() => {
            session = useJazzSession();
            return null;
          })()}
        </JazzClientProvider>
      ),
      container,
    );
    expect(session.snapshot().status).toBe("ready");
    expect(session.snapshot().client).toBe(client);
    expect(session.snapshot().user).toEqual({
      accountId: "00000000-0000-4000-8000-000000000002",
      kind: "device",
      identity: { issuer: "urn:jazz:local-first", subject: "device-1" },
      claims: {},
    });
    await expect(session.signOut()).rejects.toThrow(/outside a Jazz session/);
    dispose();
  });

  it("reads its own client, not an enclosing <JazzProvider>'s session", async () => {
    const outerSignOut = vi.fn(async () => {});
    const outer = {
      session: { signOut: outerSignOut } as never,
      snapshot: () => ({ status: "ready", user: null, localFirst: false }) as never,
    };
    const client = makeClient();
    let session!: UseJazzSession;
    const dispose = render(
      () => (
        <SessionContext.Provider value={outer}>
          <JazzClientProvider client={client}>
            {(() => {
              session = useJazzSession();
              return null;
            })()}
          </JazzClientProvider>
        </SessionContext.Provider>
      ),
      container,
    );
    expect(session.snapshot().client).toBe(client);
    expect(session.snapshot().user?.kind).toBe("device");
    await expect(session.signOut()).rejects.toThrow(/outside a Jazz session/);
    expect(outerSignOut).not.toHaveBeenCalled();
    dispose();
  });

  it("useSession() still works and warns once, naming useJazzSession", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const client = makeClient();
    let user: string | null | undefined;
    const dispose = render(
      () => (
        <JazzClientProvider client={client}>
          {(() => {
            user = useSession()()?.user;
            useSession();
            return null;
          })()}
        </JazzClientProvider>
      ),
      container,
    );
    expect(user).toBe("00000000-0000-4000-8000-000000000002");
    const calls = warn.mock.calls.filter((call) => String(call[0]).includes("useSession()"));
    expect(calls).toHaveLength(1);
    expect(String(calls[0]![0])).toContain("useJazzSession().snapshot().user");
    warn.mockRestore();
    dispose();
  });
});
