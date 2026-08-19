import { createSignal } from "solid-js";
import { render } from "solid-js/web";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { DbConfig } from "../runtime/db.js";
import type { JazzClient } from "../web/create-jazz-client.js";
import { JazzClientProvider, JazzProvider } from "./provider.js";
import type { PendingSolidJazzClient } from "./create-solid-jazz-client.js";

const createJazzClient = vi.hoisted(() => vi.fn<(config: DbConfig) => Promise<JazzClient>>());

vi.mock("../web/create-jazz-client.js", () => ({ createJazzClient }));
vi.mock("../runtime/db.js", () => ({}));

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

function makeClient(id: string, shutdown = vi.fn(async () => undefined)): JazzClient {
  return {
    db: { id } as never,
    session: null,
    shutdown,
  };
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

describe("Solid providers", () => {
  let container: HTMLDivElement;

  beforeEach(() => {
    container = document.createElement("div");
    document.body.append(container);
    createJazzClient.mockReset();
  });

  afterEach(() => {
    container.remove();
  });

  it("JazzClientProvider exposes a caller-owned client without shutting it down", () => {
    const client: PendingSolidJazzClient = {
      db: {} as never,
      session: null,
      authState: null,
      shutdown: vi.fn(async () => undefined),
      loading: false,
      error: undefined,
      state: "ready",
    };

    const dispose = render(
      () => (
        <JazzClientProvider client={client}>
          <p>Ready</p>
        </JazzClientProvider>
      ),
      container,
    );

    expect(container.textContent).toBe("Ready");
    dispose();
    expect(client.shutdown).not.toHaveBeenCalled();
  });

  it("JazzProvider owns client creation, replacement and shutdown", async () => {
    const first = deferred<JazzClient>();
    const second = deferred<JazzClient>();
    createJazzClient.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);

    const [config, setConfig] = createSignal<DbConfig>({ appId: "first" });
    const dispose = render(
      () => (
        <JazzProvider config={config()} fallback={<p>Loading</p>}>
          <p>Ready</p>
        </JazzProvider>
      ),
      container,
    );

    await flushMicrotasks();
    expect(createJazzClient).toHaveBeenCalledWith({ appId: "first" });
    expect(container.textContent).toBe("Loading");

    const firstShutdown = deferred<void>();
    const firstClient = makeClient(
      "first",
      vi.fn(() => firstShutdown.promise),
    );
    first.resolve(firstClient);
    await flushMicrotasks();
    expect(container.textContent).toBe("Ready");

    setConfig({ appId: "second" });
    await flushMicrotasks();
    expect(firstClient.shutdown).toHaveBeenCalledOnce();
    expect(createJazzClient).toHaveBeenCalledOnce();
    expect(container.textContent).toBe("Loading");

    firstShutdown.resolve();
    await flushMicrotasks();
    expect(createJazzClient).toHaveBeenCalledWith({ appId: "second" });
    expect(container.textContent).toBe("Loading");

    const secondClient = makeClient("second");
    second.resolve(secondClient);
    await flushMicrotasks();
    expect(container.textContent).toBe("Ready");

    dispose();
    await flushMicrotasks();
    expect(secondClient.shutdown).toHaveBeenCalledOnce();
  });
});
