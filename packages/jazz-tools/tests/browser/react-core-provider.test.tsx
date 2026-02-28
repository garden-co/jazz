import * as React from "react";
import { afterEach, beforeEach, describe, it } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { JazzProvider, useDb, useSession } from "../../src/react-core/provider.js";
import { useAll, useAllSuspense } from "../../src/react-core/use-all.js";
import {
  makeDeferred,
  type CacheEntryHandle,
  type QueryEntryCallbacks,
  type SubscriptionsOrchestrator,
} from "../../src/subscriptions-orchestrator.js";
import type { Session } from "../../src/runtime/context.js";
import type { QueryBuilder } from "../../src/runtime/db.js";
import type { SubscriptionDelta } from "../../src/runtime/subscription-manager.js";

type Todo = {
  id: string;
  title: string;
};

type ControlledEntry<T extends { id: string }> = CacheEntryHandle<T> & {
  emitFulfilled: (data: T[]) => void;
  emitDelta: (delta: SubscriptionDelta<T>) => void;
  emitError: (error: unknown) => void;
  resolvePending: (data: T[]) => void;
};

class ControlledManager {
  private readonly entries = new Map<string, ControlledEntry<any>>();

  register<T extends { id: string }>(query: QueryBuilder<T>, entry: ControlledEntry<T>): void {
    this.entries.set(query._build(), entry);
  }

  makeQueryKey<T extends { id: string }>(query: QueryBuilder<T>): string {
    return query._build();
  }

  getCacheEntry<T extends { id: string }>(key: string): CacheEntryHandle<T> {
    const entry = this.entries.get(key);
    if (!entry) {
      throw new Error(`Unknown query key "${key}"`);
    }
    return entry as CacheEntryHandle<T>;
  }
}

type TestClient = Awaited<React.ComponentProps<typeof JazzProvider>["client"]>;

type TestClientOptions = {
  db?: unknown;
  manager?: ControlledManager;
  session?: Session | null;
};

const BASE_QUERY: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: {
    todos: {
      columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
    },
  } as any,
  _rowType: {} as Todo,
  _build() {
    return "test-query:todos";
  },
};

let root: Root | null = null;
let container: HTMLDivElement | null = null;

beforeEach(() => {
  container = document.createElement("div");
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(() => {
  if (root) {
    root.unmount();
    root = null;
  }
  if (container) {
    container.remove();
    container = null;
  }
});

describe("react-core provider/hooks browser coverage", () => {
  it("RCB-B01: provider accepts already-resolved client object", async () => {
    const client = makeClient({ db: { name: "resolved-db" } });

    render(
      <JazzProvider client={client}>
        <DbNameView />
      </JazzProvider>,
    );

    await expectText("db-name", "resolved-db");
  });

  it("RCB-B02: provider accepts promised client and suspends until resolution", async () => {
    const deferredClient = defer<TestClient>();

    render(
      <CaptureErrorBoundary>
        <React.Suspense fallback={<div data-testid="provider-fallback">loading-client</div>}>
          <JazzProvider client={deferredClient.promise}>
            <DbNameView />
          </JazzProvider>
        </React.Suspense>
      </CaptureErrorBoundary>,
    );

    await expectText("provider-fallback", "loading-client");

    deferredClient.resolve(makeClient({ db: { name: "promised-db" } }));
    await expectText("db-name", "promised-db");
  });

  it("RCB-B03: suspense fallback is shown while promised client is pending", async () => {
    const deferredClient = defer<TestClient>();

    render(
      <CaptureErrorBoundary>
        <React.Suspense fallback={<div data-testid="provider-fallback">loading-client</div>}>
          <JazzProvider client={deferredClient.promise}>
            <DbNameView />
          </JazzProvider>
        </React.Suspense>
      </CaptureErrorBoundary>,
    );

    await expectText("provider-fallback", "loading-client");

    deferredClient.resolve(makeClient({ db: { name: "ready" } }));
    await expectText("db-name", "ready");
  });

  it("covers promised client rejection through suspense/error boundary path", async () => {
    const deferredClient = defer<TestClient>();

    render(
      <CaptureErrorBoundary>
        <React.Suspense fallback={<div data-testid="provider-fallback">loading-client</div>}>
          <JazzProvider client={deferredClient.promise}>
            <DbNameView />
          </JazzProvider>
        </React.Suspense>
      </CaptureErrorBoundary>,
    );

    await expectText("provider-fallback", "loading-client");

    deferredClient.reject(new Error("client-init-failed"));
    await expectText("error", "client-init-failed");
  });

  it("RCB-B04: useDb returns the client db instance", async () => {
    const dbRef = { name: "identity-db" };
    const client = makeClient({ db: dbRef });

    render(
      <JazzProvider client={client}>
        <DbIdentityView expected={dbRef} />
      </JazzProvider>,
    );

    await expectText("db-identity", "same");
  });

  it("RCB-B05: useSession returns null when session is absent", async () => {
    const client = makeClient({ session: undefined });

    render(
      <JazzProvider client={client}>
        <SessionView />
      </JazzProvider>,
    );

    await expectText("session", "null");
  });

  it("RCB-B06: useSession returns session when present", async () => {
    const session: Session = { user_id: "user-123", claims: { role: "writer" } };
    const client = makeClient({ session });

    render(
      <JazzProvider client={client}>
        <SessionView />
      </JazzProvider>,
    );

    await expectText("session", "user-123");
  });

  it("RCB-B07: useAll returns undefined during pending phase", async () => {
    const manager = new ControlledManager();
    const entry = createEntry<Todo>();
    manager.register(BASE_QUERY, entry);
    const client = makeClient({ manager });

    render(
      <JazzProvider client={client}>
        <UseAllView query={BASE_QUERY} />
      </JazzProvider>,
    );

    await expectText("rows", "undefined");
  });

  it("RCB-B08: useAll transitions to data after first fulfillment", async () => {
    const manager = new ControlledManager();
    const entry = createEntry<Todo>();
    manager.register(BASE_QUERY, entry);
    const client = makeClient({ manager });

    render(
      <JazzProvider client={client}>
        <UseAllView query={BASE_QUERY} />
      </JazzProvider>,
    );

    await expectText("rows", "undefined");

    entry.emitFulfilled([
      { id: "a", title: "Alpha" },
      { id: "b", title: "Beta" },
    ]);

    await expectText("rows", "Alpha|Beta");
  });

  it("RCB-B09: useAllSuspense suspends then renders resolved data", async () => {
    const manager = new ControlledManager();
    const entry = createEntry<Todo>();
    manager.register(BASE_QUERY, entry);
    const client = makeClient({ manager });

    render(
      <CaptureErrorBoundary>
        <React.Suspense fallback={<div data-testid="rows-fallback">loading-rows</div>}>
          <JazzProvider client={client}>
            <UseAllSuspenseView query={BASE_QUERY} />
          </JazzProvider>
        </React.Suspense>
      </CaptureErrorBoundary>,
    );

    await expectText("rows-fallback", "loading-rows");

    entry.resolvePending([
      { id: "a", title: "Alpha" },
      { id: "b", title: "Beta" },
    ]);

    await expectText("rows", "Alpha|Beta");
  });

  it("RCB-B10: delta change stream is reflected in rendered list", async () => {
    const manager = new ControlledManager();
    const entry = createEntry<Todo>({
      status: "fulfilled",
      data: [{ id: "a", title: "Alpha" }],
      error: null,
    });
    manager.register(BASE_QUERY, entry);
    const client = makeClient({ manager });

    render(
      <JazzProvider client={client}>
        <UseAllView query={BASE_QUERY} />
      </JazzProvider>,
    );

    await expectText("rows", "Alpha");

    entry.emitDelta({
      all: [
        { id: "a", title: "Alpha" },
        { id: "b", title: "Beta" },
      ],
      delta: [{ kind: 0, id: "b", index: 1, item: { id: "b", values: [] } as any }],
    });
    await expectText("rows", "Alpha|Beta");

    entry.emitDelta({
      all: [
        { id: "b", title: "Beta*" },
        { id: "a", title: "Alpha" },
      ],
      delta: [{ kind: 2, id: "b", index: 0 }],
    });
    await expectText("rows", "Beta*|Alpha");

    entry.emitDelta({
      all: [{ id: "b", title: "Beta*" }],
      delta: [{ kind: 1, id: "a", index: 1 }],
    });
    await expectText("rows", "Beta*");
  });

  it("RCB-B11: rejected entry state throws through suspense/error boundary path", async () => {
    const manager = new ControlledManager();
    const rejection = new Error("boom");
    const entry = createEntry<Todo>({
      status: "rejected",
      data: undefined,
      error: rejection,
    });
    manager.register(BASE_QUERY, entry);
    const client = makeClient({ manager });

    render(
      <CaptureErrorBoundary>
        <React.Suspense fallback={<div data-testid="rows-fallback">loading-rows</div>}>
          <JazzProvider client={client}>
            <UseAllSuspenseView query={BASE_QUERY} />
          </JazzProvider>
        </React.Suspense>
      </CaptureErrorBoundary>,
    );

    await expectText("error", "boom");
  });

  it("RCB-B12: hook usage outside provider throws expected invariant error", async () => {
    const cases: Array<{ key: string; element: React.ReactNode }> = [
      { key: "useDb", element: <OutsideProviderDbView /> },
      { key: "useSession", element: <OutsideProviderSessionView /> },
      { key: "useAll", element: <OutsideProviderUseAllView /> },
      { key: "useAllSuspense", element: <OutsideProviderUseAllSuspenseView /> },
    ];

    for (const testCase of cases) {
      render(
        <CaptureErrorBoundary key={testCase.key}>
          <React.Suspense fallback={<div data-testid="outside-fallback">loading</div>}>
            {testCase.element}
          </React.Suspense>
        </CaptureErrorBoundary>,
      );

      await expectText("error", "useDb must be used within <JazzProvider>");
    }
  });
});

class CaptureErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { error: Error | null }
> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  render() {
    if (this.state.error) {
      return <div data-testid="error">{this.state.error.message}</div>;
    }
    return this.props.children;
  }
}

function DbNameView() {
  const db = useDb<{ name: string }>();
  return <div data-testid="db-name">{db.name}</div>;
}

function DbIdentityView({ expected }: { expected: unknown }) {
  const db = useDb();
  return <div data-testid="db-identity">{db === expected ? "same" : "different"}</div>;
}

function SessionView() {
  const session = useSession();
  return <div data-testid="session">{session ? session.user_id : "null"}</div>;
}

function UseAllView({ query }: { query: QueryBuilder<Todo> }) {
  const rows = useAll(query);
  return (
    <div data-testid="rows">{rows ? rows.map((row) => row.title).join("|") : "undefined"}</div>
  );
}

function UseAllSuspenseView({ query }: { query: QueryBuilder<Todo> }) {
  const rows = useAllSuspense(query);
  return <div data-testid="rows">{rows.map((row) => row.title).join("|")}</div>;
}

function OutsideProviderDbView() {
  useDb();
  return <div data-testid="outside-db">ok</div>;
}

function OutsideProviderSessionView() {
  useSession();
  return <div data-testid="outside-session">ok</div>;
}

function OutsideProviderUseAllView() {
  useAll(BASE_QUERY);
  return <div data-testid="outside-use-all">ok</div>;
}

function OutsideProviderUseAllSuspenseView() {
  useAllSuspense(BASE_QUERY);
  return <div data-testid="outside-use-all-suspense">ok</div>;
}

function makeClient(opts?: TestClientOptions): TestClient {
  return {
    db: opts?.db ?? { name: "default-db" },
    manager: (opts?.manager ?? new ControlledManager()) as unknown as SubscriptionsOrchestrator,
    session: opts?.session,
    shutdown: async () => {},
  };
}

function createEntry<T extends { id: string }>(
  initialState?:
    | { status: "fulfilled"; data: T[]; error: null }
    | { status: "rejected"; data: undefined; error: unknown },
): ControlledEntry<T> {
  const deferred = makeDeferred<T[]>();
  const listeners = new Set<QueryEntryCallbacks<T>>();
  let state:
    | { status: "pending"; data: undefined; promise: typeof deferred; error: null }
    | { status: "fulfilled"; data: T[]; error: null }
    | { status: "rejected"; data: undefined; error: unknown } = {
    status: "pending",
    data: undefined,
    promise: deferred,
    error: null,
  };

  if (initialState?.status === "fulfilled") {
    state = initialState;
    deferred.resolve(initialState.data);
  } else if (initialState?.status === "rejected") {
    state = initialState;
    deferred.reject(initialState.error);
    void deferred.catch(() => {});
  }

  const entry: ControlledEntry<T> = {
    get state() {
      return state as ControlledEntry<T>["state"];
    },
    get status() {
      return state.status;
    },
    get promise() {
      return deferred;
    },
    get error() {
      return state.status === "rejected" ? state.error : null;
    },
    subscribe(callbacks: QueryEntryCallbacks<T>) {
      listeners.add(callbacks);
      return () => {
        listeners.delete(callbacks);
      };
    },
    emitFulfilled(data: T[]) {
      if (deferred.status === "pending") {
        deferred.resolve(data);
      }
      state = { status: "fulfilled", data: [...data], error: null };
      for (const listener of listeners) {
        listener.onfulfilled?.([...data]);
      }
    },
    emitDelta(delta: SubscriptionDelta<T>) {
      state = { status: "fulfilled", data: [...delta.all], error: null };
      for (const listener of listeners) {
        listener.onDelta?.(delta);
      }
    },
    emitError(error: unknown) {
      if (deferred.status === "pending") {
        deferred.reject(error);
        void deferred.catch(() => {});
      }
      state = { status: "rejected", data: undefined, error };
      for (const listener of listeners) {
        listener.onError?.(error);
      }
    },
    resolvePending(data: T[]) {
      if (deferred.status === "pending") {
        deferred.resolve(data);
      }
      state = { status: "fulfilled", data: [...data], error: null };
    },
  };

  return entry;
}

function render(node: React.ReactNode): void {
  if (!root) throw new Error("render called before root initialization");
  root.render(node);
}

function getByTestId(testId: string): HTMLElement | null {
  if (!container) return null;
  return container.querySelector(`[data-testid="${testId}"]`);
}

async function expectText(testId: string, expected: string): Promise<void> {
  await waitForCondition(
    async () => getByTestId(testId)?.textContent === expected,
    3000,
    `Expected [data-testid="${testId}"] to equal "${expected}"`,
  );
}

async function waitForCondition(
  check: () => boolean | Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`Timeout: ${message}`);
}

function defer<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
