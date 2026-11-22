// A recording stand-in for the generated `app` and for `jazz-tools/react`, so
// component tests can say what each query returns and see what each screen
// asked for and wrote, with no runtime behind them.
import { vi } from "vitest";

export interface Q {
  table: string;
  filter?: Record<string, unknown>;
  columns?: string[];
  order?: [string, string];
  max?: number;
}

function builder(q: Q): Q & Record<string, unknown> {
  return {
    ...q,
    where: (filter: Record<string, unknown>) => builder({ ...q, filter }),
    select: (...columns: string[]) => builder({ ...q, columns }),
    orderBy: (column: string, direction: string) => builder({ ...q, order: [column, direction] }),
    limit: (max: number) => builder({ ...q, max }),
  };
}

/** Stands in for the generated `app`: `app.messages.where(...)` is a recorded `Q`. */
export const fakeApp = new Proxy({} as Record<string, Q>, {
  get: (_target, table: string) => builder({ table }),
});

type Answer = { data: unknown[]; unreadable?: unknown[] } | undefined;

export const world = {
  /** What a query returns; undefined = still loading. */
  answer: (_q: Q): Answer => undefined,
  /** Every query a component opened, in order. */
  asked: [] as Q[],
  account: "alice-account",
};

export function resetWorld() {
  world.answer = () => undefined;
  world.asked = [];
  world.account = "alice-account";
  for (const fn of Object.values(db) as unknown[]) if (vi.isMockFunction(fn)) fn.mockClear();
  db.encryption.keys.deliver.mockReset().mockResolvedValue({ delivered: 1, failed: 0 });
  db.encryption.keys.rotate.mockReset().mockResolvedValue(1);
  db.encryption.members.where.mockReset().mockImplementation(async (space, accountId) => ({
    scopeId: "scope",
    identifier: space.id,
    ...(accountId ? { accountId } : {}),
  }));
  db.encryption.members.row.mockReset().mockImplementation(async (space, accountId) => ({
    scopeId: "scope",
    identifier: space.id,
    accountId,
  }));
}

const written = () => ({ wait: vi.fn(async () => undefined) });

export const db = {
  insert: vi.fn((..._args: unknown[]) => written()),
  update: vi.fn((..._args: unknown[]) => written()),
  delete: vi.fn((..._args: unknown[]) => written()),
  transaction: vi.fn(async (_run: (tx: unknown) => unknown) => written()),
  serializableTransaction: vi.fn(async (_run: (tx: unknown) => unknown) => written()),
  beginTransaction: vi.fn(),
  onMutationError: vi.fn(() => () => undefined),
  encryption: {
    members: {
      where: vi.fn(async (space: { id: string }, accountId?: string) => ({
        scopeId: "scope",
        identifier: space.id,
        ...(accountId ? { accountId } : {}),
      })),
      row: vi.fn(async (space: { id: string }, accountId: string) => ({
        scopeId: "scope",
        identifier: space.id,
        accountId,
      })),
    },
    keys: {
      deliver: vi.fn(async () => ({ delivered: 1, failed: 0 })),
      rotate: vi.fn(async (_space: unknown) => 1),
    },
  },
};

/** The `jazz-tools/react` module as the screens see it. */
export const reactMock = {
  useDb: () => db,
  useAll: (q?: Q) => {
    if (q) world.asked.push(q);
    const answer = q ? world.answer(q) : undefined;
    return {
      data: answer?.data,
      unreadable: answer?.unreadable ?? [],
      isLoading: !answer,
      error: null,
      db,
    };
  },
  useJazzSession: () => ({ user: { accountId: world.account } }),
  JazzProvider: ({ children }: { children: unknown }) => children,
};

export const sameFilter = (q: Q, filter: Record<string, unknown>) =>
  JSON.stringify(q.filter) === JSON.stringify(filter);
