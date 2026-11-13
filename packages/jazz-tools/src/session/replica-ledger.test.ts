import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  replicaLedger,
  replicaLifecycle,
  type Locks,
  type ReplicaEntry,
  type ReplicaLifecycle,
} from "./replica-ledger.js";

// The #5250 model test: several tabs of one browser, one per-account store,
// a backend and the device's account store, all simulated; random
// interleavings of open / write / sign out / close / crash / freeze /
// reconcile / backend sign-out, with the invariants checked throughout.

const REGISTRY = "https://app.example/jazz/apps/a/accounts";

/** Web Locks as one browser has them, with a view per tab so a crash releases that tab's locks. */
class LockManager {
  private shared = new Map<string, Map<string, number>>();
  private exclusive = new Map<string, string>();
  private waiters: Array<() => void> = [];
  private wake() {
    const waiters = this.waiters;
    this.waiters = [];
    for (const wake of waiters) wake();
  }
  sharedHolders(name: string) {
    return [...(this.shared.get(name) ?? new Map()).values()].reduce((a, b) => a + b, 0);
  }
  isExclusive(name: string) {
    return this.exclusive.has(name);
  }
  crash(tab: string) {
    for (const holders of this.shared.values()) holders.delete(tab);
    for (const [name, owner] of this.exclusive) if (owner === tab) this.exclusive.delete(name);
    this.wake();
  }
  view(tab: string): Locks {
    return {
      query: async () => ({
        held: [
          ...[...this.shared]
            .filter(([name]) => this.sharedHolders(name) > 0)
            .map(([name]) => ({ name, mode: "shared" })),
          ...[...this.exclusive.keys()].map((name) => ({ name, mode: "exclusive" })),
        ],
      }),
      request: async (name, options, callback) => {
        if (options.mode === "exclusive") {
          if (this.exclusive.has(name) || this.sharedHolders(name) > 0) {
            if (options.ifAvailable) return callback(null);
            throw new Error("the model only asks for exclusive locks ifAvailable");
          }
          this.exclusive.set(name, tab);
          try {
            return await callback({ name });
          } finally {
            if (this.exclusive.get(name) === tab) this.exclusive.delete(name);
            this.wake();
          }
        }
        while (this.exclusive.has(name))
          await new Promise<void>((resolve) => this.waiters.push(resolve));
        const holders = this.shared.get(name) ?? new Map<string, number>();
        this.shared.set(name, holders);
        holders.set(tab, (holders.get(tab) ?? 0) + 1);
        try {
          return await callback({ name });
        } finally {
          const count = holders.get(tab);
          if (count !== undefined) {
            if (count <= 1) holders.delete(tab);
            else holders.set(tab, count - 1);
          }
          this.wake();
        }
      },
    };
  }
}

type Account = {
  id: string;
  kind: ReplicaEntry["kind"];
  identity: { issuer: string; subject: string };
};
const alice: Account = {
  id: "account-alice",
  kind: "backend",
  identity: { issuer: "jazz:backend", subject: "alice" },
};
const guest: Account = {
  id: "account-guest",
  kind: "device",
  identity: { issuer: "urn:jazz:local-first", subject: "guest" },
};

/** One browser: its storage, locks, worker-side stores, backend and account store. */
function browser() {
  const locks = new LockManager();
  const lockName = (account: string) => `jazz-replica-v1:${REGISTRY}:${account}`;
  /** The worker's stores: absent, intact with pending writes, or partly deleted by a crash. */
  const stores = new Map<string, { pending: number; partial?: true; generation: number }>();
  const deletes: Array<{ account: string; generation: number }> = [];
  const violations: string[] = [];
  const trace: string[] = [];
  const warnings: number[] = [];
  const backend = { user: alice.identity as Account["identity"] | null, answers: 0 };
  /** The account store: which device accounts' keys it keeps. */
  const device = { kept: new Set<string>() };
  /** Accounts a tab is signing out right now: their last tab out wipes, key or not. */
  const signingOut = new Set<string>();
  let generation = 0;
  let heldNextDelete = false;
  let crashNextDelete = false;

  type Tab = {
    name: string;
    lifecycle: ReplicaLifecycle;
    open?: { account: Account; release: () => Promise<void> };
    opening?: string;
    gone: boolean;
  };
  const tabs: Tab[] = [];

  const deleteStore = async (account: string) => {
    if (locks.sharedHolders(lockName(account)) > 0)
      violations.push(`I1: deleted ${account} while a tab held it`);
    const store = stores.get(account);
    if (account === guest.id && device.kept.has(guest.id) && !signingOut.has(account) && !store?.partial)
      violations.push("I8: wiped a guest whose key the device keeps");
    if (heldNextDelete) {
      // A closing worker realm, or a tab of another build: nothing deleted.
      heldNextDelete = false;
      return "held" as const;
    }
    if (crashNextDelete) {
      crashNextDelete = false;
      if (store) store.partial = true;
      throw new Error("the tab crashed mid-delete");
    }
    if (store) deletes.push({ account, generation: store.generation });
    stores.delete(account);
    return "deleted" as const;
  };

  const tab = (name: string): Tab => {
    const self: Tab = { name, gone: false } as Tab;
    self.lifecycle = replicaLifecycle({
      registry: REGISTRY,
      ledger: replicaLedger(REGISTRY),
      locks: locks.view(name),
      confirmMs: 0,
      heldHere: (account) =>
        !self.gone && (self.open?.account.id === account || self.opening === account),
      deleteStore,
      backendUser: async () => {
        backend.answers += 1;
        return backend.user;
      },
      deviceKeyKept: async (account) => device.kept.has(account),
    });
    tabs.push(self);
    return self;
  };

  const open = async (t: Tab, account: Account) => {
    if (t.open || t.opening || t.gone) return;
    trace.push(`${t.name} opens ${account.id}`);
    t.opening = account.id;
    try {
      const release = await t.lifecycle.hold({
        account: account.id,
        kind: account.kind,
        identity: account.identity,
      });
      if (t.gone) return;
      const store = stores.get(account.id);
      if (store?.partial) violations.push(`I5: ${t.name} opened ${account.id} half-deleted`);
      if (!store) stores.set(account.id, { pending: 0, generation: ++generation });
      t.open = { account, release };
      if (account.kind === "device") device.kept.add(account.id);
    } finally {
      t.opening = undefined;
    }
  };
  const write = (t: Tab) => {
    if (!t.open || t.gone) return;
    stores.get(t.open.account.id)!.pending += 1;
  };
  const signOut = async (t: Tab) => {
    if (!t.open || t.gone) return;
    const { account, release } = t.open;
    const pending = stores.get(account.id)?.pending ?? 0;
    const othersHold = tabs.some((o) => o !== t && !o.gone && o.open?.account.id === account.id);
    trace.push(`${t.name} signs out ${account.id} (others hold: ${othersHold})`);
    t.open = undefined;
    await release();
    signingOut.add(account.id);
    const warned = vi.mocked(console.warn).mock.calls.length;
    const outcome = await t.lifecycle.signedOut(account.id, pending).finally(() => {
      if (!tabs.some((o) => o !== t && !o.gone && o.open?.account.id === account.id))
        signingOut.delete(account.id);
    });
    trace.push(`${t.name} signed out ${account.id}: ${outcome}`);
    if (outcome === "wiped" && pending > 0) {
      const counted = vi.mocked(console.warn).mock.calls.slice(warned).join(" ");
      if (!counted.includes(`wiped ${pending} write`))
        violations.push(`I7: the warning didn't count ${pending} writes`);
    }
  };
  const close = async (t: Tab) => {
    if (!t.open || t.gone) return;
    const { release } = t.open;
    trace.push(`${t.name} closes ${t.open.account.id}`);
    t.open = undefined;
    await release();
  };
  const crash = (t: Tab) => {
    trace.push(`${t.name} crashes`);
    t.gone = true;
    t.open = undefined;
    locks.crash(t.name);
  };

  return {
    locks,
    stores,
    deletes,
    violations,
    trace,
    warnings,
    backend,
    device,
    tabs,
    tab,
    open,
    write,
    signOut,
    close,
    crash,
    crashNextDelete: () => (crashNextDelete = true),
    heldNextDelete: () => (heldNextDelete = true),
    /** The account store forgets the guest's key (clearOnSignOut, or a link to a user). */
    forgetGuestKey: () => device.kept.delete(guest.id),
    ledger: replicaLedger(REGISTRY),
  };
}

beforeEach(() => {
  const storage = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => void storage.set(key, value),
    removeItem: (key: string) => void storage.delete(key),
    key: (index: number) => [...storage.keys()][index] ?? null,
    get length() {
      return storage.size;
    },
  });
  vi.spyOn(console, "warn").mockImplementation(() => {});
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("the ledger", () => {
  it("keeps account, kind and identity, never claims", () => {
    const ledger = replicaLedger(REGISTRY);
    ledger.record({
      account: "a",
      kind: "backend",
      identity: { issuer: "i", subject: "s", claims: { email: "x@example.com" } } as never,
    });
    const stored = Array.from({ length: localStorage.length }, (_, i) =>
      localStorage.getItem(localStorage.key(i)!),
    ).join("\n");
    expect(stored).toContain('"subject":"s"');
    expect(stored).not.toContain("x@example.com");
    expect(ledger.read()).toEqual([
      { account: "a", kind: "backend", identity: { issuer: "i", subject: "s" } },
    ]);
  });

  it("reads alpha.61's record as backend users, and forgets in both", () => {
    localStorage.setItem(
      `jazz-clear-on-sign-out-v1:${encodeURIComponent(REGISTRY)}`,
      JSON.stringify([{ account: "old", identity: { issuer: "i", subject: "s" } }]),
    );
    const ledger = replicaLedger(REGISTRY);
    expect(ledger.read()).toEqual([
      { account: "old", kind: "backend", identity: { issuer: "i", subject: "s" } },
    ]);
    ledger.forget("old");
    expect(ledger.read()).toEqual([]);
    expect(
      localStorage.getItem(`jazz-clear-on-sign-out-v1:${encodeURIComponent(REGISTRY)}`),
    ).toBeNull();
  });

  it("keeps each account under its own key, so two tabs recording at once keep both", () => {
    const tabA = replicaLedger(REGISTRY);
    const tabB = replicaLedger(REGISTRY);
    tabA.record({ account: "a", kind: "device", identity: { issuer: "i", subject: "a" } });
    tabB.record({ account: "b", kind: "backend", identity: { issuer: "i", subject: "b" } });
    tabA.markWiping("a", true);
    tabB.forget("b");
    expect(replicaLedger(REGISTRY).read()).toEqual([
      { account: "a", kind: "device", identity: { issuer: "i", subject: "a" }, wiping: true },
    ]);
  });

  it("keeps a wipe's mark when the account is recorded again", () => {
    const ledger = replicaLedger(REGISTRY);
    ledger.record({ account: "a", kind: "device", identity: { issuer: "i", subject: "s" } });
    ledger.markWiping("a", true);
    ledger.record({ account: "a", kind: "device", identity: { issuer: "i", subject: "s" } });
    expect(ledger.get("a")?.wiping).toBe(true);
  });
});

describe("the lifecycle, scenario by scenario", () => {
  it("E13: a tab signing out while another holds the store wipes nothing; the last out wipes, counting both", async () => {
    const b = browser();
    const [one, two] = [b.tab("A"), b.tab("B")];
    await b.open(one, alice);
    await b.open(two, alice);
    b.write(one);
    b.write(two);
    await b.signOut(one);
    expect(b.deletes).toEqual([]);
    expect(b.stores.get(alice.id)?.pending).toBe(2);
    expect(b.ledger.get(alice.id)).toBeDefined();
    await b.signOut(two);
    expect(b.deletes).toHaveLength(1);
    expect(vi.mocked(console.warn).mock.calls.join(" ")).toContain("wiped 2 write(s)");
    expect(b.ledger.read()).toEqual([]);
    expect(b.violations).toEqual([]);
  });

  it("E14: both tabs follow a backend sign-out at once: exactly one wipe", async () => {
    const b = browser();
    const [one, two] = [b.tab("A"), b.tab("B")];
    await b.open(one, alice);
    await b.open(two, alice);
    await Promise.all([b.signOut(one), b.signOut(two)]);
    expect(b.deletes).toHaveLength(1);
    expect(b.violations).toEqual([]);
  });

  it("a tab that closes while signed in leaves its store until the backend says the user is out, twice", async () => {
    const b = browser();
    const [one, two] = [b.tab("A"), b.tab("B")];
    await b.open(one, alice);
    await b.open(two, alice);
    b.write(two);
    await b.signOut(one);
    await b.close(two);
    await one.lifecycle.reconcile();
    expect(b.deletes).toEqual([]);
    expect(b.backend.answers).toBe(1);
    b.backend.user = null;
    await one.lifecycle.reconcile();
    expect(b.deletes).toHaveLength(1);
    expect(b.backend.answers).toBe(3);
  });

  it("one 'no one' answer the next contradicts wipes nothing", async () => {
    const b = browser();
    const one = b.tab("A");
    await b.open(one, alice);
    await b.close(one);
    let answer = 0;
    const flaky = replicaLifecycle({
      registry: REGISTRY,
      ledger: replicaLedger(REGISTRY),
      locks: b.locks.view("C"),
      confirmMs: 0,
      heldHere: () => false,
      deleteStore: async () => "deleted",
      backendUser: async () => (answer++ === 0 ? null : alice.identity),
      deviceKeyKept: async () => false,
    });
    await flaky.reconcile();
    expect(b.ledger.get(alice.id)).toBeDefined();
  });

  it("a backend that can't be asked wipes nothing", async () => {
    const b = browser();
    const one = b.tab("A");
    await b.open(one, alice);
    await b.close(one);
    const offline = replicaLifecycle({
      registry: REGISTRY,
      ledger: replicaLedger(REGISTRY),
      locks: b.locks.view("C"),
      heldHere: () => false,
      deleteStore: async () => "deleted",
      backendUser: async () => {
        throw new TypeError("Failed to fetch");
      },
      deviceKeyKept: async () => false,
    });
    await offline.reconcile();
    expect(b.ledger.get(alice.id)).toBeDefined();
  });

  it("an offline sign-out of the last tab still wipes: no backend needed", async () => {
    const b = browser();
    const one = b.tab("A");
    await b.open(one, alice);
    b.write(one);
    b.backend.user = alice.identity;
    await b.signOut(one);
    expect(b.deletes).toHaveLength(1);
    expect(b.backend.answers).toBe(0);
  });

  it("a guest store left behind by a killed tab is kept while the device keeps its key, and wiped once it doesn't", async () => {
    const b = browser();
    const [one, two] = [b.tab("A"), b.tab("B")];
    await b.open(one, guest);
    await b.open(two, guest);
    await b.signOut(one);
    b.crash(two);
    // Signed out, but its key is kept: it can be reopened, so its data stays.
    await one.lifecycle.reconcile();
    expect(b.deletes).toEqual([]);
    b.forgetGuestKey();
    await one.lifecycle.reconcile();
    expect(b.deletes).toHaveLength(1);
    expect(b.violations).toEqual([]);
  });

  it("a guest another user signed in over keeps its data while its key is kept", async () => {
    const b = browser();
    const one = b.tab("A");
    await b.open(one, guest);
    b.write(one);
    await b.close(one);
    await b.open(one, alice);
    await b.tab("C").lifecycle.reconcile();
    expect(b.stores.get(guest.id)?.pending).toBe(1);
    expect(b.violations).toEqual([]);
  });

  it("keeps a crashed delete's mark when the next delete is held, and opens only once it finished", async () => {
    const b = browser();
    const one = b.tab("A");
    await b.open(one, alice);
    b.crashNextDelete();
    await b.signOut(one).catch(() => {});
    // The worker is closing: nothing deleted, and the mark must stay.
    b.heldNextDelete();
    await b.tab("C").lifecycle.reconcile();
    expect(b.ledger.get(alice.id)?.wiping).toBe(true);
    // An open waits for the wipe to finish rather than open half-deleted storage.
    b.heldNextDelete();
    const next = b.tab("D");
    await b.open(next, alice);
    expect(b.violations).toEqual([]);
    expect(b.deletes).toHaveLength(1);
    expect(b.ledger.get(alice.id)?.wiping).toBeUndefined();
  });

  it("a guest whose key is kept keeps its store when its tab closes", async () => {
    const b = browser();
    const one = b.tab("A");
    await b.open(one, guest);
    await b.close(one);
    await b.tab("C").lifecycle.reconcile();
    expect(b.deletes).toEqual([]);
  });

  it("a crash mid-delete is finished before anyone opens the store again", async () => {
    const b = browser();
    const one = b.tab("A");
    await b.open(one, alice);
    b.crashNextDelete();
    await b.signOut(one).catch(() => {});
    expect(b.ledger.get(alice.id)?.wiping).toBe(true);
    const next = b.tab("C");
    await b.open(next, alice);
    expect(b.violations).toEqual([]);
    expect(b.deletes).toHaveLength(1);
    expect(b.stores.get(alice.id)?.pending).toBe(0);
    expect(b.ledger.get(alice.id)?.wiping).toBeUndefined();
  });

  it("a Wiping store is finished by reconcile without asking the backend", async () => {
    const b = browser();
    const one = b.tab("A");
    await b.open(one, alice);
    b.crashNextDelete();
    await b.signOut(one).catch(() => {});
    await b.tab("C").lifecycle.reconcile();
    expect(b.deletes).toHaveLength(1);
    expect(b.backend.answers).toBe(0);
  });

  it("a tab signing back in while another's sign-out wipes gets a fresh store, still recorded", async () => {
    const b = browser();
    const [one, two] = [b.tab("A"), b.tab("B")];
    await b.open(one, alice);
    b.write(one);
    await Promise.all([b.signOut(one), b.open(two, alice)]);
    expect(b.violations).toEqual([]);
    expect(b.ledger.get(alice.id)).toBeDefined();
    expect(b.stores.get(alice.id)).toBeDefined();
  });

  it("a delete another build's worker holds deletes nothing and keeps the entry unmarked", async () => {
    const lifecycle = replicaLifecycle({
      registry: REGISTRY,
      ledger: replicaLedger(REGISTRY),
      locks: new LockManager().view("A"),
      heldHere: () => false,
      deleteStore: async () => "held",
      backendUser: async () => null,
      deviceKeyKept: async () => false,
    });
    replicaLedger(REGISTRY).record({
      account: alice.id,
      kind: "backend",
      identity: alice.identity,
    });
    expect(await lifecycle.signedOut(alice.id, 0)).toBe("held");
    expect(replicaLedger(REGISTRY).get(alice.id)).toEqual({
      account: alice.id,
      kind: "backend",
      identity: alice.identity,
    });
  });
});

/** A small seeded PRNG, so a failing interleaving can be replayed by its seed. */
function rng(seed: number) {
  return () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };
}

describe("the lifecycle, under random interleavings", () => {
  it.each(Array.from({ length: 200 }, (_, seed) => seed))(
    "seed %i keeps every invariant",
    async (seed) => {
      const random = rng(seed + 1);
      const b = browser();
      const pages = [b.tab("A"), b.tab("B"), b.tab("C")];
      const pick = <T>(items: T[]) => items[Math.floor(random() * items.length)]!;
      const step = async () => {
        const page = pick(
          pages.filter((p) => !p.gone).length ? pages.filter((p) => !p.gone) : pages,
        );
        switch (Math.floor(random() * 9)) {
          case 0:
          case 1:
            // An open fails when the wipe it must finish first fails.
            return b.open(page, random() < 0.7 ? alice : guest).catch(() => {});
          case 2:
            return b.write(page);
          case 3:
            return b.signOut(page).catch(() => {});
          case 4:
            return b.close(page);
          case 5:
            if (random() < 0.3) {
              b.crash(page);
              pages[pages.indexOf(page)] = b.tab(`${page.name}'`);
            }
            return;
          case 6:
            b.backend.user = random() < 0.5 ? null : alice.identity;
            return;
          case 7: {
            const roll = random();
            if (roll < 0.1) b.crashNextDelete();
            else if (roll < 0.2) b.heldNextDelete();
            else if (roll < 0.3) b.forgetGuestKey();
            return;
          }
          default:
            return page.lifecycle.reconcile();
        }
      };
      for (let i = 0; i < 40; i++) {
        // Two steps at once, so their awaits interleave.
        await Promise.all([step(), step()]);
        expect(b.violations, b.trace.join("\n")).toEqual([]);
      }
      // I4: at most one successful delete per store generation.
      const generations = b.deletes.map((d) => `${d.account}:${d.generation}`);
      expect(new Set(generations).size).toBe(generations.length);
      // Every tab signs out: once all have, nothing is left (I3), unless a delete crashed.
      for (const page of pages) await b.signOut(page).catch(() => {});
      for (const page of pages) await page.lifecycle.reconcile();
      b.backend.user = null;
      for (const page of pages) await page.lifecycle.reconcile();
      expect(b.violations).toEqual([]);
      // I3: once every tab signed out, the last out wiped (or a reconcile
      // finished a crashed wipe). Only a guest whose key the device keeps,
      // whose tab went away without signing out, keeps its store: it can be
      // reopened. A delete a closing worker held off waits for a next trigger.
      b.forgetGuestKey();
      for (const page of pages) await page.lifecycle.reconcile();
      const kept = (_account: string) => false;
      expect(
        b.ledger.read().filter((entry) => !kept(entry.account)),
        b.trace.join("\n"),
      ).toEqual([]);
      expect([...b.stores.keys()].filter((account) => !kept(account))).toEqual([]);
    },
  );
});
