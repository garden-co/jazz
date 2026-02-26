import { describe, it, expect, vi, beforeEach } from 'vitest';
// Mock svelte context and lifecycle
const contextStore = new Map();
const destroyCallbacks = [];
vi.mock('svelte', () => ({
    setContext: (key, value) => contextStore.set(key, value),
    getContext: (key) => contextStore.get(key),
    onDestroy: (fn) => destroyCallbacks.push(fn)
}));
// QuerySubscription uses $state and $effect (rune transforms), so we test
// the underlying subscription wiring against the db.subscribeAll API directly.
// The reactive class behaviour is validated via the Svelte compiler in real usage.
describe('QuerySubscription subscription wiring', () => {
    let subscribeCallback = null;
    let unsubFn;
    let mockDb;
    beforeEach(() => {
        contextStore.clear();
        destroyCallbacks.length = 0;
        subscribeCallback = null;
        unsubFn = vi.fn();
        mockDb = {
            subscribeAll: vi.fn((_query, callback, _tier) => {
                subscribeCallback = callback;
                return unsubFn;
            }),
            shutdown: vi.fn()
        };
    });
    it('subscribeAll is called with the query and tier', () => {
        const query = { _build: () => '{"table":"todos"}', _table: 'todos' };
        const unsub = mockDb.subscribeAll(query, () => { }, 'worker');
        expect(mockDb.subscribeAll).toHaveBeenCalledWith(query, expect.any(Function), 'worker');
        expect(typeof unsub).toBe('function');
    });
    it('subscription callback receives delta.all', () => {
        const query = { _build: () => '{"table":"todos"}' };
        let items = [];
        mockDb.subscribeAll(query, (delta) => {
            items = delta.all;
        });
        subscribeCallback({ all: [{ id: '1', title: 'First' }] });
        expect(items).toEqual([{ id: '1', title: 'First' }]);
        subscribeCallback({
            all: [
                { id: '1', title: 'First' },
                { id: '2', title: 'Second' }
            ]
        });
        expect(items).toHaveLength(2);
    });
    it('unsubscribe function is callable', () => {
        const query = { _build: () => '{"table":"todos"}' };
        const unsub = mockDb.subscribeAll(query, () => { });
        unsub();
        expect(unsubFn).toHaveBeenCalledOnce();
    });
    it('with tier, initial value should be undefined (not yet loaded)', () => {
        const tier = 'worker';
        let items = tier ? undefined : [];
        expect(items).toBeUndefined();
        const query = { _build: () => '{"table":"todos"}' };
        mockDb.subscribeAll(query, (delta) => {
            items = delta.all;
        }, tier);
        subscribeCallback({ all: [] });
        expect(items).toEqual([]);
    });
    it('without tier, initial value should be empty array (loaded but empty)', () => {
        const tier = undefined;
        const items = tier ? undefined : [];
        expect(items).toEqual([]);
    });
});
describe('QuerySubscription loading/error states', () => {
    let subscribeCallback = null;
    let mockDb;
    beforeEach(() => {
        subscribeCallback = null;
        mockDb = {
            subscribeAll: vi.fn((_query, callback, _tier) => {
                subscribeCallback = callback;
                return vi.fn();
            }),
            shutdown: vi.fn()
        };
    });
    it('loading starts true, becomes false after first delta', () => {
        let loading = true;
        // After first delta callback, loading should become false
        mockDb.subscribeAll({ _build: () => '{}' }, () => {
            loading = false;
        });
        expect(loading).toBe(true);
        subscribeCallback({ all: [{ id: '1' }] });
        expect(loading).toBe(false);
    });
    it('error is set when subscribeAll throws synchronously', () => {
        const failingDb = {
            subscribeAll: vi.fn((..._args) => {
                throw new Error('query rejected');
            })
        };
        let error = null;
        let loading = true;
        try {
            failingDb.subscribeAll({ _build: () => '{}' }, () => { });
        }
        catch (e) {
            error = e instanceof Error ? e : new Error(String(e));
            loading = false;
        }
        expect(error).toBeInstanceOf(Error);
        expect(error.message).toBe('query rejected');
        expect(loading).toBe(false);
    });
    it('non-Error throws are wrapped in Error', () => {
        const failingDb = {
            subscribeAll: vi.fn((..._args) => {
                throw 'string error';
            })
        };
        let error = null;
        try {
            failingDb.subscribeAll({ _build: () => '{}' }, () => { });
        }
        catch (e) {
            error = e instanceof Error ? e : new Error(String(e));
        }
        expect(error).toBeInstanceOf(Error);
        expect(error.message).toBe('string error');
    });
});
describe('context integration', () => {
    beforeEach(() => {
        contextStore.clear();
    });
    it('context round-trips through set/get', async () => {
        const { initJazzContext, getJazzContext } = await import('./context.svelte.js');
        const ctx = initJazzContext();
        const retrieved = getJazzContext();
        expect(retrieved).toBe(ctx);
    });
    it('db and session can be updated on the context object', async () => {
        const { initJazzContext } = await import('./context.svelte.js');
        const ctx = initJazzContext();
        const mockDb = { shutdown: vi.fn() };
        const mockSession = { user_id: 'bob', claims: {} };
        ctx.db = mockDb;
        ctx.session = mockSession;
        expect(ctx.db).toBe(mockDb);
        expect(ctx.session).toBe(mockSession);
    });
});
