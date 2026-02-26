import { getContext, setContext } from 'svelte';
const JAZZ_CTX_KEY = Symbol('jazz');
export function initJazzContext() {
    const ctx = $state({ db: null, session: null });
    setContext(JAZZ_CTX_KEY, ctx);
    return ctx;
}
export function getJazzContext() {
    const ctx = getContext(JAZZ_CTX_KEY);
    if (!ctx) {
        throw new Error('getDb/getSession must be used within <JazzSvelteProvider>');
    }
    return ctx;
}
export function getDb() {
    const ctx = getJazzContext();
    if (!ctx.db) {
        throw new Error('Jazz database is not yet initialised');
    }
    return ctx.db;
}
export function getSession() {
    return getJazzContext().session;
}
