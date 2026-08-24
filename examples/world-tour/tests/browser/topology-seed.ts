/**
 * Injected by Vite into browser-test modules.  It intentionally lives apart
 * from Node-side global setup constants, which Vitest evaluates without the
 * browser transform.
 */
declare const __JAZZ_EXAMPLE_TOPOLOGY_SEED__: number;

export const TOPOLOGY_SEED = __JAZZ_EXAMPLE_TOPOLOGY_SEED__;
