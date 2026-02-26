import type { Db } from '../runtime/db.js';
import type { Session } from '../runtime/context.js';
export interface JazzContext {
    db: Db | null;
    session: Session | null;
}
export declare function initJazzContext(): JazzContext;
export declare function getJazzContext(): JazzContext;
export declare function getDb(): Db;
export declare function getSession(): Session | null;
//# sourceMappingURL=context.svelte.d.ts.map