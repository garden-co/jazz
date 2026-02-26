import { type DbConfig } from '../runtime/db.js';
import type { Db } from '../runtime/db.js';
interface Props {
    config: DbConfig;
    children: import('svelte').Snippet<[{
        db: Db;
    }]>;
    fallback?: import('svelte').Snippet;
}
declare const JazzSvelteProvider: import("svelte").Component<Props, {}, "">;
type JazzSvelteProvider = ReturnType<typeof JazzSvelteProvider>;
export default JazzSvelteProvider;
//# sourceMappingURL=JazzSvelteProvider.svelte.d.ts.map