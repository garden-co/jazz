// Every rule. `basis` says who decided it:
//   brief      given with the task
//   proposed   the proposed one-store contract
//   contract   the rules from the native-language API owners
//   pending    not decided: it runs and is reported NOT_COVERED with what was observed, never PASS
import artifacts from "./artifacts.mjs";
import observed from "./observed.mjs";
import persist from "./persist.mjs";
import signout from "./signout.mjs";
import store from "./store.mjs";
import waits from "./waits.mjs";

const COMMITTED = [...artifacts, ...persist, ...store, ...signout, ...waits, ...observed];

/**
 * LA_LOCAL_RULES: a module of rules run beside these, or in place of the committed rule of the same
 * id (a check that isn't covered by the workflow, run on a machine of one's own).
 */
const local = process.env.LA_LOCAL_RULES ? (await import(process.env.LA_LOCAL_RULES)).default : [];
export const RULES = [...COMMITTED.filter((rule) => !local.some(({ id }) => id === rule.id)), ...local];
