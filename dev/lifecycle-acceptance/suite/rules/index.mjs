// Every rule. `basis` says who decided it:
//   brief      given with the task
//   proposed   the proposed one-store contract
//   contract   decided by #native-language-apis (relayed 2026-11-22 22:37 UTC): scored
//   pending    not decided, or awaiting Anselm: it runs and is reported NOT_COVERED with what was observed, never PASS
import artifacts from "./artifacts.mjs";
import decided from "./decided.mjs";
import observed from "./observed.mjs";
import perAccount from "./per-account.mjs";
import persist from "./persist.mjs";
import signout from "./signout.mjs";
import store from "./store.mjs";
import waits from "./waits.mjs";

const COMMITTED = [...artifacts, ...persist, ...decided, ...store, ...signout, ...perAccount, ...waits, ...observed];

/**
 * LA_LOCAL_RULES: a module of rules run beside these, or in place of the committed rule of the same
 * id (a check that isn't covered by the workflow, run on a machine of one's own).
 */
const local = process.env.LA_LOCAL_RULES ? (await import(process.env.LA_LOCAL_RULES)).default : [];
export const RULES = [...COMMITTED.filter((rule) => !local.some(({ id }) => id === rule.id)), ...local];
