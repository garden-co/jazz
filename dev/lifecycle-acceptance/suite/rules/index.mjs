// Every rule. `basis` says who decided it:
//   brief      given with the task
//   proposed   the proposed one-store contract
//   contract   the rules from the native-language API owners
//   pending    not decided: it runs and is reported NOT_COVERED with what was observed, never PASS
import artifacts from "./artifacts.mjs";
import persist from "./persist.mjs";
import store from "./store.mjs";

export const RULES = [...artifacts, ...persist, ...store];
