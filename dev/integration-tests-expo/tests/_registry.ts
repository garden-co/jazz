import type { Suite } from "../runner/harness";
import bulkWrites from "./bulk-writes.suite";
import crud from "./crud.suite";
import queries from "./queries.suite";
import subscriptions from "./subscriptions.suite";
import relations from "./relations.suite";
import durability from "./durability.suite";

// Explicit, ordered list of suites. Add a new file + one line here to grow the
// suite. Order is the order tests render and run on screen.
export const suites: Suite[] = [bulkWrites, crud, queries, subscriptions, relations, durability];
