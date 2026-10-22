import { createBrowserTestDb, uniqueDbName } from "./support.js";
import { testComputedColumns } from "../shared/computed-columns.js";

testComputedColumns(() =>
  createBrowserTestDb({
    appId: "computed-columns",
    driver: { type: "persistent", dbName: uniqueDbName("computed-columns") },
  }),
);
