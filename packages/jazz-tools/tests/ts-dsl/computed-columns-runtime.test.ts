import { createDb } from "../../src/runtime/default-create-db.js";
import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import { testComputedColumns } from "../shared/computed-columns.js";
import { uniqueDbName } from "./factories.js";

testComputedColumns(async () =>
  createDb({
    ...(await localAccountConfig("computed-columns")),
    driver: { type: "persistent", dbName: uniqueDbName("computed-columns") },
  }),
);
