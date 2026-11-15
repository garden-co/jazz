import { createDb } from "../../src/runtime/default-create-db.js";
import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import { testDependentTransforms } from "../shared/dependent-transforms.js";
import { uniqueDbName } from "./factories.js";

testDependentTransforms(async () =>
  createDb({
    ...(await localAccountConfig("dependent-transforms")),
    driver: { type: "persistent", dbName: uniqueDbName("dependent-transforms") },
  }),
);
