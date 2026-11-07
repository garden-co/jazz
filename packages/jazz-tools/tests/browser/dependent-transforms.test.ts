import { createBrowserTestDb, uniqueDbName } from "./support.js";
import { testDependentTransforms } from "../shared/dependent-transforms.js";

testDependentTransforms(() =>
  createBrowserTestDb({
    appId: "dependent-transforms",
    driver: { type: "persistent", dbName: uniqueDbName("dependent-transforms") },
  }),
);
