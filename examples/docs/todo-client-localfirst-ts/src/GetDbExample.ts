// #region writing-get-db-ts
import { createDb } from "jazz-tools";

const db = await createDb({
  appId: "my-app",
  env: "dev",
});
// #endregion writing-get-db-ts
