// #region writing-get-db-ts
import { createDb } from "jazz-tools";

const db = await createDb({
  appId: "my-app",
  env: "dev",
  userBranch: "main",
});
// #endregion writing-get-db-ts
