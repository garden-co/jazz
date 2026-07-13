// #region writing-get-db-ts
import { createDb } from "jazz-tools";

const db = await createDb({
  appId: "my-app", // Register for an app ID free at v2.dashboard.jazz.tools
  env: "dev",
  userBranch: "main",
});
// #endregion writing-get-db-ts
