import { createDb } from "jazz-tools";

// #region branch-ts
export async function createBranchDb() {
  return createDb({
    appId: "my-app", // Register for an app ID free at v2.dashboard.jazz.tools
    env: "prod",
    userBranch: "staging",
  });
}
// #endregion branch-ts
