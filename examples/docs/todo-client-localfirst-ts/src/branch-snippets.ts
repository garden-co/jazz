import { createDb } from "jazz-tools";

// #region branch-ts
export async function createBranchDb() {
  return createDb({
    appId: "my-app",
    env: "prod",
    userBranch: "staging",
  });
}
// #endregion branch-ts
