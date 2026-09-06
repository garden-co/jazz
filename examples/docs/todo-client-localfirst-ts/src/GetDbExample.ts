// #region writing-get-db-ts
import { createAccountManager, createDb } from "jazz-tools";

const config = { appId: "my-app", serverUrl: "https://core.example", env: "dev" };
const accounts = await createAccountManager(config);
const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();
const db = await createDb({ ...config, account });
// #endregion writing-get-db-ts

void db;
