import "../permissions.js";
import { createAccountManager, createDb } from "jazz-tools";
import { app } from "../schema.js";

const config = {
  appId: import.meta.env.VITE_JAZZ_APP_ID,
  serverUrl: import.meta.env.VITE_JAZZ_SERVER_URL,
};
const accounts = await createAccountManager(config);
export const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();
export const db = await createDb({ ...config, account, e2ee: { app } });
