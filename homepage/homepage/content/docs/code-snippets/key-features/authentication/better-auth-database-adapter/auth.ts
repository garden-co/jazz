import { betterAuth } from "better-auth";
import { JazzBetterAuthDatabaseAdapter } from "jazz-tools/better-auth/database-adapter";
const apiKey = process.env.JAZZ_API_KEY;

const auth = betterAuth({
  database: JazzBetterAuthDatabaseAdapter({
    syncServer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    accountID: "auth-worker-account-id",
    accountSecret: "your-worker-account-secret",
  }),

  // other Better Auth settings
});
