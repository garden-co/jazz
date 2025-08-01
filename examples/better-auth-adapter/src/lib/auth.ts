import { betterAuth } from "better-auth";
import { JazzBetterAuthDatabaseAdapter } from "jazz-better-auth/database-adapter";

export const auth = betterAuth({
  database: JazzBetterAuthDatabaseAdapter({
    syncServer: process.env.SYNC_SERVER!,
    accountID: process.env.WORKER_ACCOUNT_ID!,
    accountSecret: process.env.WORKER_ACCOUNT_SECRET!,
  }),
  emailAndPassword: {
    enabled: true,
  },
});
