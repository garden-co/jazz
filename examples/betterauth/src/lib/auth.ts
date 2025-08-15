import { betterAuth } from "better-auth";
import { getMigrations } from "better-auth/db";
import Database from "better-sqlite3";
import { jazzPlugin } from "jazz-tools/better-auth/auth/server";

export const auth = await (async () => {
  // Configure Better Auth server
  const auth = betterAuth({
    appName: "Jazz Example: Better Auth",
    database: new Database("sqlite.db"),
    emailAndPassword: {
      enabled: true,
      async sendResetPassword({ url }) {
        // Here we can send an email to the user with the reset password link
        console.log("****** RESET PASSWORD ******");
        console.log("navigate to", url, "to reset your password");
        console.log("******");
      },
    },
    emailVerification: {
      async sendVerificationEmail(data) {
        console.error("Not implemented");
      },
    },
    socialProviders: {
      github: {
        clientId: process.env.GITHUB_CLIENT_ID!,
        clientSecret: process.env.GITHUB_CLIENT_SECRET!,
      },
    },
    user: {
      deleteUser: {
        enabled: true,
      },
    },
    plugins: [jazzPlugin()],
    databaseHooks: {
      user: {
        create: {
          async after(user) {
            // Here we can send a welcome email to the user
            console.log("User created with Jazz Account ID:", user.accountID);
          },
        },
      },
    },
  });

  // Run database migrations
  const migrations = await getMigrations(auth.options);
  await migrations.runMigrations();

  return auth;
})();
