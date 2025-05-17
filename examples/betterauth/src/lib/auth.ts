import { betterAuth } from "better-auth";
import { getMigrations } from "better-auth/db";
import Database from "better-sqlite3";
import { jazzPlugin } from "jazz-betterauth-server-plugin";
import { socialProviders } from "./socialProviders";

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
    socialProviders,
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
            console.error("Not implemented");
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
