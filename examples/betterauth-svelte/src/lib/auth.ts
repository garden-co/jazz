import { betterAuth } from "better-auth";
import { jazzPlugin } from "jazz-tools/better-auth/auth/server";
import { DatabaseSync } from "node:sqlite";

export const auth = await (async () => {
  // Configure Better Auth server
  const auth = betterAuth({
    appName: "Jazz Example: Better Auth: Svelte",
    database: new DatabaseSync("./sqlite.db"),
    emailAndPassword: {
      enabled: true,
    },
    emailVerification: {
      async sendVerificationEmail(data) {
        console.error("Not implemented");
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

  return auth;
})();
