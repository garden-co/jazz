import { betterAuth } from "better-auth";
import { jazzPlugin } from "jazz-tools/better-auth/auth/server";

// Your Better Auth server configuration
export const auth = betterAuth({
  // Add the Jazz plugin
  plugins: [
    jazzPlugin(),
    // other server plugins
  ],

  // rest of the Better Auth configuration
  // like database, email/password authentication, social providers, etc.
});

// #region WithHooks
export const authWithHooks = betterAuth({
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
// #endregion
