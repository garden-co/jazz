import { Account } from "jazz-tools";

// #region Basic
import { betterAuthClient } from "./auth-client";
// @ts-expect-error no actual implementation
// Get these from your own implementation
import { getAuthStorage, getJazzContext } from "./jazz-utils";
const jazzContext = getJazzContext();
const authSecretStorage = getAuthStorage();

betterAuthClient.jazz.setJazzContext(jazzContext);
betterAuthClient.jazz.setAuthSecretStorage(authSecretStorage);
// #endregion

// #region SignUp
const me = Account.getMe();
await betterAuthClient.signUp.email(
  {
    email: "email@example.com",
    password: "password",
    name: "John Doe",
  },
  {
    onSuccess: async () => {
      // Don't forget to update the profile's name. It's not done automatically.
      if (me.profile.$isLoaded) {
        me.profile.$jazz.set("name", "John Doe");
      }
    },
  },
);
// #endregion

// #region SignInOut
await betterAuthClient.signIn.email({
  email: "email@example.com",
  password: "password",
});

await betterAuthClient.signOut();
// #endregion
