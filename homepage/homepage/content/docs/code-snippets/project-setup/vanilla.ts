import { MyAppAccount } from "./schema";
import { createVanillaJazzApp } from "./jazz";
import { apiKey } from "./apiKey";
const { me, logOut, authSecretStorage } = await createVanillaJazzApp({
  sync: {
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    when: "always",
  },
  AccountSchema: MyAppAccount,
});
