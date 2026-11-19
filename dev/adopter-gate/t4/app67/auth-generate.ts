import { betterAuth } from "better-auth";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { authOptions } from "./auth-options";

export const auth = betterAuth({
  ...authOptions,
  database: jazzAdapter({
    db: async () => {
      throw new Error("The generation config cannot query the database");
    },
    schema: {},
  }),
});
