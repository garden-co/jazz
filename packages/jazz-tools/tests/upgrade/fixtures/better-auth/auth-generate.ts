// The Better Auth CLI's generation config, as the alpha.64 adapter docs wrote it.
import { betterAuth } from "better-auth";
import { schema as s } from "jazz-tools";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";

export const auth = betterAuth({
  database: jazzAdapter({
    db: async () => {
      throw new Error("The generation config cannot query the database");
    },
    schema: s.defineApp({}, { migrations: "none" }).wasmSchema,
  }),
});
