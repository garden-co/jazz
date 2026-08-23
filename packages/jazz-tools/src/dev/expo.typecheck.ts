// Typecheck-only port of the deleted origin/main Expo dev plugin tests to the v2 API.
import { withJazz, type ExpoConfigLike } from "./expo.js";
import { withJazzExpo } from "./index.js";

const config: ExpoConfigLike = {
  name: "my-app",
  slug: "my-app",
  extra: {
    existing: true,
  },
};

async function configureExpo() {
  const resolved = await withJazz(config, {
    schemaDir: "schema",
    server: {
      port: 4200,
      adminSecret: "admin",
    },
  });
  const resolvedFromBarrel = await withJazzExpo(config, { server: false });
  resolved satisfies ExpoConfigLike;
  resolvedFromBarrel satisfies ExpoConfigLike;
}

void configureExpo;
