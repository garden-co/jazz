import type { BetterAuthClientPlugin } from "better-auth";
import type { jazzPlugin } from "jazz-betterauth-server-plugin";

type JazzPlugin = typeof jazzPlugin;

export const jazzClientPlugin = () => {
  return {
    id: "jazz-plugin",
    $InferServerPlugin: {} as ReturnType<JazzPlugin>,
  } satisfies BetterAuthClientPlugin;
};
