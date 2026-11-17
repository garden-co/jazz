// Name-blind public fixture for #2363's two forward-reference carriers. The
// app is apps/live-authority-replay (its Node-side backend reads the same files).
export { app as liveAuthorityApp } from "./apps/live-authority-replay/jazz.generated.js";
export { default as liveAuthorityPermissions } from "./apps/live-authority-replay/jazz.permissions.json";

export interface LiveAuthoritySeed {
  parentId: string;
  authorId: string;
  labelId: string;
  itemId: string;
}
