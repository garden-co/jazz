import { app } from "./apps/session-tab/jazz.generated.js";
import permissions from "./apps/session-tab/jazz.permissions.json";

/** The app the session tab tests run (apps/session-tab): one table every user may read and write. */
export const sessionTabApp = app;

/** Its jazz.permissions.json, as `startJazzApp` takes it. */
export const sessionTabPermissions = permissions;
