import type { JazzClientConfig } from "jazz-tools/vue";

/** Tests may name an app id (taken as the client's `scope`) and pass session options. */
export type AppConfig = Omit<Partial<JazzClientConfig>, "appId"> & { appId?: string };
