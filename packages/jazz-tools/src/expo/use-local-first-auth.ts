import { createUseLocalFirstAuth } from "../react-core/use-local-first-auth.js";
import { expoAuthSecretStore } from "./auth-secret-store.js";

export const useLocalFirstAuth = createUseLocalFirstAuth(expoAuthSecretStore);
export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
