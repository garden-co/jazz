import { createUseLocalFirstAuth } from "../react-core/use-local-first-auth.js";
import { browserAuthSecretStore } from "../runtime/auth-secret-store.js";

export const useLocalFirstAuth = createUseLocalFirstAuth(browserAuthSecretStore);
export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
