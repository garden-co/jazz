import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
import { expoAuthSecretStore } from "./auth-secret-store.js";

export function useLocalFirstAuth(): LocalFirstAuth {
  return useLocalFirstAuthWithStore(expoAuthSecretStore);
}

export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
