import { Account, JazzContextManager, type JazzContextType } from "jazz-tools";
import { Accessor, createContext, useContext } from "solid-js";

export type JazzContextValue<Acc extends Account = Account> =
  JazzContextType<Acc>;

export const JazzContext = createContext<Accessor<JazzContextValue>>();

/**
 * Get the current Jazz context.
 * @returns The current Jazz context.
 */
export function useJazzContext<Acc extends Account>() {
  const context = useContext(JazzContext);

  if (!context) {
    throw new Error("useJazzContext must be used within a JazzProvider");
  }

  return context as Accessor<JazzContextValue<Acc>>;
}

export const JazzManagerContext =
  createContext<Accessor<JazzContextManager<Account, {}>>>();

export function useJazzManager<Acc extends Account>() {
  const manager = useContext(JazzManagerContext);

  if (!manager) {
    throw new Error("useJazzManager must be used within a JazzProvider");
  }

  return manager as Accessor<JazzContextManager<Acc, {}>>;
}

/**
 * Get the current Jazz auth secret storage.
 * @returns The current Jazz auth secret storage.
 */
export function useAuthSecretStorage() {
  const manager = useJazzManager();
  const authStorage = () => manager().getAuthSecretStorage();
  return authStorage;
}
