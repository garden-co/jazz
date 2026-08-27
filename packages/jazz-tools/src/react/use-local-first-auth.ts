import { createContext, createElement, useContext, type ReactNode } from "react";
import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import {
  BrowserAuthSecretStore,
  browserAuthSecretStore,
  type BrowserAuthSecretStoreOptions,
} from "../runtime/auth-secret-store.js";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

export type UseLocalFirstAuthOptions = Pick<
  BrowserAuthSecretStoreOptions,
  "key" | "appId" | "profile"
>;

type LocalFirstAuthStoreContextValue = {
  appId: string;
  store: AuthSecretStore;
};

const LocalFirstAuthStoreContext = createContext<LocalFirstAuthStoreContextValue | null>(null);

export function LocalFirstAuthStoreProvider({
  appId,
  store,
  children,
}: {
  appId: string;
  store: AuthSecretStore;
  children: ReactNode;
}) {
  return createElement(LocalFirstAuthStoreContext.Provider, { value: { appId, store } }, children);
}

export function useLocalFirstAuth(options: UseLocalFirstAuthOptions = {}) {
  const inherited = useContext(LocalFirstAuthStoreContext);
  const hasCustomOptions = Object.values(options).some((value) => value !== undefined);
  const selectsInheritedStore =
    inherited !== null &&
    options.appId === inherited.appId &&
    options.key === undefined &&
    options.profile === undefined;
  const store = hasCustomOptions
    ? selectsInheritedStore
      ? inherited.store
      : BrowserAuthSecretStore.getDefault(options)
    : (inherited?.store ?? browserAuthSecretStore);
  return useLocalFirstAuthWithStore(store);
}

export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
