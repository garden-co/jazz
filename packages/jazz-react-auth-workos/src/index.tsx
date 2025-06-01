import { useEffect, useState, useMemo, useContext, createContext, useCallback } from "react";
import {
  JazzProvider,
  JazzProviderProps,
  useAuthSecretStorage,
  useJazzContext,
} from "jazz-react";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  InMemoryKVStore,
  KvStoreContext,
} from "jazz-tools";
import {
  JazzWorkOSAuth,
  WorkOSAuthHook,
  RedirectOptions
} from "jazz-auth-workos";
import { LocalStorageKVStore } from "jazz-browser";

type JazzWorkOSAuthContextType = {
  getAccessToken: (options?: {
    forceRefresh?: boolean;
  }) => Promise<string> 
  switchToOrganization: ({ organizationId, signInOpts, }: {
    organizationId: string;
    signInOpts?: Omit<RedirectOptions, "type" | "organizationId">;
  }) => Promise<void>
  signIn: (opts?: Omit<RedirectOptions, "type">) => Promise<void>;
  signUp: (opts?: Omit<RedirectOptions, "type">) => Promise<void>;
  signOut: (options?: { returnTo?: string, navigate?: true }) => void;
  isReady: boolean;
  role: string | null;
  organizationId: string | null;
  permissions: string[];
};

export const JazzWorkOSAuthContext = createContext<JazzWorkOSAuthContextType | null>(null);

export const useJazzWorkOSAuth = () => {
  const context = useContext(JazzWorkOSAuthContext);
  if (!context) {
    throw new Error("useJazzWorkOSAuth must be used within a JazzWorkOSAuthProvider");
  }
  return context;
};

export const JazzWorkOSAuthProvider = ({
  workos,
  children,
}: {
  workos: WorkOSAuthHook;
  children: React.ReactNode;
}) => {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  const [auth, setAuth] = useState<JazzWorkOSAuth | null>(null);
  const [isReady, setIsReady] = useState<boolean>(false);

  useEffect(() => {
    if ("guest" in context) return;
    if (workos.isLoading) return;
  
    setupKvStore();
    const authMethod = new JazzWorkOSAuth(context.authenticate, authSecretStorage);
    authMethod.initializeAuth(workos).catch(console.error).then(() => {
      setAuth(authMethod);
      setIsReady(true);
    });
  }, [workos.user, workos.isLoading]);

  const value = useMemo(() => {
    if (!auth) return null;
    
    return {
      getAccessToken: (options?: { forceRefresh?: boolean }) => workos.getAccessToken(options),
      switchToOrganization: ({ organizationId, signInOpts, }: {
        organizationId: string;
        signInOpts?: Omit<RedirectOptions, "type" | "organizationId">;
      }) => workos.switchToOrganization({ organizationId, signInOpts }),
      signIn: (opts?: Omit<RedirectOptions, "type">) => workos.signIn(opts),
      signUp: (opts?: Omit<RedirectOptions, "type">) => workos.signUp(opts),
      signOut: (options?: { returnTo?: string, navigate?: true }) => auth.signOut(workos, options),
      isReady: isReady,
      role: workos.role,
      organizationId: workos.organizationId,
      permissions: workos.permissions
    };
  }, [auth, isReady, workos.role, workos.organizationId, workos.permissions]);

  if (!value) return null;

  return (
    <JazzWorkOSAuthContext.Provider value={value}>
      {children}
    </JazzWorkOSAuthContext.Provider>
  );
};


export const JazzProviderWithWorkOS = <
  S extends AccountClass<Account> & CoValueFromRaw<Account> | AnyAccountSchema,
>({
  workos,
  ...props
}: {
  workos: WorkOSAuthHook;
} & JazzProviderProps<S>) => {

  return (
    <JazzProvider {...props}>
      <JazzWorkOSAuthProvider workos={workos}>
        {props.children}
      </JazzWorkOSAuthProvider>
    </JazzProvider>
  );
};

function setupKvStore() {
  KvStoreContext.getInstance().initialize(
    typeof window === "undefined"
      ? new InMemoryKVStore()
      : new LocalStorageKVStore()
  );
}
