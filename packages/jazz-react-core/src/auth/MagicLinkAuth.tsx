import {
  MagicLinkAuth,
  MagicLinkAuthConsumerOptions,
  MagicLinkAuthCreateAsConsumer,
  MagicLinkAuthCreateAsProvider,
  MagicLinkAuthHandleAsConsumer,
  MagicLinkAuthHandleAsProvider,
  MagicLinkAuthOptions,
  MagicLinkAuthProviderOptions,
} from "jazz-tools";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useSyncExternalStore,
} from "react";
import { useAuthSecretStorage, useJazzContext } from "../hooks.js";

const DEFAULT_EXPIRE_IN_MS = 15 * 60 * 1000;

export type UseMagicLinkAuthAsProviderOptions = MagicLinkAuthOptions &
  MagicLinkAuthProviderOptions;

export type UseMagicLinkAuthAsConsumerOptions = MagicLinkAuthOptions &
  MagicLinkAuthConsumerOptions;

export function useCreateMagicLinkAuthAsProvider(
  origin: string,
  {
    expireInMs = DEFAULT_EXPIRE_IN_MS,
    ...options
  }: Partial<UseMagicLinkAuthAsProviderOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  const magicLinkAuth = useMemo(() => {
    return new MagicLinkAuthCreateAsProvider(
      new MagicLinkAuth(
        context.node.crypto,
        context.authenticate,
        authSecretStorage,
        origin,
        options,
      ),
    );
  }, [origin]);

  const authState = useSyncExternalStore(
    useCallback(magicLinkAuth.subscribe, [magicLinkAuth]),
    () => magicLinkAuth.authState,
  );

  if ("guest" in context) {
    throw new Error("Magic Link Auth is not supported in guest mode");
  }

  return {
    ...authState,
    createLink: () => magicLinkAuth.createLink(),
    cancelFlow: () => magicLinkAuth.cancelFlow(),
  } as const;
}

export function useCreateMagicLinkAuthAsConsumer(
  origin: string,
  {
    handlerTimeout = 30 * 1000,
    onLoggedIn,
    ...options
  }: Partial<UseMagicLinkAuthAsConsumerOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  const magicLinkAuth = useMemo(() => {
    return new MagicLinkAuthCreateAsConsumer(
      new MagicLinkAuth(
        context.node.crypto,
        context.authenticate,
        authSecretStorage,
        origin,
        options,
      ),
      { handlerTimeout, onLoggedIn },
    );
  }, [origin]);

  const authState = useSyncExternalStore(
    useCallback(magicLinkAuth.subscribe, [magicLinkAuth]),
    () => magicLinkAuth.authState,
  );

  if ("guest" in context) {
    throw new Error("Magic Link Auth is not supported in guest mode");
  }

  return {
    ...authState,
    createLink: () => magicLinkAuth.createLink(),
    cancelFlow: () => magicLinkAuth.cancelFlow(),
  } as const;
}

export function useHandleMagicLinkAuthAsConsumer(
  origin: string,
  url: string,
  {
    handlerTimeout = 30 * 1000,
    onLoggedIn,
    ...options
  }: Partial<UseMagicLinkAuthAsConsumerOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const urlRef = useRef<string | undefined>();

  const magicLinkAuth = useMemo(() => {
    return new MagicLinkAuthHandleAsConsumer(
      new MagicLinkAuth(
        context.node.crypto,
        context.authenticate,
        authSecretStorage,
        origin,
        options,
      ),
      { handlerTimeout, onLoggedIn },
    );
  }, [origin]);

  const authState = useSyncExternalStore(
    useCallback(magicLinkAuth.subscribe, [magicLinkAuth]),
    () => magicLinkAuth.authState,
  );

  useEffect(() => {
    if (!magicLinkAuth.checkValidUrl(url)) return;

    if (urlRef.current === url) return;
    urlRef.current = url;

    magicLinkAuth.handleFlow(url);

    return () => {
      magicLinkAuth.cancelFlow();
    };
  }, [url]);

  return {
    ...authState,
    cancelFlow: () => magicLinkAuth.cancelFlow(),
  } as const;
}

export function useHandleMagicLinkAuthAsProvider(
  origin: string,
  url: string,
  {
    expireInMs = DEFAULT_EXPIRE_IN_MS,
    ...options
  }: Partial<UseMagicLinkAuthAsProviderOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const urlRef = useRef<string | undefined>();

  const magicLinkAuth = useMemo(() => {
    return new MagicLinkAuthHandleAsProvider(
      new MagicLinkAuth(
        context.node.crypto,
        context.authenticate,
        authSecretStorage,
        origin,
        options,
      ),
      { expireInMs },
    );
  }, [origin]);

  const authState = useSyncExternalStore(
    useCallback(magicLinkAuth.subscribe, [magicLinkAuth]),
    () => magicLinkAuth.authState,
  );

  useEffect(() => {
    if (!magicLinkAuth.checkValidUrl(url)) return;

    if (urlRef.current === url) return;
    urlRef.current = url;

    magicLinkAuth.handleFlow(url);

    return () => {
      magicLinkAuth.cancelFlow();
    };
  }, [url]);

  return {
    ...authState,
    cancelFlow: () => magicLinkAuth.cancelFlow(),
  } as const;
}
