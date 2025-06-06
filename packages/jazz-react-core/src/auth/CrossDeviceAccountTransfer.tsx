import {
  CrossDeviceAccountTransfer,
  CrossDeviceAccountTransferAsSourceOptions,
  CrossDeviceAccountTransferAsTargetOptions,
  CrossDeviceAccountTransferCreateAsSource,
  CrossDeviceAccountTransferCreateAsTarget,
  CrossDeviceAccountTransferHandleAsSource,
  CrossDeviceAccountTransferHandleAsTarget,
  CrossDeviceAccountTransferOptions,
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

export type UseCrossDeviceAccountTransferAsSourceOptions =
  CrossDeviceAccountTransferOptions & CrossDeviceAccountTransferAsSourceOptions;

export type UseCrossDeviceAccountTransferAsTargetOptions =
  CrossDeviceAccountTransferOptions & CrossDeviceAccountTransferAsTargetOptions;

export function useCreateAccountTransferAsSource(
  origin: string,
  {
    expireInMs = DEFAULT_EXPIRE_IN_MS,
    ...options
  }: Partial<UseCrossDeviceAccountTransferAsSourceOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  const crossDeviceAccountTransfer = useMemo(() => {
    return new CrossDeviceAccountTransferCreateAsSource(
      new CrossDeviceAccountTransfer(
        context.node.crypto,
        context.authenticate,
        authSecretStorage,
        origin,
        options,
      ),
    );
  }, [origin]);

  const authState = useSyncExternalStore(
    useCallback(crossDeviceAccountTransfer.subscribe, [
      crossDeviceAccountTransfer,
    ]),
    () => crossDeviceAccountTransfer.authState,
  );

  if ("guest" in context) {
    throw new Error(
      "Cross-Device Account Transfer is not supported in guest mode",
    );
  }

  return {
    ...authState,
    createLink: () => crossDeviceAccountTransfer.createLink(),
    cancelFlow: () => crossDeviceAccountTransfer.cancelFlow(),
  } as const;
}

export function useCreateAccountTransferAsTarget(
  origin: string,
  {
    handlerTimeout = 30 * 1000,
    onLoggedIn,
    ...options
  }: Partial<UseCrossDeviceAccountTransferAsTargetOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  const onLoggedInRef = useRef(onLoggedIn);
  onLoggedInRef.current = onLoggedIn;

  const crossDeviceAccountTransfer = useMemo(() => {
    const onLoggedIn = () => {
      onLoggedInRef.current?.();
    };

    return new CrossDeviceAccountTransferCreateAsTarget(
      new CrossDeviceAccountTransfer(
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
    useCallback(crossDeviceAccountTransfer.subscribe, [
      crossDeviceAccountTransfer,
    ]),
    () => crossDeviceAccountTransfer.authState,
  );

  if ("guest" in context) {
    throw new Error(
      "Cross-Device Account Transfer is not supported in guest mode",
    );
  }

  return {
    ...authState,
    createLink: () => crossDeviceAccountTransfer.createLink(),
    cancelFlow: () => crossDeviceAccountTransfer.cancelFlow(),
  } as const;
}

export function useAcceptAccountTransferAsTarget(
  origin: string,
  url: string,
  {
    handlerTimeout = 30 * 1000,
    onLoggedIn,
    ...options
  }: Partial<UseCrossDeviceAccountTransferAsTargetOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const urlRef = useRef<string | undefined>();

  const onLoggedInRef = useRef(onLoggedIn);
  onLoggedInRef.current = onLoggedIn;

  const crossDeviceAccountTransfer = useMemo(() => {
    const onLoggedIn = () => {
      onLoggedInRef.current?.();
    };

    return new CrossDeviceAccountTransferHandleAsTarget(
      new CrossDeviceAccountTransfer(
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
    useCallback(crossDeviceAccountTransfer.subscribe, [
      crossDeviceAccountTransfer,
    ]),
    () => crossDeviceAccountTransfer.authState,
  );

  useEffect(() => {
    if (!crossDeviceAccountTransfer.checkValidUrl(url)) return;

    if (urlRef.current === url) return;
    urlRef.current = url;

    crossDeviceAccountTransfer.handleFlow(url);

    return () => {
      crossDeviceAccountTransfer.cancelFlow();
    };
  }, [url]);

  return {
    ...authState,
    cancelFlow: () => crossDeviceAccountTransfer.cancelFlow(),
  } as const;
}

export function useAcceptAccountTransferAsSource(
  origin: string,
  url: string,
  {
    expireInMs = DEFAULT_EXPIRE_IN_MS,
    ...options
  }: Partial<UseCrossDeviceAccountTransferAsSourceOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const urlRef = useRef<string | undefined>();

  const crossDeviceAccountTransfer = useMemo(() => {
    return new CrossDeviceAccountTransferHandleAsSource(
      new CrossDeviceAccountTransfer(
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
    useCallback(crossDeviceAccountTransfer.subscribe, [
      crossDeviceAccountTransfer,
    ]),
    () => crossDeviceAccountTransfer.authState,
  );

  useEffect(() => {
    if (!crossDeviceAccountTransfer.checkValidUrl(url)) return;

    if (urlRef.current === url) return;
    urlRef.current = url;

    crossDeviceAccountTransfer.handleFlow(url);

    return () => {
      crossDeviceAccountTransfer.cancelFlow();
    };
  }, [url]);

  return {
    ...authState,
    cancelFlow: () => crossDeviceAccountTransfer.cancelFlow(),
  } as const;
}
