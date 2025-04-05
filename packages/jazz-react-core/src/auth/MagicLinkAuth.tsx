import {
  Account,
  MagicLinkAuth,
  MagicLinkAuthOptions,
  waitForCoValueCondition,
} from "jazz-tools";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useAuthSecretStorage, useJazzContext } from "../hooks.js";

const DEFAULT_EXPIRE_IN_MS = 15 * 60 * 1000;

export function useCreateMagicLinkAuthAsProvider(
  origin: string,
  {
    autoConfirmLogIn = false,
    ...options
  }: Partial<{ autoConfirmLogIn: boolean } & MagicLinkAuthOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const [status, setStatus] = useState<
    | "idle"
    | "waitingForConsumer"
    | "waitingForConfirmLogIn"
    | "confirmedLogIn"
    | "authorized"
    | "expired"
    | "error"
  >("idle");
  const [confirmLogIn, setConfirmLogIn] = useState<null | (() => void)>(null);

  if ("guest" in context) {
    throw new Error("Magic Link Auth is not supported in guest mode");
  }

  const magicLinkAuth = useMemo(() => {
    return new MagicLinkAuth(
      context.node.crypto,
      context.authenticate,
      authSecretStorage,
      origin,
      options,
    );
  }, [origin, options]);

  const createLink = useCallback(
    async (expireInMs?: number) => {
      expireInMs = expireInMs ?? DEFAULT_EXPIRE_IN_MS;

      let transfer = await magicLinkAuth.createTransferAsProvider(
        new Date(Date.now() + expireInMs),
      );

      const url = magicLinkAuth.createLink("consumer", transfer, "writeOnly");

      async function handleFlow() {
        try {
          // Wait for consumer to accept the transfer
          setStatus("waitingForConsumer");

          transfer = await waitForCoValueCondition(
            transfer,
            {},
            (t) => Boolean(t.acceptedBy),
            expireInMs,
          );

          if (!transfer.acceptedBy) throw new Error("Transfer not accepted");

          // Wait for confirmation if needed
          if (!autoConfirmLogIn) {
            setStatus("waitingForConfirmLogIn");
            await new Promise<void>((resolve) =>
              setConfirmLogIn(() => resolve),
            );
            setConfirmLogIn(null);
          }
          setStatus("confirmedLogIn");

          // Check if the transfer has expired
          if (transfer.expiresAt && transfer.expiresAt < new Date()) {
            setStatus("expired");
            return;
          }

          // Reveal the secret to the transfer
          await magicLinkAuth.revealSecretToTransfer(transfer);

          // Wait for the transfer to be authorized and update the status
          await waitForCoValueCondition(
            transfer,
            {},
            (t) => t.status === "authorized",
          );
          setStatus("authorized");
        } catch (error) {
          console.error("Magic Link Auth error", error);
          setStatus("error");
          setConfirmLogIn(null);
        }
      }

      handleFlow();

      return url;
    },
    [magicLinkAuth, autoConfirmLogIn],
  );

  return {
    status,
    createLink,
    confirmLogIn: () => confirmLogIn?.(),
  } as const;
}

export function useCreateMagicLinkAuthAsConsumer(
  origin: string,
  {
    handlerTimeout = DEFAULT_EXPIRE_IN_MS,
    onLoggedIn,
    ...options
  }: Partial<
    { handlerTimeout: number; onLoggedIn: () => void } & MagicLinkAuthOptions
  > = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const [status, setStatus] = useState<
    | "idle"
    | "waitingForProvider"
    | "waitingForConfirmLogIn"
    | "authorized"
    | "error"
  >("idle");

  const magicLinkAuth = useMemo(() => {
    return new MagicLinkAuth(
      context.node.crypto,
      context.authenticate,
      authSecretStorage,
      origin,
      options,
    );
  }, [origin, options]);

  const createLink = useCallback(async () => {
    let transfer = await magicLinkAuth.createTransferAsConsumer();

    const url = magicLinkAuth.createLink("provider", transfer, "writer");

    async function handleFlow() {
      try {
        // Wait for the provider to accept the transfer
        setStatus("waitingForProvider");
        transfer = await waitForCoValueCondition(
          transfer,
          {},
          (t) => Boolean(t.acceptedBy),
          handlerTimeout,
        );
        setStatus("waitingForConfirmLogIn");

        // Wait for provider to confirm and reveal the secret
        transfer = await waitForCoValueCondition(
          transfer,
          {},
          (t) => Boolean(t.secret),
          handlerTimeout,
        );
        if (!transfer.secret) throw new Error("Transfer secret not set");

        // Log in using the transfer secret
        await magicLinkAuth.logInViaTransfer(transfer);
        setStatus("authorized");
        onLoggedIn?.();
      } catch (error) {
        console.error("Magic Link Auth error", error);
        setStatus("error");
      }
    }

    handleFlow();

    return url;
  }, [magicLinkAuth]);

  return {
    status,
    createLink,
  } as const;
}

export function useHandleMagicLinkAuthAsConsumer(
  origin: string,
  url: string,
  {
    confirmLogInTimeout = DEFAULT_EXPIRE_IN_MS,
    onLoggedIn,
    ...options
  }: Partial<
    {
      confirmLogInTimeout: number;
      onLoggedIn: () => void;
    } & MagicLinkAuthOptions
  > = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const hasRunRef = useRef(false);

  const [status, setStatus] = useState<
    "idle" | "waitingForProvider" | "authorized" | "error"
  >("idle");

  const magicLinkAuth = useMemo(() => {
    return new MagicLinkAuth(
      context.node.crypto,
      context.authenticate,
      authSecretStorage,
      origin,
      options,
    );
  }, [origin, options]);

  useEffect(() => {
    if (hasRunRef.current) return;
    hasRunRef.current = true;

    async function handleFlow() {
      try {
        let transfer = await magicLinkAuth.acceptTransferUrl(url, "consumer");

        setStatus("waitingForProvider");
        transfer = await waitForCoValueCondition(
          transfer,
          {},
          (t) => Boolean(t.secret),
          confirmLogInTimeout,
        );
        if (!transfer.secret) throw new Error("Transfer secret not set");

        setStatus("authorized");
        await magicLinkAuth.logInViaTransfer(transfer);
        onLoggedIn?.();
      } catch (error) {
        console.error("Magic Link Auth error", error);
        setStatus("error");
      }
    }

    handleFlow();
  }, [url]);

  return {
    status,
  } as const;
}

export function useHandleMagicLinkAuthAsProvider(
  origin: string,
  url: string,
  {
    autoConfirmLogIn,
    ...options
  }: Partial<{ autoConfirmLogIn: boolean } & MagicLinkAuthOptions> = {},
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const hasRunRef = useRef(false);

  const [status, setStatus] = useState<
    | "idle"
    | "waitingForConfirmLogIn"
    | "confirmedLogIn"
    | "authorized"
    | "expired"
    | "error"
  >("idle");
  const [confirmLogIn, setConfirmLogIn] = useState<null | (() => void)>(null);
  const magicLinkAuth = useMemo(() => {
    return new MagicLinkAuth(
      context.node.crypto,
      context.authenticate,
      authSecretStorage,
      origin,
      options,
    );
  }, [origin, options]);

  useEffect(() => {
    if (hasRunRef.current) return;
    hasRunRef.current = true;

    async function handleFlow() {
      try {
        const transfer = await magicLinkAuth.acceptTransferUrl(url, "provider");

        // Wait for the consumer to confirm the transfer
        if (!autoConfirmLogIn) {
          setStatus("waitingForConfirmLogIn");
          await new Promise<void>((resolve) => setConfirmLogIn(() => resolve));
          setConfirmLogIn(null);
        }
        setStatus("confirmedLogIn");

        // Check whether the transfer has expired
        if (transfer.expiresAt && transfer.expiresAt < new Date()) {
          setStatus("expired");
          return;
        }

        // Reveal the secret to the transfer
        await magicLinkAuth.revealSecretToTransfer(transfer);

        // Wait for the transfer to be authorized and update the status
        await waitForCoValueCondition(
          transfer,
          {},
          (t) => t.status === "authorized",
        );
        setStatus("authorized");
      } catch (error) {
        console.error("Magic Link Auth error", error);
        setStatus("error");
      }
    }

    handleFlow();
  }, [url]);

  return {
    status,
    confirmLogIn: () => confirmLogIn?.(),
  } as const;
}
