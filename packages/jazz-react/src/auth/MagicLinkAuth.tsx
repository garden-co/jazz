import {
  type UseMagicLinkAuthAsConsumerOptions,
  type UseMagicLinkAuthAsProviderOptions,
  useCreateMagicLinkAuthAsConsumer,
  useCreateMagicLinkAuthAsProvider,
  useHandleMagicLinkAuthAsConsumer,
  useHandleMagicLinkAuthAsProvider,
} from "jazz-react-core";
import { useRef } from "react";

export function useCreateMagicLinkAuth(
  options: {
    mode: "share-local-credentials";
  } & Partial<UseMagicLinkAuthAsProviderOptions>,
): ReturnType<typeof useCreateMagicLinkAuthAsProvider>;
export function useCreateMagicLinkAuth(
  options: {
    mode: "authenticate-current-device";
  } & Partial<UseMagicLinkAuthAsConsumerOptions>,
): ReturnType<typeof useCreateMagicLinkAuthAsConsumer>;
export function useCreateMagicLinkAuth({
  mode,
  ...options
}:
  | ({
      mode: "share-local-credentials";
    } & Partial<UseMagicLinkAuthAsProviderOptions>)
  | ({
      mode: "authenticate-current-device";
    } & Partial<UseMagicLinkAuthAsConsumerOptions>)) {
  const initialMode = useRef(mode);

  if (initialMode.current !== mode) {
    console.warn("useCreateMagicLinkAuth mode cannot be changed once mounted.");
  }

  if (initialMode.current === "share-local-credentials") {
    return useCreateMagicLinkAuthAsProvider(window.location.origin, options);
  } else {
    return useCreateMagicLinkAuthAsConsumer(window.location.origin, options);
  }
}

export function useHandleMagicLinkAuth(
  options: {
    mode: "share-local-credentials";
  } & Partial<UseMagicLinkAuthAsProviderOptions>,
): ReturnType<typeof useHandleMagicLinkAuthAsProvider>;
export function useHandleMagicLinkAuth(
  options: {
    mode: "authenticate-current-device";
  } & Partial<UseMagicLinkAuthAsConsumerOptions>,
): ReturnType<typeof useHandleMagicLinkAuthAsConsumer>;
export function useHandleMagicLinkAuth({
  mode,
  ...options
}:
  | ({
      mode: "share-local-credentials";
    } & Partial<UseMagicLinkAuthAsProviderOptions>)
  | ({
      mode: "authenticate-current-device";
    } & Partial<UseMagicLinkAuthAsConsumerOptions>)) {
  const initialMode = useRef(mode);

  if (initialMode.current !== mode) {
    console.warn("useHandleMagicLinkAuth mode cannot be changed once mounted.");
  }

  if (initialMode.current === "share-local-credentials") {
    return useHandleMagicLinkAuthAsProvider(
      window.location.origin,
      window.location.href,
      options,
    );
  } else {
    return useHandleMagicLinkAuthAsConsumer(
      window.location.origin,
      window.location.href,
      options,
    );
  }
}
