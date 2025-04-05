import {
  useCreateMagicLinkAuthAsConsumer as useCreateMagicLinkAuthAsConsumerCore,
  useCreateMagicLinkAuthAsProvider as useCreateMagicLinkAuthAsProviderCore,
  useHandleMagicLinkAuthAsConsumer as useHandleMagicLinkAuthAsConsumerCore,
  useHandleMagicLinkAuthAsProvider as useHandleMagicLinkAuthAsProviderCore,
} from "jazz-react-core";
import { MagicLinkAuthOptions } from "jazz-tools";

export function useCreateMagicLinkAuthAsProvider(
  options?: Partial<{ autoConfirmLogIn: boolean } & MagicLinkAuthOptions>,
) {
  return useCreateMagicLinkAuthAsProviderCore(window.location.origin, options);
}

export function useCreateMagicLinkAuthAsConsumer(
  options?: Partial<
    { handlerTimeout: number; onLoggedIn: () => void } & MagicLinkAuthOptions
  >,
) {
  return useCreateMagicLinkAuthAsConsumerCore(window.location.origin, options);
}

export function useHandleMagicLinkAuthAsConsumer(
  options?: Partial<{ onLoggedIn: () => void } & MagicLinkAuthOptions>,
) {
  return useHandleMagicLinkAuthAsConsumerCore(
    window.location.origin,
    window.location.href,
    options,
  );
}

export function useHandleMagicLinkAuthAsProvider(
  options?: Partial<{ autoConfirmLogIn: boolean } & MagicLinkAuthOptions>,
) {
  return useHandleMagicLinkAuthAsProviderCore(
    window.location.origin,
    window.location.href,
    options,
  );
}
