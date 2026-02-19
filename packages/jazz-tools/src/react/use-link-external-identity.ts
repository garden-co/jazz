import { useCallback } from "react";
import type { LocalAuthMode } from "../runtime/context.js";
import {
  linkExternalIdentity,
  type LinkExternalResponse,
} from "../runtime/sync-transport.js";
import {
  getActiveSyntheticAuth,
  type SyntheticUserStorageOptions,
} from "./synthetic-users.js";

export interface LinkExternalIdentityInput {
  jwtToken: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
}

export interface UseLinkExternalIdentityOptions extends SyntheticUserStorageOptions {
  appId: string;
  serverUrl: string;
  serverPathPrefix?: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
  logPrefix?: string;
}

/**
 * React hook that links the active local anonymous/demo identity
 * to an external JWT identity.
 *
 * If local auth fields are not provided in options/input, this hook
 * resolves them from the active synthetic-user profile in localStorage.
 */
export function useLinkExternalIdentity(options: UseLinkExternalIdentityOptions) {
  return useCallback(
    async (input: LinkExternalIdentityInput): Promise<LinkExternalResponse> => {
      let localAuthMode = input.localAuthMode ?? options.localAuthMode;
      let localAuthToken = input.localAuthToken ?? options.localAuthToken;

      if (!localAuthMode || !localAuthToken) {
        const fallbackAuth = getActiveSyntheticAuth(options.appId, {
          storage: options.storage,
          storageKey: options.storageKey,
          defaultMode: options.defaultMode ?? "anonymous",
        });
        localAuthMode ??= fallbackAuth.localAuthMode;
        localAuthToken ??= fallbackAuth.localAuthToken;
      }

      if (!localAuthMode || !localAuthToken) {
        throw new Error("Local auth mode and token are required to link external identity");
      }

      return linkExternalIdentity(
        options.serverUrl,
        {
          jwtToken: input.jwtToken,
          localAuthMode,
          localAuthToken,
          pathPrefix: options.serverPathPrefix,
        },
        options.logPrefix,
      );
    },
    [
      options.appId,
      options.defaultMode,
      options.localAuthMode,
      options.localAuthToken,
      options.logPrefix,
      options.serverPathPrefix,
      options.serverUrl,
      options.storage,
      options.storageKey,
    ],
  );
}
