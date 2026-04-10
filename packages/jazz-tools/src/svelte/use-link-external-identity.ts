import type { LocalAuthMode } from "../runtime/context.js";
import { linkExternalIdentity, type LinkExternalResponse } from "../runtime/sync-transport.js";
import { getActiveSyntheticAuth, type SyntheticUserStorageOptions } from "../synthetic-users.js";

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
 * Returns a function that links the active local anonymous/demo identity
 * to an external JWT identity.
 *
 * Unlike the React hook, this is a plain function (no memoisation needed
 * in Svelte since there is no render loop to worry about).
 */
export function useLinkExternalIdentity(
  options: UseLinkExternalIdentityOptions,
): (input: LinkExternalIdentityInput) => Promise<LinkExternalResponse> {
  return async (input: LinkExternalIdentityInput): Promise<LinkExternalResponse> => {
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
  };
}
