import { linkExternalIdentity } from '../runtime/sync-transport.js';
import { getActiveSyntheticAuth } from '../synthetic-users.js';
/**
 * Returns a function that links the active local anonymous/demo identity
 * to an external JWT identity.
 *
 * Unlike the React hook, this is a plain function (no memoisation needed
 * in Svelte since there is no render loop to worry about).
 */
export function useLinkExternalIdentity(options) {
    return async (input) => {
        let localAuthMode = input.localAuthMode ?? options.localAuthMode;
        let localAuthToken = input.localAuthToken ?? options.localAuthToken;
        if (!localAuthMode || !localAuthToken) {
            const fallbackAuth = getActiveSyntheticAuth(options.appId, {
                storage: options.storage,
                storageKey: options.storageKey,
                defaultMode: options.defaultMode ?? 'anonymous'
            });
            localAuthMode ??= fallbackAuth.localAuthMode;
            localAuthToken ??= fallbackAuth.localAuthToken;
        }
        if (!localAuthMode || !localAuthToken) {
            throw new Error('Local auth mode and token are required to link external identity');
        }
        return linkExternalIdentity(options.serverUrl, {
            jwtToken: input.jwtToken,
            localAuthMode,
            localAuthToken,
            pathPrefix: options.serverPathPrefix
        }, options.logPrefix);
    };
}
