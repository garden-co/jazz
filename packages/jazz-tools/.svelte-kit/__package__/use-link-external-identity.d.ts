import type { LocalAuthMode } from '../runtime/context.js';
import { type LinkExternalResponse } from '../runtime/sync-transport.js';
import { type SyntheticUserStorageOptions } from '../synthetic-users.js';
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
export declare function useLinkExternalIdentity(options: UseLinkExternalIdentityOptions): (input: LinkExternalIdentityInput) => Promise<LinkExternalResponse>;
//# sourceMappingURL=use-link-external-identity.d.ts.map