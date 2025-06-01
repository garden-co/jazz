import {
    Account,
    AuthCredentials,
    AuthSecretStorage,
    AuthenticateAccountFunction
} from 'jazz-tools';
import { getWorkOSUsername } from './getWorkOSUsername.js';
import {
    WorkOSAuthHook,
    RedirectOptions,
    JazzCredentials,
    isJazzCredentials,
} from './types.js';

export type { WorkOSAuthHook };
export type { JazzCredentials };
export type { RedirectOptions };
export { isJazzCredentials };

export class JazzWorkOSAuth {
    constructor(
        private authenticate: AuthenticateAccountFunction,
        private authSecretStorage: AuthSecretStorage,
    ) { }

    private isFirstCall = true;
    private static provider = 'workos'

    static loadWorkOSAuthData(
        credentials: JazzCredentials,
        storage: AuthSecretStorage,
    ) {
        return storage.set({
            provider: this.provider,
            accountID: credentials.jazzAccountID,
            accountSecret: credentials.jazzAccountSecret,
            secretSeed: credentials.jazzAccountSeed
                ? Uint8Array.from(credentials.jazzAccountSeed)
                : undefined,
        });
    }

    async initializeAuth(
        workOSClient: Pick<WorkOSAuthHook, "user">, 
    ) {
        const secretStorage = new AuthSecretStorage();

        if (workOSClient.user && this.isFirstCall) {
            const credentials =  await secretStorage.get();
            if (!credentials) {
                throw new Error("No credentials found");
            }

            const jazzAccountSeed = credentials.secretSeed
            ? Array.from(credentials.secretSeed)
            : undefined;
    

            const updatedCredentials = {
                ...credentials,
                seed: jazzAccountSeed,
                provider: JazzWorkOSAuth.provider,
            }
            
            const currentAccount = await Account.getMe().ensureLoaded({
                resolve: {
                    profile: true,
                },
            });
    
            const username = getWorkOSUsername(workOSClient);
            if (username) {
                currentAccount.profile.name = username;
            }

            await this.authenticate(updatedCredentials);
            await JazzWorkOSAuth.loadWorkOSAuthData({
                jazzAccountID: credentials.accountID,
                jazzAccountSecret: credentials.accountSecret,
                jazzAccountSeed,
            }, secretStorage);

            this.isFirstCall = false;
        }
    }

    signOut = async (workosClient: Pick<WorkOSAuthHook, "signOut">, options?: { returnTo?: string, navigate?: true }) => {
        const credentials = await this.authSecretStorage.get();
        if (!credentials) return

        await this.authSecretStorage.clear();
        workosClient.signOut(options);
    }
}


// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace BrowserWorkOSAuth {
    export interface Driver {
        onError: (error: string | Error) => void;
    }
}