import {
    Account,
    AuthCredentials,
    AuthSecretStorage,
    AuthenticateAccountFunction
} from 'jazz-tools';
import { getWorkOSUsername } from './getWorkOSUsername.js';
import {
    MinimalWorkOSClient,
    WorkOSCredentials,
    isWorkOSCredentials,
    isWorkOSAuthStateEqual
} from './types.js';

export type { MinimalWorkOSClient };
export { isWorkOSCredentials };

export class JazzWorkOSAuth {
    constructor(
        private authenticate: AuthenticateAccountFunction,
        private authSecretStorage: AuthSecretStorage,
    ) { }

    static loadWorkOSAuthData(
        credentials: WorkOSCredentials,
        storage: AuthSecretStorage,
    ) {
        return storage.set({
            accountID: credentials.jazzAccountID,
            accountSecret: credentials.jazzAccountSecret,
            secretSeed: credentials.jazzAccountSeed
                ? Uint8Array.from(credentials.jazzAccountSeed)
                : undefined,
            provider: "workos",
        });
    }

    static async initializeAuth(workos: MinimalWorkOSClient) {
        const secretStorage = new AuthSecretStorage();

        if (!isWorkOSCredentials(workos.user?.metadata)) {
            return;
        }

        await JazzWorkOSAuth.loadWorkOSAuthData(
            workos.user.metadata,
            secretStorage,
        );
    }

    private isFirstCall = true;

    registerListener(workOSClient: MinimalWorkOSClient) {
        let previousUser: MinimalWorkOSClient["user"] | null =
            workOSClient.user ?? null;
    
        // Need to use addListener because the WorkOS user object is not updated when the user logs in
        return workOSClient.addListener((event) => {
          const user = (event as Pick<MinimalWorkOSClient, "user">).user ?? null;
    
          if (!isWorkOSAuthStateEqual(previousUser, user) || this.isFirstCall) {
            this.onWorkOSUserChange({ user });
            previousUser = user;
            this.isFirstCall = false;
          }
        });
    }

    onWorkOSUserChange = async (workOSClient: Pick<MinimalWorkOSClient, "user">) => {
        const isAuthenticated = this.authSecretStorage.isAuthenticated;
    
        if (!workOSClient.user) {
          if (isAuthenticated) {
            this.authSecretStorage.clear();
          }
          return;
        }

        if (isAuthenticated) return;

        const workOSCredentials = workOSClient.user.metadata as WorkOSCredentials;

        if (!workOSCredentials.jazzAccountID) {
            await this.signIn(workOSClient);
        } else {
            await this.logIn(workOSClient);
        }
    }

    logIn = async (workOSClient: Pick<MinimalWorkOSClient, "user">) => {
        if (!workOSClient.user) {
            throw new Error("Not signed in on WorkOS")
        }

        const workOSCredentials = workOSClient.user.metadata;
        if (!isWorkOSCredentials(workOSCredentials)) {
            throw new Error("No credentials found on WorkOS");
        }

        const credentials = {
            provider: "workos",
            accountID: workOSCredentials.jazzAccountID,
            accountSecret: workOSCredentials.jazzAccountSecret,
            secretSeed: workOSCredentials.jazzAccountSeed ? Uint8Array.from(workOSCredentials.jazzAccountSeed) : undefined,
        } satisfies AuthCredentials;

        await this.authenticate(credentials);

        await JazzWorkOSAuth.loadWorkOSAuthData({
            jazzAccountID: credentials.accountID,
            jazzAccountSecret: credentials.accountSecret,
            jazzAccountSeed: workOSCredentials.jazzAccountSeed,
        }, this.authSecretStorage);
    }

    signIn = async (workOSClient: Pick<MinimalWorkOSClient, "user">) => {
        const credentials = await this.authSecretStorage.get();

        if (!credentials) {
            throw new Error("No credentials found");
        }

        const jazzAccountSeed = credentials.secretSeed
        ? Array.from(credentials.secretSeed)
        : undefined;

        await workOSClient.user?.update({
            metadata: {
                jazzAccountID: credentials.accountID,
                jazzAccountSecret: credentials.accountSecret,
                jazzAccountSeed,
            } satisfies WorkOSCredentials,
        });

        const currentAccount = await Account.getMe().ensureLoaded({
            resolve: {
                profile: true,
            },
        });

        const username = getWorkOSUsername(workOSClient);

        if (username) {
            currentAccount.profile.name = username;
        }

        await JazzWorkOSAuth.loadWorkOSAuthData({
            jazzAccountID: credentials.accountID,
            jazzAccountSecret: credentials.accountSecret,
            jazzAccountSeed,
        }, this.authSecretStorage);
    }
}


// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace BrowserWorkOSAuth {
    export interface Driver {
        onError: (error: string | Error) => void;
    }
}