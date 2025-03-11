import { base58 } from "@scure/base";
import { createAuthClient } from "better-auth/client";
import { inferAdditionalFields } from "better-auth/client/plugins";
import { AgentSecret } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { SignerSecret } from "cojson/src/crypto/crypto.js";
import {
  Account,
  AuthCredentials,
  AuthSecretStorage,
  AuthenticateAccountFunction,
  ID,
} from "jazz-tools";

export const newAuthClient = (baseUrl: string) =>
  createAuthClient({
    baseURL: baseUrl,
    plugins: [
      inferAdditionalFields({
        user: {
          accountID: {
            type: "string",
            required: true,
          },
          secretSeed: {
            type: "string",
            required: false,
          },
          accountSecret: {
            type: "string",
            required: true,
          },
          provider: {
            type: "string",
            required: false,
          },
        },
      }),
    ],
  });

export type AuthClient = ReturnType<typeof newAuthClient>;
export type Session = AuthClient["$Infer"]["Session"];

export class CloudAuth {
  constructor(
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
    private authClient: AuthClient,
    private keyserver: string,
  ) {}

  static async splitKey(
    credentials: AuthCredentials,
  ): Promise<[Uint8Array, Uint8Array]> {
    const wasmCrypto = await WasmCrypto.create();
    const signerSecret = wasmCrypto.getAgentSignerSecret(
      credentials.accountSecret,
    );
    const keyBytes = wasmCrypto.signerSecretToBytes(signerSecret);
    console.log(`SPLITTING k: ${wasmCrypto.signerSecretFromBytes(keyBytes)}`);
    const k1 = new Uint8Array(keyBytes.length);
    window.crypto.getRandomValues(k1);
    const k0 = new Uint8Array(keyBytes.length);
    for (let i = 0; i < keyBytes.length; i++) {
      if (keyBytes[i] === undefined || k1[i] === undefined) {
        console.log(`keyBytes[${i}]`, keyBytes[i], `k1[${i}]`, k1[i]);
      }
      const kByte = keyBytes[i] || 0;
      const k1Byte = k1[i] || 0;
      k0[i] = kByte ^ k1Byte;
    }
    console.log(
      `SPLITTING k0: ${wasmCrypto.signerSecretFromBytes(k0)}, k1: ${wasmCrypto.signerSecretFromBytes(k1)}`,
    );
    return [k0, k1];
  }

  static async combineKeyShards(
    k0: Uint8Array,
    k1: Uint8Array,
  ): Promise<SignerSecret> {
    const wasmCrypto = await WasmCrypto.create();
    console.log(
      `MERGING k0: ${wasmCrypto.signerSecretFromBytes(k0)}, k1: ${wasmCrypto.signerSecretFromBytes(k1)}`,
    );
    const k = new Uint8Array(k0.length);
    for (let i = 0; i < k0.length; i++) {
      if (k0[i] === undefined || k1[i] === undefined) {
        console.log(`k0[${i}]`, k0[i], `k1[${i}]`, k1[i]);
      }
      const k0Byte = k0[i] || 0;
      const k1Byte = k1[i] || 0;
      k[i] = k0Byte ^ k1Byte;
    }
    console.log(`MERGING k: ${wasmCrypto.signerSecretFromBytes(k)}`);
    return wasmCrypto.signerSecretFromBytes(k);
  }

  // Splits key into k0 and k1
  // k1 gets stored in the key server, k0 gets stored in Better Auth
  static async splitAndSendKey(
    credentials: AuthCredentials,
    keyserver: string,
  ) {
    const [k0, k1] = await this.splitKey(credentials);
    const wasmCrypto = await WasmCrypto.create();
    const signerSecret = wasmCrypto.getAgentSignerSecret(
      credentials.accountSecret,
    );
    const signerId = wasmCrypto.getSignerID(signerSecret);
    const keyShard = Array.from(k1);
    const signedKeyShard = wasmCrypto.sign(signerSecret, keyShard);
    fetch(`${keyserver}/api/key-shard`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        accountId: credentials.accountID,
        signerId: signerId,
        signedKeyShard: signedKeyShard,
        keyShard: keyShard,
      }),
    })
      .then((res) => res.json())
      .then((data) => {
        console.log("Response data:", data);
      })
      .catch((err) => {
        console.log("Unable to fetch:", err);
      });
    return k0;
  }

  static async getAndMergeKey(
    k0: Uint8Array,
    authClient: AuthClient,
    accountID: ID<Account>,
    keyserver: string,
  ) {
    let jwt = "";
    await authClient.getSession({
      fetchOptions: {
        onSuccess: async (ctx) => {
          jwt = ctx.response.headers.get("set-auth-jwt") ?? "";
        },
      },
    });
    const response = await fetch(
      `${keyserver}/api/key-shard/${accountID}/${jwt}`,
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${jwt}`,
        },
      },
    );
    const data = await response.json();
    console.log("Response data:", data);
    return await CloudAuth.combineKeyShards(k0, Uint8Array.from(data.keyShard));
  }

  static async sessionWithMergedKey(
    session: Pick<Session, "user">,
    authClient: AuthClient,
    keyserver: string,
  ) {
    const wasmCrypto = await WasmCrypto.create();
    const accountID = session.user.accountID as ID<Account>;
    const betterauthAccountSecret = session.user.accountSecret as AgentSecret;
    const k0 = wasmCrypto.signerSecretToBytes(
      wasmCrypto.getAgentSignerSecret(betterauthAccountSecret),
    );
    const signerSecret = await CloudAuth.getAndMergeKey(
      k0,
      authClient,
      accountID,
      keyserver,
    );
    const sealerSecret = wasmCrypto.getAgentSealerSecret(
      betterauthAccountSecret,
    );
    const accountSecret = `${sealerSecret}/${signerSecret}` as AgentSecret;

    return {
      accountID: accountID,
      accountSecret: accountSecret,
      secretSeed: session.user.secretSeed
        ? Uint8Array.from(base58.decode(session.user.secretSeed))
        : undefined,
      provider: session.user.provider ? session.user.provider : "",
    } satisfies AuthCredentials;
  }

  // Uses Better Auth session data & keyserver to set auth data locally
  static async loadAuthData(
    session: Pick<Session, "user">,
    storage: AuthSecretStorage,
    authClient: AuthClient,
    keyserver: string,
  ) {
    const authSetPayload = await CloudAuth.sessionWithMergedKey(
      session,
      authClient,
      keyserver,
    );
    // console.log(`Setting account secret: ${accountSecret}`);
    console.log(`storage.set: ${JSON.stringify(authSetPayload)}`);
    return storage.set(authSetPayload);
  }

  onUserChange = async (session: Pick<Session, "user">) => {
    if (!session || !session.user) return;
    const isAuthenticated = this.authSecretStorage.isAuthenticated;
    if (!isAuthenticated) return;
  };

  logIn = async (session: Pick<Session, "user">) => {
    console.log(`Login session: ${JSON.stringify(session)}`);
    if (!session.user) throw new Error("Not signed in");
    const credentials = await CloudAuth.sessionWithMergedKey(
      session,
      this.authClient,
      this.keyserver,
    );
    console.log(`this.authenticate: ${JSON.stringify(credentials)}`);
    await this.authenticate(credentials);
    await CloudAuth.loadAuthData(
      session,
      this.authSecretStorage,
      this.authClient,
      this.keyserver,
    );
  };

  signIn = async (session: Pick<Session, "user">) => {
    console.log(`Sign-in session: ${JSON.stringify(session)}`);
    const credentials = await this.authSecretStorage.get();
    if (!credentials) throw new Error("No credentials found");
    console.log(`Sign-in credentials: ${JSON.stringify(credentials)}`);
    const jazzAccountSeed = credentials.secretSeed
      ? Array.from(credentials.secretSeed)
      : undefined;

    // Split key, store one shard in Better Auth, other shard in keyserver
    const wasmCrypto = await WasmCrypto.create();
    const k0 = await CloudAuth.splitAndSendKey(credentials, this.keyserver);
    const signerSecret = wasmCrypto.signerSecretFromBytes(k0);
    const sealerSecret = wasmCrypto.getAgentSealerSecret(
      credentials.accountSecret,
    );
    const accountSecret = `${sealerSecret}/${signerSecret}`;

    const updatedUser = {
      accountID: credentials.accountID,
      accountSecret: accountSecret,
      secretSeed: jazzAccountSeed
        ? base58.encode(Uint8Array.from(jazzAccountSeed))
        : undefined,
      provider: credentials.provider,
    };
    await this.authClient.updateUser(updatedUser);
    session.user = { ...session.user, ...updatedUser };

    const currentAccount = await Account.getMe().ensureLoaded({
      profile: {},
    });
    currentAccount.profile.name = session.user.name;
    await CloudAuth.loadAuthData(
      session,
      this.authSecretStorage,
      this.authClient,
      this.keyserver,
    );
  };
}
