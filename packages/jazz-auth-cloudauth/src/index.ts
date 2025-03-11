import { base58 } from "@scure/base";
import { createAuthClient } from "better-auth/client";
import { inferAdditionalFields } from "better-auth/client/plugins";
import { AgentSecret } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { SealerSecret, SignerSecret } from "cojson/src/crypto/crypto.js";
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

  static splitKeyBytes(keyBytes: Uint8Array): [Uint8Array, Uint8Array] {
    const k1 = new Uint8Array(keyBytes.length);
    window.crypto.getRandomValues(k1);
    const k0 = new Uint8Array(keyBytes.length);
    for (let i = 0; i < keyBytes.length; i++) {
      const kByte = keyBytes[i] || 0;
      const k1Byte = k1[i] || 0;
      k0[i] = kByte ^ k1Byte;
    }
    return [k0, k1];
  }

  static async splitSignerSecret(
    accountSecret: AgentSecret,
  ): Promise<[Uint8Array, Uint8Array]> {
    const wasmCrypto = await WasmCrypto.create();
    const signerSecret = wasmCrypto.getAgentSignerSecret(accountSecret);
    const keyBytes = wasmCrypto.signerSecretToBytes(signerSecret);
    return CloudAuth.splitKeyBytes(keyBytes);
  }

  static async splitSealerSecret(
    accountSecret: AgentSecret,
  ): Promise<[Uint8Array, Uint8Array]> {
    const wasmCrypto = await WasmCrypto.create();
    const sealerSecret = wasmCrypto.getAgentSealerSecret(accountSecret);
    const keyBytes = wasmCrypto.sealerSecretToBytes(sealerSecret);
    return CloudAuth.splitKeyBytes(keyBytes);
  }

  static mergeKeyShards(k0: Uint8Array, k1: Uint8Array): Uint8Array {
    const k = new Uint8Array(k0.length);
    for (let i = 0; i < k0.length; i++) {
      const k0Byte = k0[i] || 0;
      const k1Byte = k1[i] || 0;
      k[i] = k0Byte ^ k1Byte;
    }
    return k;
  }

  static async mergeSignerSecret(
    k0: Uint8Array,
    k1: Uint8Array,
  ): Promise<SignerSecret> {
    return (await WasmCrypto.create()).signerSecretFromBytes(
      CloudAuth.mergeKeyShards(k0, k1),
    );
  }

  static async mergeSealerSecret(
    k0: Uint8Array,
    k1: Uint8Array,
  ): Promise<SealerSecret> {
    return (await WasmCrypto.create()).sealerSecretFromBytes(
      CloudAuth.mergeKeyShards(k0, k1),
    );
  }

  // Splits keys into k0 & k1 for each key
  // k1 gets stored in the key server, k0 gets stored in Better Auth
  // Does this for two keys, being the signing key and sealer key
  static async splitAndSendKeys(
    credentials: AuthCredentials,
    keyserver: string,
  ): Promise<[Uint8Array, Uint8Array]> {
    const [signer0, signer1] = await this.splitSignerSecret(
      credentials.accountSecret,
    );
    const [sealer0, sealer1] = await this.splitSealerSecret(
      credentials.accountSecret,
    );

    const wasmCrypto = await WasmCrypto.create();
    const signerSecret = wasmCrypto.getAgentSignerSecret(
      credentials.accountSecret,
    );
    const signerId = wasmCrypto.getSignerID(signerSecret);
    const signedSignerKeyShard = wasmCrypto.sign(
      signerSecret,
      Array.from(signer1),
    );
    const signedSealerKeyShard = wasmCrypto.sign(
      signerSecret,
      Array.from(sealer1),
    );

    fetch(`${keyserver}/api/key-shard`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        accountId: credentials.accountID,
        signerId: signerId,
        signerKeyShard: Array.from(signer1),
        sealerKeyShard: Array.from(sealer1),
        signedSignerKeyShard: signedSignerKeyShard,
        signedSealerKeyShard: signedSealerKeyShard,
      }),
    })
      .then((res) => res.json())
      .then((data) => {
        console.log("Response data:", data);
      })
      .catch((err) => {
        console.log("Unable to fetch:", err);
      });
    return [signer0, sealer0];
  }

  static async getAndMergeKeys(
    signer0: Uint8Array,
    sealer0: Uint8Array,
    authClient: AuthClient,
    accountID: ID<Account>,
    keyserver: string,
  ): Promise<[SignerSecret, SealerSecret]> {
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
    const signerSecret = await CloudAuth.mergeSignerSecret(
      signer0,
      Uint8Array.from(data.signerKeyShard),
    );
    const sealerSecret = await CloudAuth.mergeSealerSecret(
      sealer0,
      Uint8Array.from(data.sealerKeyShard),
    );
    return [signerSecret, sealerSecret];
  }

  static async sessionWithMergedKey(
    session: Pick<Session, "user">,
    authClient: AuthClient,
    keyserver: string,
  ) {
    const wasmCrypto = await WasmCrypto.create();
    const accountID = session.user.accountID as ID<Account>;
    const betterauthAccountSecret = session.user.accountSecret as AgentSecret;
    const signer0 = wasmCrypto.signerSecretToBytes(
      wasmCrypto.getAgentSignerSecret(betterauthAccountSecret),
    );
    const sealer0 = wasmCrypto.sealerSecretToBytes(
      wasmCrypto.getAgentSealerSecret(betterauthAccountSecret),
    );
    const [signerSecret, sealerSecret] = await CloudAuth.getAndMergeKeys(
      signer0,
      sealer0,
      authClient,
      accountID,
      keyserver,
    );
    const accountSecret = `${sealerSecret}/${signerSecret}` as AgentSecret;

    return {
      accountID: accountID,
      accountSecret: accountSecret,
      secretSeed: session.user.secretSeed
        ? Uint8Array.from(base58.decode(session.user.secretSeed))
        : undefined,
      provider: "cloudauth",
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
    return storage.set(authSetPayload);
  }

  onUserChange = async (session: Pick<Session, "user">) => {
    if (!session || !session.user) return;
    const isAuthenticated = this.authSecretStorage.isAuthenticated;
    if (!isAuthenticated) return;
  };

  logIn = async (session: Pick<Session, "user">) => {
    if (!session.user) throw new Error("Not signed in");
    const credentials = await CloudAuth.sessionWithMergedKey(
      session,
      this.authClient,
      this.keyserver,
    );
    await this.authenticate(credentials);
    await CloudAuth.loadAuthData(
      session,
      this.authSecretStorage,
      this.authClient,
      this.keyserver,
    );
  };

  signIn = async (session: Pick<Session, "user">) => {
    const credentials = await this.authSecretStorage.get();
    if (!credentials) throw new Error("No credentials found");
    const jazzAccountSeed = credentials.secretSeed
      ? Array.from(credentials.secretSeed)
      : undefined;

    // Split key, store one shard in Better Auth, other shard in keyserver
    const wasmCrypto = await WasmCrypto.create();
    const [signer0, sealer0] = await CloudAuth.splitAndSendKeys(
      credentials,
      this.keyserver,
    );
    const signerSecret = wasmCrypto.signerSecretFromBytes(signer0);
    const sealerSecret = wasmCrypto.sealerSecretFromBytes(sealer0);
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
