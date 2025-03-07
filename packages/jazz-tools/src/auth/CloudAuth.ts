import { randomBytes } from "crypto";
import { createAuthClient } from "better-auth/client";
import { inferAdditionalFields } from "better-auth/client/plugins";
import { AgentSecret } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { SignerSecret } from "cojson/src/crypto/crypto.js";
import { JsonArray, JsonObject } from "cojson/src/jsonValue.js";
import { Account } from "../coValues/account.js";
import { ID } from "../internal.js";
import { AuthCredentials, AuthenticateAccountFunction } from "../types.js";
import { AuthSecretStorage } from "./AuthSecretStorage.js";

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
            type: "number[]",
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
    const k1 = randomBytes(keyBytes.length);
    const k0 = new Uint8Array(keyBytes.length);
    for (let i = 0; i < keyBytes.length; i++) {
      const kByte = keyBytes[i];
      const k1Byte = k1[i];
      if (!kByte || !k1Byte) continue;
      k0[i] = kByte ^ k1Byte;
    }
    return [k0, k1];
  }

  static async combineKeyShards(
    k0: Uint8Array,
    k1: Uint8Array,
  ): Promise<SignerSecret> {
    const wasmCrypto = await WasmCrypto.create();
    const k = new Uint8Array(k0.length);
    for (let i = 0; i < k0.length; i++) {
      const k0Byte = k0[i];
      const k1Byte = k1[i];
      if (!k0Byte || !k1Byte) continue;
      k[i] = k0Byte ^ k1Byte;
    }
    return wasmCrypto.signerSecretFromBytes(k);
  }

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
    credentials: AuthCredentials,
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
      `${keyserver}/api/key-shard/${credentials.accountID}/${jwt}`,
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
    return await CloudAuth.combineKeyShards(k0, data);
  }

  static loadAuthData(
    session: Pick<Session, "user">,
    storage: AuthSecretStorage,
  ) {
    return storage.set({
      accountID: session.user.accountID as ID<Account>,
      accountSecret: session.user.accountSecret as AgentSecret,
      secretSeed: session.user.secretSeed
        ? Uint8Array.from(session.user.secretSeed)
        : undefined,
      provider: session.user.provider ? session.user.provider : "",
    });
  }

  onUserChange = async (session: Pick<Session, "user">) => {
    if (!session.user) return;
    const isAuthenticated = this.authSecretStorage.isAuthenticated;
    if (!isAuthenticated) return;
  };

  logIn = async (session: Pick<Session, "user">) => {
    if (!session.user) throw new Error("Not signed in");
    const credentials = {
      accountID: session.user.accountID as ID<Account>,
      accountSecret: session.user.accountSecret as AgentSecret,
      secretSeed: session.user.secretSeed
        ? Uint8Array.from(session.user.secretSeed)
        : undefined,
      provider: session.user.provider ? session.user.provider : "",
    } satisfies AuthCredentials;
    await this.authenticate(credentials);
    await CloudAuth.loadAuthData(session, this.authSecretStorage);
  };

  signIn = async (session: Pick<Session, "user">) => {
    const credentials = await this.authSecretStorage.get();
    if (!credentials) throw new Error("No credentials found");
    const jazzAccountSeed = credentials.secretSeed
      ? Array.from(credentials.secretSeed)
      : undefined;
    await this.authClient.updateUser({
      accountID: credentials.accountID,
      accountSecret: credentials.accountSecret,
      secretSeed: jazzAccountSeed,
      provider: credentials.provider,
    });
    const currentAccount = await Account.getMe().ensureLoaded({
      profile: {},
    });
    currentAccount.profile.name = session.user.name;
    await CloudAuth.loadAuthData(session, this.authSecretStorage);
  };
}
